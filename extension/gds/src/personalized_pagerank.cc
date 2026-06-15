/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "personalized_pagerank.h"

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "utils/option_utils.h"
#include "utils/parallel_utils.h"
#include "utils/subgraph_utils.h"

namespace neug {
namespace gds {

class PersonalizedPageRank {
 public:
  PersonalizedPageRank(const StorageReadInterface& graph, label_t vertex_label,
                       label_t edge_label, const std::string& edge_weight_prop,
                       const std::string& node_weight_prop, int max_iterations,
                       double damping_factor, int concurrency)
      : graph_(graph),
        vertex_label_(vertex_label),
        edge_label_(edge_label),
        max_iterations_(max_iterations),
        damping_factor_(damping_factor),
        concurrency_(concurrency) {
    edge_weight_accessor_ = graph.GetEdgeDataAccessor(
        vertex_label, vertex_label, edge_label, edge_weight_prop);
    node_weight_column_ = std::dynamic_pointer_cast<TypedRefColumn<double>>(
        graph.GetVertexPropColumn(vertex_label, node_weight_prop));

    size_t vertex_count = graph.GetVertexSet(vertex_label).size();
    normalized_node_weights_ = std::make_unique<double[]>(vertex_count);
    sum_of_outgoing_weights_ = std::make_unique<double[]>(vertex_count);
    valid_vertices_.reserve(vertex_count);
    for (vid_t v : graph.GetVertexSet(vertex_label)) {
      valid_vertices_.push_back(v);
    }
    double sum_of_node_weights = 0.0;
    auto oe_view = graph.GetGenericOutgoingGraphView(vertex_label, vertex_label,
                                                     edge_label);
    auto dangling_flag = std::make_unique<bool[]>(vertex_count);

    ParallelUtils::parallel_for(
        valid_vertices_.data(), valid_vertices_.size(),
        [&](vid_t v, int thread_id) {
          normalized_node_weights_[v] = node_weight_column_->get_view(v);
          sum_of_node_weights += normalized_node_weights_[v];
          double sum_weights = 0.0;
          auto oes = oe_view.get_edges(v);
          for (auto it = oes.begin(); it != oes.end(); ++it) {
            double weight = edge_weight_accessor_.get_typed_data<double>(it);
            sum_weights += weight;
          }
          if (sum_weights < 1e-9) {
            dangling_flag[v] = true;
          } else {
            dangling_flag[v] = false;
          }
          sum_of_outgoing_weights_[v] = sum_weights;
          pr_[v] = normalized_node_weights_[v];
        },
        concurrency_);
    for (vid_t v = 0; v < vertex_count; ++v) {
      if (dangling_flag[v]) {
        dangling_vertices_.push_back(v);
      }
    }
    ParallelUtils::parallel_for(
        valid_vertices_.data(), valid_vertices_.size(),
        [&](vid_t v, int thread_id) {
          normalized_node_weights_[v] /= sum_of_node_weights;
        },
        concurrency_);
  }

  void computePersonalizedPageRank() {
    size_t vertex_count = graph_.GetVertexSet(vertex_label_).size();
    std::unique_ptr<double[]> new_pr = std::make_unique<double[]>(vertex_count);
    auto ie_view = graph_.GetGenericIncomingGraphView(
        vertex_label_, vertex_label_, edge_label_);
    for (int iter = 0; iter < max_iterations_; ++iter) {
      double dangling_sum = 0.0;
      for (vid_t v : dangling_vertices_) {
        dangling_sum += pr_[v];
      }
      ParallelUtils::parallel_for(
          valid_vertices_.data(), valid_vertices_.size(),
          [&](vid_t v, int thread_id) {
            double rank_sum = 0.0;
            auto ies = ie_view.get_edges(v);
            for (auto it = ies.begin(); it != ies.end(); ++it) {
              vid_t src = it.get_vertex();
              double weight = edge_weight_accessor_.get_typed_data<double>(it);
              double sum_of_outgoing_weights_src =
                  sum_of_outgoing_weights_[src];
              if (sum_of_outgoing_weights_src > 1e-9) {
                rank_sum += pr_[src] * weight / sum_of_outgoing_weights_src;
              }
            }
            new_pr[v] =
                damping_factor_ * rank_sum +
                damping_factor_ * dangling_sum * normalized_node_weights_[v] +
                (1 - damping_factor_) * normalized_node_weights_[v];
          },
          concurrency_);
      std::swap(pr_, new_pr);
    }
  }

  void sink(execution::Context& ctx, int node_alias, int pr_alias) {
    execution::MSVertexColumnBuilder builder(vertex_label_);
    builder.reserve(valid_vertices_.size());
    execution::ValueColumnBuilder<double> pr_builder;
    pr_builder.reserve(valid_vertices_.size());
    for (vid_t v : valid_vertices_) {
      builder.push_back_opt(v);
      pr_builder.push_back_opt(pr_[v]);
    }
    execution::DataChunk chunk;
    chunk.set(node_alias, builder.finish());
    chunk.set(pr_alias, pr_builder.finish());
    ctx.append_chunk(std::move(chunk));
  }

 private:
  const StorageReadInterface& graph_;
  label_t vertex_label_;
  label_t edge_label_;
  EdgeDataAccessor edge_weight_accessor_;
  std::shared_ptr<TypedRefColumn<double>> node_weight_column_;
  std::unique_ptr<double[]> normalized_node_weights_;
  std::unique_ptr<double[]> sum_of_outgoing_weights_;
  std::unique_ptr<double[]> pr_;
  std::vector<vid_t> dangling_vertices_;
  std::vector<vid_t> valid_vertices_;
  int max_iterations_;
  double damping_factor_;
  int concurrency_;
};

struct PersonalizedPageRankInput : public function::CallFuncInputBase {
  ~PersonalizedPageRankInput() = default;
  bool parse_subgraph(const ::physical::Subgraph& subgraph,
                      label_t& vertex_label, label_t& edge_label,
                      const execution::ContextMeta& ctx_meta) {
    ParsedSubgraph parsed;
    if (!parse_subgraph_entries(subgraph, ctx_meta, parsed)) {
      return false;
    }
    if (!check_simple_graph_subgraph(parsed, "Personalized PageRank")) {
      return false;
    }

    if (parsed.vertex_entries[0].predicate != nullptr) {
      LOG(ERROR)
          << "Vertex predicates are not supported in Personalized PageRank.";
      return false;
    }
    if (parsed.edge_entries[0].predicate != nullptr) {
      LOG(ERROR)
          << "Edge predicates are not supported in Personalized PageRank.";
      return false;
    }

    vertex_label = parsed.vertex_entries[0].label;
    edge_label = parsed.edge_entries[0].triplet.edge_label;
    return true;
  }

  label_t vertex_label;
  label_t edge_label;
  std::string edge_weight;
  std::string node_weight;
  int max_iterations;
  double damping_factor;
  int32_t node_alias, pr_alias;
  int32_t concurrency;
};

void check_property_valid(const Schema& schema, label_t vertex_label,
                          label_t edge_label,
                          const std::string& edge_weight_prop,
                          const std::string& node_weight_prop) {
  const auto& vertex_prop_names =
      schema.get_vertex_property_names(vertex_label);
  const auto& vertex_prop_types = schema.get_vertex_properties(vertex_label);
  bool node_weight_found = false;
  for (size_t i = 0; i < vertex_prop_names.size(); ++i) {
    if (vertex_prop_names[i] == node_weight_prop) {
      if (vertex_prop_types[i].id() != DataTypeId::kDouble) {
        LOG(ERROR) << "Node weight property must be of type DOUBLE.";
        THROW_NOT_SUPPORTED_EXCEPTION(
            "Node weight property must be of type DOUBLE");
      } else {
        node_weight_found = true;
      }
      break;
    }
  }
  if (!node_weight_found) {
    LOG(ERROR) << "Node weight property not found in vertex properties.";
    THROW_NOT_SUPPORTED_EXCEPTION("Node weight property not found");
  }

  const auto& edge_prop_names =
      schema.get_edge_property_names(vertex_label, vertex_label, edge_label);
  const auto& edge_prop_types =
      schema.get_edge_properties(vertex_label, vertex_label, edge_label);
  bool edge_weight_found = false;
  for (size_t i = 0; i < edge_prop_names.size(); ++i) {
    if (edge_prop_names[i] == edge_weight_prop) {
      if (edge_prop_types[i].id() != DataTypeId::kDouble) {
        LOG(ERROR) << "Edge weight property must be of type DOUBLE.";
        THROW_NOT_SUPPORTED_EXCEPTION(
            "Edge weight property must be of type DOUBLE");
      } else {
        edge_weight_found = true;
      }
      break;
    }
  }
  if (!edge_weight_found) {
    LOG(ERROR) << "Edge weight property not found in edge properties.";
    THROW_NOT_SUPPORTED_EXCEPTION("Edge weight property not found");
  }
}

std::unique_ptr<function::CallFuncInputBase> PersonalizedPageRankFunction::bind(
    const Schema& schema, const execution::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr();
  const auto& subgraph = opr.gds_algo().sub_graph();
  const auto& options = opr.gds_algo().options();
  label_t vertex_label, edge_label;
  auto input = std::make_unique<PersonalizedPageRankInput>();
  if (!input->parse_subgraph(subgraph, vertex_label, edge_label, ctx_meta)) {
    LOG(ERROR) << "Failed to parse subgraph for Personalized PageRank.";
    THROW_NOT_SUPPORTED_EXCEPTION("Invalid subgraph for Personalized PageRank");
  }
  input->vertex_label = vertex_label;
  input->edge_label = edge_label;
  input->node_alias = plan.plan(op_idx).meta_data(0).alias();
  input->pr_alias = plan.plan(op_idx).meta_data(1).alias();
  input->damping_factor =
      get_option_value<double>(options, "damping_factor", 0.85);
  input->max_iterations =
      get_option_value<int32_t>(options, "max_iterations", 20);
  input->edge_weight =
      get_option_value<std::string>(options, "edge_weight", "");
  input->node_weight =
      get_option_value<std::string>(options, "node_weight", "");
  check_property_valid(schema, input->vertex_label, input->edge_label,
                       input->edge_weight, input->node_weight);
  input->concurrency = get_option_value<int32_t>(options, "concurrency", 1);
  return input;
}

execution::Context PersonalizedPageRankFunction::exec(
    const function::CallFuncInputBase& input, neug::IStorageInterface& g) {
  const auto& func_input =
      dynamic_cast<const PersonalizedPageRankInput&>(input);
  const auto& graph = dynamic_cast<const StorageReadInterface&>(g);

  PersonalizedPageRank pagerank(
      graph, func_input.vertex_label, func_input.edge_label,
      func_input.edge_weight, func_input.node_weight, func_input.max_iterations,
      func_input.damping_factor, func_input.concurrency);
  pagerank.computePersonalizedPageRank();
  execution::Context ret;
  pagerank.sink(ret, func_input.node_alias, func_input.pr_alias);
  return ret;
}

function::function_set PersonalizedPageRankFunction::getFunctionSet() {
  function::function_set funcSet;
  // two input params:
  // 1. subgraph name in string
  // 2. options in map
  std::vector<common::DataTypeId> inputTypes = {
      common::DataTypeId::kVarchar, common::DataTypeId::kUnknown};
  // two output columns:
  // 1. node type
  // 2. personalized page rank value in double
  function::call_output_columns outputColumns = {
      {"node", common::DataTypeId::kVertex},
      {"personalized_page_rank", common::DataTypeId::kDouble}};
  auto function = std::make_unique<function::GDSAlgoFunction>(name, inputTypes,
                                                              outputColumns);
  function->bindFunc = bind;
  function->execFunc = exec;
  funcSet.emplace_back(std::move(function));
  return funcSet;
}

}  // namespace gds
}  // namespace neug