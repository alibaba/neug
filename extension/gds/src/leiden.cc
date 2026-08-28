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

#include "leiden.h"

#include <unordered_set>
#include "impl/leiden_impl.h"
#include "utils/option_utils.h"
#include "utils/subgraph_utils.h"

namespace neug {
namespace gds {

struct LeidenInput : public function::CallFuncInputBase {
  ~LeidenInput() = default;
  LeidenInput() = default;

  bool parse_subgraph(const ::physical::Subgraph& subgraph,
                      const execution::ContextMeta& ctx_meta) {
    ParsedSubgraph parsed;
    if (!parse_subgraph_entries(subgraph, ctx_meta, parsed)) {
      return false;
    }
    if (parsed.vertex_entries.empty()) {
      LOG(ERROR) << "leiden requires at least one vertex label.";
      return false;
    }
    if (parsed.edge_entries.empty()) {
      LOG(ERROR) << "leiden requires at least one edge label.";
      return false;
    }
    // Validate: no duplicate vertex labels
    {
      std::unordered_set<label_t> seen_labels;
      for (const auto& ve : parsed.vertex_entries) {
        if (seen_labels.count(ve.label)) {
          LOG(ERROR) << "leiden"
                     << ": duplicate vertex label '" << ve.label << "'.";
          return false;
        }
        seen_labels.insert(ve.label);
      }
    }
    // Collect declared vertex labels for triplet validation
    std::unordered_set<label_t> declared_vertex_labels;
    for (auto& ve : parsed.vertex_entries) {
      declared_vertex_labels.insert(ve.label);
      vertex_labels.push_back(ve.label);
      vertex_preds.push_back(std::move(ve.predicate));
    }
    // Validate: all edge triplet labels reference declared vertex labels
    for (auto& ee : parsed.edge_entries) {
      if (declared_vertex_labels.find(ee.triplet.src_label) ==
          declared_vertex_labels.end()) {
        LOG(ERROR) << "leiden"
                   << ": edge triplet src_label '" << ee.triplet.src_label
                   << "' is not a declared vertex label.";
        return false;
      }
      if (declared_vertex_labels.find(ee.triplet.dst_label) ==
          declared_vertex_labels.end()) {
        LOG(ERROR) << "leiden"
                   << ": edge triplet dst_label '" << ee.triplet.dst_label
                   << "' is not a declared vertex label.";
        return false;
      }
      edge_triplets.push_back(ee.triplet);
      edge_preds.push_back(std::move(ee.predicate));
    }
    return true;
  }

  std::vector<label_t> vertex_labels;
  std::vector<LabelTriplet> edge_triplets;
  // Predicates aligned with vertex_labels / edge_triplets; entries may be
  // null when the corresponding entry carries no predicate.
  std::vector<std::unique_ptr<execution::ExprBase>> vertex_preds;
  std::vector<std::unique_ptr<execution::ExprBase>> edge_preds;
  double resolution = 1.0;
  bool directed = false;
  double threshold = 1e-7;
  int32_t concurrency;
  std::string initial_community_property;
  bool allow_relocation = false;
  std::string weight;
  int32_t node_alias;
  int32_t community_alias;
  int32_t previous_community_alias = -1;
};

std::unique_ptr<function::CallFuncInputBase> LeidenFunction::bind(
    const Schema& schema, const execution::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr();
  const auto& subgraph = opr.gds_algo().sub_graph();
  const auto& options = opr.gds_algo().options();

  auto input = std::make_unique<LeidenInput>();
  if (!input->parse_subgraph(subgraph, ctx_meta)) {
    LOG(ERROR) << "Failed to parse subgraph for leiden.";
    THROW_NOT_SUPPORTED_EXCEPTION("Invalid subgraph for leiden");
  }

  input->resolution = get_option_value<double>(options, "resolution", 1.0);
  input->directed = get_option_value<bool>(options, "directed", false);
  input->threshold = get_option_value<double>(options, "threshold", 1e-7);
  input->concurrency = get_option_value<int32_t>(
      options, "concurrency", std::thread::hardware_concurrency());
  input->initial_community_property =
      get_option_value<std::string>(options, "initial_community_property", "");
  input->allow_relocation =
      get_option_value<bool>(options, "allow_relocation", false);
  input->weight = get_option_value<std::string>(options, "weight", "");

  input->node_alias = -1;
  input->community_alias = -1;
  input->previous_community_alias = -1;
  int int64_count = 0;
  const auto& meta_data = plan.plan(op_idx);
  for (int i = 0; i < meta_data.meta_data_size(); i++) {
    const auto& meta = meta_data.meta_data(i);
    auto type = parse_from_ir_data_type(meta.type());
    if (type.id() == common::DataTypeId::kVertex) {
      input->node_alias = meta.alias();
    } else if (type.id() == common::DataTypeId::kInt64) {
      if (int64_count == 0)
        input->community_alias = meta.alias();
      else
        input->previous_community_alias = meta.alias();
      ++int64_count;
    }
  }

  return input;
}

execution::Context LeidenFunction::exec(
    const function::CallFuncInputBase& input_base, IStorageInterface& g) {
  const auto& input = dynamic_cast<const LeidenInput&>(input_base);
  const auto& graph = dynamic_cast<const StorageReadInterface&>(g);

  // directed is accepted for interface compatibility but ignored (same as
  // the original leiden implementation).
  std::vector<execution::ExprBase*> vertex_preds;
  vertex_preds.reserve(input.vertex_preds.size());
  for (const auto& pred : input.vertex_preds)
    vertex_preds.push_back(pred.get());
  std::vector<execution::ExprBase*> edge_preds;
  edge_preds.reserve(input.edge_preds.size());
  for (const auto& pred : input.edge_preds)
    edge_preds.push_back(pred.get());
  community::Leiden leiden(graph, input.vertex_labels, input.edge_triplets,
                           input.resolution, input.threshold, input.concurrency,
                           input.initial_community_property,
                           input.allow_relocation, input.weight,
                           std::move(vertex_preds), std::move(edge_preds));
  leiden.compute();

  execution::Context ctx;
  leiden.sink(ctx, input.node_alias, input.community_alias,
              input.previous_community_alias);
  return ctx;
}

function::function_set LeidenFunction::getFunctionSet() {
  function::function_set funcSet;
  function::call_input_types inputTypes = {
      common::DataType(common::DataTypeId::kVarchar),
      common::DataType(common::DataTypeId::kUnknown)};
  function::call_output_columns outputColumns = {
      {"node", common::DataType(common::DataTypeId::kVertex)},
      {"community", common::DataType(common::DataTypeId::kInt64)},
      {"previous_community", common::DataType(common::DataTypeId::kInt64)}};
  auto function = std::make_unique<function::GDSAlgoFunction>(name, inputTypes,
                                                              outputColumns);
  function->bindFunc = bind;
  function->execFunc = exec;

  funcSet.emplace_back(std::move(function));
  return funcSet;
}

}  // namespace gds
}  // namespace neug
