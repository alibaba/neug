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

#include "neug/execution/expression/expr.h"
#include "neug/execution/expression/predicates.h"

namespace neug {
namespace gds {

class PersonalizedPageRank {
 public:
  PersonalizedPageRank(const StorageReadInterface& graph, label_t vertex_label,
                       label_t edge_label, const std::string& edge_weight_prop,
                       const std::string& node_weight_prop, int max_iterations,
                       double damping_factor)
      : graph_(graph),
        vertex_label_(vertex_label),
        edge_label_(edge_label),
        max_iterations_(max_iterations),
        damping_factor_(damping_factor) {
    edge_weight_accessor_ = graph.GetEdgeDataAccessor(
        vertex_label, vertex_label, edge_label, edge_weight_prop);
    node_weight_column_ = std::dynamic_pointer_cast<TypedRefColumn<double>>(
        graph.GetVertexPropColumn(vertex_label, node_weight_prop));

    size_t vertex_count = graph.GetVertexSet(vertex_label).size();
    normalized_node_weights_ = std::make_unique<double[]>(vertex_count);
    sum_of_outgoing_weights_ = std::make_unique<double[]>(vertex_count);
    double sum_of_node_weights = 0.0;
    auto oe_view = graph.GetGenericOutgoingGraphView(vertex_label, vertex_label,
                                                     edge_label);

    for (vid_t v = 0; v < vertex_count; ++v) {
      normalized_node_weights_[v] = node_weight_column_->get_view(v);
      sum_of_node_weights += normalized_node_weights_[v];
      double sum_weights = 0.0;
      auto oes = oe_view.get_edges(v);
      for (auto it = oes.begin(); it != oes.end(); ++it) {
        double weight = edge_weight_accessor_.get_typed_data<double>(it);
        sum_weights += weight;
      }
      if (sum_weights < 1e-9) {
        dangling_vertices_.push_back(v);
      }
      sum_of_outgoing_weights_[v] = sum_weights;
      pr_[v] = normalized_node_weights_[v];
    }
    for (vid_t v = 0; v < vertex_count; ++v) {
      normalized_node_weights_[v] /= sum_of_node_weights;
    }
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
      for (vid_t v = 0; v < vertex_count; ++v) {
        double rank_sum = 0.0;
        auto ies = ie_view.get_edges(v);
        for (auto it = ies.begin(); it != ies.end(); ++it) {
          vid_t src = it.get_vertex();
          double weight = edge_weight_accessor_.get_typed_data<double>(it);
          double sum_of_outgoing_weights_src = sum_of_outgoing_weights_[src];
          if (sum_of_outgoing_weights_src > 1e-9) {
            rank_sum += pr_[src] * weight / sum_of_outgoing_weights_src;
          }
        }
        new_pr[v] =
            damping_factor_ * rank_sum +
            damping_factor_ * dangling_sum * normalized_node_weights_[v] +
            (1 - damping_factor_) * normalized_node_weights_[v];
      }
      std::swap(pr_, new_pr);
    }
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
  int max_iterations_;
  double damping_factor_;
};

bool parse_subgraph(const ::physical::Subgraph& subgraph, label_t& vertex_label,
                    label_t& edge_label) {
  if (subgraph.vertex_entries().size() != 1) {
    LOG(ERROR) << "Personalized PageRank currently only supports subgraphs "
                  "with exactly one vertex label.";
    return false;
  }
  const auto& vertex_entry = subgraph.vertex_entries(0);
  if (vertex_entry.has_predicate()) {
    LOG(ERROR)
        << "Vertex predicates are not supported in Personalized PageRank.";
    return false;
  }
  vertex_label = vertex_entry.label_id();
  if (subgraph.edge_entries().size() != 1) {
    LOG(ERROR) << "Personalized PageRank currently only supports subgraphs "
                  "with exactly one edge label.";
    return false;
  }

  const auto& edge_entry = subgraph.edge_entries(0);
  if (edge_entry.has_predicate()) {
    LOG(ERROR) << "Edge predicates are not supported in Personalized PageRank.";
    return false;
  }
  edge_label = edge_entry.edge_label_id();
  if (edge_entry.src_label_id() != vertex_label ||
      edge_entry.dst_label_id() != vertex_label) {
    LOG(ERROR) << "Source and destination vertex labels of the edge must match "
                  "the vertex label in Personalized PageRank.";
    return false;
  }
  return true;
}

execution::Context PersonalizedPageRankFunction::PersonalizedPageRankExec(
    execution::Context& ctx, const ::physical::Subgraph& subgraph,
    const function::options_t& options, IStorageInterface& g) {
  const auto& graph = dynamic_cast<const StorageReadInterface&>(g);

  double damping_factor = 0.85;
  if (options.find("damping_factor") != options.end()) {
    damping_factor = std::stod(options.at("damping_factor"));
  }
  label_t vertex_label, edge_label;
  if (!parse_subgraph(subgraph, vertex_label, edge_label)) {
    LOG(ERROR) << "Failed to parse subgraph for Personalized PageRank.";
    THROW_NOT_SUPPORTED_EXCEPTION("Invalid subgraph for Personalized PageRank");
  }

  std::string edge_weight_prop;
  std::string node_weight_prop;
  int max_iterations = 20;

  PersonalizedPageRank pagerank(graph, vertex_label, edge_label,
                                edge_weight_prop, node_weight_prop,
                                max_iterations, damping_factor);

  return neug::execution::Context();
}

}  // namespace gds
}  // namespace neug