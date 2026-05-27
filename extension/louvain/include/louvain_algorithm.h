/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace neug {
namespace louvain {

/// @brief Weighted edge in the internal graph representation.
struct WeightedEdge {
  int64_t target;
  double weight;
};

/// @brief In-memory graph for Louvain algorithm.
///
/// Stores adjacency lists with optional weights. Supports both directed
/// and undirected graphs. For undirected graphs each edge (u,v) is stored
/// in both u's and v's adjacency lists.
class LouvainGraph {
 public:
  LouvainGraph() = default;

  /// @brief Add a vertex.
  void AddVertex(int64_t id);

  /// @brief Add an edge with optional weight.
  /// For undirected graphs, call only once per edge (u,v) — the reverse
  /// edge is added automatically.
  void AddEdge(int64_t src, int64_t dst, double weight = 1.0,
               bool directed = false);

  /// @brief Get the number of vertices.
  size_t num_vertices() const { return vertices_.size(); }

  /// @brief Get all vertex ids.
  const std::vector<int64_t>& vertex_ids() const { return vertices_; }

  /// @brief Get outgoing neighbors of a vertex.
  const std::vector<WeightedEdge>& neighbors(int64_t v) const;

  /// @brief Check if vertex exists.
  bool has_vertex(int64_t v) const {
    return adj_out_.count(v) > 0;
  }

 private:
  std::vector<int64_t> vertices_;
  std::unordered_map<int64_t, std::vector<WeightedEdge>> adj_out_;
  std::unordered_map<int64_t, std::vector<WeightedEdge>> adj_in_;
  static const std::vector<WeightedEdge> kEmptyEdges;
};

/// @brief Result of Louvain community detection.
struct LouvainResult {
  /// Maps vertex id -> community id (0-based).
  std::unordered_map<int64_t, int64_t> communities;
  /// Final modularity value.
  double modularity = 0.0;
  /// Number of communities found.
  int64_t num_communities = 0;
};

/// @brief Run the Louvain community detection algorithm.
///
/// @param graph     The input graph.
/// @param directed  Whether the graph is directed.
/// @param resolution Resolution parameter (gamma). Values > 1 favor smaller
///                   communities, < 1 favor larger communities.
/// @param threshold Modularity gain threshold for convergence.
/// @return LouvainResult containing community assignments.
LouvainResult RunLouvain(const LouvainGraph& graph, bool directed = false,
                         double resolution = 1.0,
                         double threshold = 1e-7);

}  // namespace louvain
}  // namespace neug
