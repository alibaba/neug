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
#include <vector>

namespace neug {
namespace leiden {

/// @brief Weighted edge in the internal graph representation.
struct WeightedEdge {
  int64_t target;
  double weight;
};

/// @brief In-memory graph for Leiden algorithm.
class LeidenGraph {
 public:
  LeidenGraph() = default;

  void AddVertex(int64_t id);
  void AddEdge(int64_t src, int64_t dst, double weight = 1.0,
               bool directed = false);

  size_t num_vertices() const { return vertices_.size(); }
  const std::vector<int64_t>& vertex_ids() const { return vertices_; }
  const std::vector<WeightedEdge>& neighbors(int64_t v) const;
  bool has_vertex(int64_t v) const { return adj_out_.count(v) > 0; }

 private:
  std::vector<int64_t> vertices_;
  std::unordered_map<int64_t, std::vector<WeightedEdge>> adj_out_;
  std::unordered_map<int64_t, std::vector<WeightedEdge>> adj_in_;
  static const std::vector<WeightedEdge> kEmptyEdges;
};

/// @brief Result of Leiden community detection.
struct LeidenResult {
  std::unordered_map<int64_t, int64_t> communities;
  double modularity = 0.0;
  int64_t num_communities = 0;
};

/// @brief Run the Leiden community detection algorithm.
///
/// The Leiden algorithm improves upon Louvain by adding a refinement phase
/// that can split communities, leading to better community structure,
/// especially for large networks.
///
/// @param graph       The input graph.
/// @param directed    Whether the graph is directed.
/// @param resolution  Resolution parameter (gamma). Values > 1 favor smaller
///                    communities, < 1 favor larger communities.
/// @param threshold   Modularity gain threshold for convergence.
/// @param n_iterations Maximum number of outer iterations.
/// @return LeidenResult containing community assignments.
LeidenResult RunLeiden(const LeidenGraph& graph, bool directed = false,
                       double resolution = 1.0, double threshold = 1e-7,
                       int n_iterations = 100);

}  // namespace leiden
}  // namespace neug
