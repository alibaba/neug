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

#include "leiden_algorithm.h"

#include <algorithm>
#include <cmath>
#include <map>
#include <numeric>
#include <random>
#include <set>

namespace neug {
namespace leiden {

const std::vector<WeightedEdge> LeidenGraph::kEmptyEdges;

void LeidenGraph::AddVertex(int64_t id) {
  if (adj_out_.count(id) == 0) {
    vertices_.push_back(id);
    adj_out_[id];
    adj_in_[id];
  }
}

void LeidenGraph::AddEdge(int64_t src, int64_t dst, double weight,
                          bool directed) {
  AddVertex(src);
  AddVertex(dst);
  adj_out_[src].push_back({dst, weight});
  adj_in_[dst].push_back({src, weight});
  if (!directed) {
    adj_out_[dst].push_back({src, weight});
    adj_in_[src].push_back({dst, weight});
  }
}

const std::vector<WeightedEdge>& LeidenGraph::neighbors(int64_t v) const {
  auto it = adj_out_.find(v);
  if (it != adj_out_.end()) {
    return it->second;
  }
  return kEmptyEdges;
}

// ---------------------------------------------------------------------------
// Internal structures for the Leiden algorithm.
// ---------------------------------------------------------------------------

namespace {

struct InternalGraph {
  size_t n = 0;
  std::vector<std::vector<std::pair<size_t, double>>> adj;
  std::vector<std::vector<std::pair<size_t, double>>> in_adj;
  std::vector<std::vector<int64_t>> nodes;

  void Init(size_t num_nodes) {
    n = num_nodes;
    adj.resize(n);
    in_adj.resize(n);
    nodes.resize(n);
    for (size_t i = 0; i < n; ++i) {
      nodes[i] = {static_cast<int64_t>(i)};
    }
  }
};

double weighted_degree(const InternalGraph& g, size_t u) {
  double deg = 0;
  for (auto& [v, w] : g.adj[u]) {
    deg += w;
  }
  return deg;
}

double out_degree(const InternalGraph& g, size_t u) {
  double deg = 0;
  for (auto& [v, w] : g.adj[u]) {
    deg += w;
  }
  return deg;
}

double in_degree(const InternalGraph& g, size_t u) {
  double deg = 0;
  for (auto& [v, w] : g.in_adj[u]) {
    deg += w;
  }
  return deg;
}

std::unordered_map<size_t, double> neighbor_weights_undirected(
    const InternalGraph& g, size_t u) {
  std::unordered_map<size_t, double> weights;
  for (auto& [v, w] : g.adj[u]) {
    if (v != u) {
      weights[v] += w;
    }
  }
  return weights;
}

std::unordered_map<size_t, double> neighbor_weights_directed(
    const InternalGraph& g, size_t u) {
  std::unordered_map<size_t, double> weights;
  for (auto& [v, w] : g.adj[u]) {
    if (v != u) {
      weights[v] += w;
    }
  }
  for (auto& [v, w] : g.in_adj[u]) {
    if (v != u) {
      weights[v] += w;
    }
  }
  return weights;
}

/// @brief Leiden local moving phase.
/// Returns true if any improvement was made.
bool local_moving_phase(
    InternalGraph& g, double m,
    std::vector<size_t>& node2com,
    std::vector<std::set<size_t>>& inner_partition,
    double resolution, bool directed, std::mt19937& rng) {
  size_t n = g.n;

  std::vector<double> degrees(n, 0.0);
  std::vector<double> in_degrees(n, 0.0);
  std::vector<double> out_degrees(n, 0.0);
  std::vector<double> Stot(n, 0.0);
  std::vector<double> Stot_in(n, 0.0);
  std::vector<double> Stot_out(n, 0.0);

  if (directed) {
    for (size_t u = 0; u < n; ++u) {
      in_degrees[u] = in_degree(g, u);
      out_degrees[u] = out_degree(g, u);
      Stot_in[u] = in_degrees[u];
      Stot_out[u] = out_degrees[u];
    }
  } else {
    for (size_t u = 0; u < n; ++u) {
      degrees[u] = weighted_degree(g, u);
      Stot[u] = degrees[u];
    }
  }

  std::vector<size_t> rand_nodes(n);
  std::iota(rand_nodes.begin(), rand_nodes.end(), 0);
  std::shuffle(rand_nodes.begin(), rand_nodes.end(), rng);

  bool improvement = false;
  int nb_moves = 1;

  while (nb_moves > 0) {
    nb_moves = 0;
    for (size_t u : rand_nodes) {
      double best_mod = 0;
      size_t best_com = node2com[u];

      auto weights2com_raw =
          directed ? neighbor_weights_directed(g, u)
                   : neighbor_weights_undirected(g, u);

      std::unordered_map<size_t, double> weights2com;
      for (auto& [nbr, wt] : weights2com_raw) {
        weights2com[node2com[nbr]] += wt;
      }

      double remove_cost = 0;
      if (directed) {
        double in_deg = in_degrees[u];
        double out_deg = out_degrees[u];
        Stot_in[best_com] -= in_deg;
        Stot_out[best_com] -= out_deg;
        remove_cost =
            -weights2com[best_com] / m +
            resolution * (out_deg * Stot_in[best_com] +
                          in_deg * Stot_out[best_com]) /
                (m * m);
      } else {
        double deg = degrees[u];
        Stot[best_com] -= deg;
        remove_cost = -weights2com[best_com] / m +
                      resolution * (Stot[best_com] * deg) / (2 * m * m);
      }

      for (auto& [nbr_com, wt] : weights2com) {
        double gain = 0;
        if (directed) {
          double in_deg = in_degrees[u];
          double out_deg = out_degrees[u];
          gain = remove_cost + wt / m -
                 resolution * (out_deg * Stot_in[nbr_com] +
                               in_deg * Stot_out[nbr_com]) /
                     (m * m);
        } else {
          double deg = degrees[u];
          gain = remove_cost + wt / m -
                 resolution * (Stot[nbr_com] * deg) / (2 * m * m);
        }
        if (gain > best_mod) {
          best_mod = gain;
          best_com = nbr_com;
        }
      }

      if (directed) {
        Stot_in[best_com] += in_degrees[u];
        Stot_out[best_com] += out_degrees[u];
      } else {
        Stot[best_com] += degrees[u];
      }

      if (best_com != node2com[u]) {
        inner_partition[node2com[u]].erase(u);
        inner_partition[best_com].insert(u);
        improvement = true;
        nb_moves++;
        node2com[u] = best_com;
      }
    }
  }

  return improvement;
}

/// @brief Leiden refinement phase.
///
/// For each community, creates a subgraph of its nodes and runs a local
/// moving phase on it to potentially split the community into sub-communities.
/// This is the key difference from Louvain.
void refine_partition(
    const InternalGraph& g,
    const std::vector<size_t>& node2com,
    std::vector<size_t>& refined_com,
    double m, double resolution, bool directed, std::mt19937& rng) {
  size_t n = g.n;

  // Group nodes by community
  std::unordered_map<size_t, std::vector<size_t>> com_nodes;
  for (size_t u = 0; u < n; ++u) {
    com_nodes[node2com[u]].push_back(u);
  }

  // Initialize refined_com to node2com
  refined_com = node2com;

  // For each community, try to refine it
  size_t next_com_id = *std::max_element(node2com.begin(), node2com.end()) + 1;

  for (auto& [com_id, nodes] : com_nodes) {
    if (nodes.size() <= 1) continue;

    // Build subgraph for this community
    std::unordered_map<size_t, size_t> local_idx;
    for (size_t i = 0; i < nodes.size(); ++i) {
      local_idx[nodes[i]] = i;
    }

    InternalGraph sub_g;
    sub_g.Init(nodes.size());

    for (size_t i = 0; i < nodes.size(); ++i) {
      size_t u = nodes[i];
      for (auto& [v, w] : g.adj[u]) {
        auto it = local_idx.find(v);
        if (it != local_idx.end()) {
          sub_g.adj[i].push_back({it->second, w});
          sub_g.in_adj[it->second].push_back({i, w});
        }
      }
    }

    // Run local moving on subgraph
    std::vector<size_t> sub_node2com(nodes.size());
    std::iota(sub_node2com.begin(), sub_node2com.end(), 0);

    std::vector<std::set<size_t>> sub_partition(nodes.size());
    for (size_t i = 0; i < nodes.size(); ++i) {
      sub_partition[i].insert(i);
    }

    double sub_m = 0;
    for (size_t i = 0; i < sub_g.n; ++i) {
      for (auto& [j, w] : sub_g.adj[i]) {
        sub_m += w;
      }
    }
    if (!directed) sub_m /= 2.0;
    if (sub_m == 0) continue;

    local_moving_phase(sub_g, sub_m, sub_node2com, sub_partition,
                       resolution, directed, rng);

    // Check if any split occurred
    bool has_split = false;
    for (size_t i = 0; i < nodes.size(); ++i) {
      if (sub_node2com[i] != 0) {
        has_split = true;
        break;
      }
    }

    if (has_split) {
      // Assign new community IDs for split communities
      std::unordered_map<size_t, size_t> sub_com_to_new;
      for (size_t i = 0; i < nodes.size(); ++i) {
        if (sub_com_to_new.find(sub_node2com[i]) == sub_com_to_new.end()) {
          sub_com_to_new[sub_node2com[i]] = next_com_id++;
        }
        refined_com[nodes[i]] = sub_com_to_new[sub_node2com[i]];
      }
    }
  }
}

/// @brief Generate aggregated graph from partition.
InternalGraph gen_graph(const InternalGraph& g,
                        const std::vector<size_t>& node2com) {
  // Count non-empty communities and remap
  std::set<size_t> unique_coms(node2com.begin(), node2com.end());
  std::unordered_map<size_t, size_t> com_remap;
  size_t idx = 0;
  for (size_t c : unique_coms) {
    com_remap[c] = idx++;
  }

  InternalGraph new_g;
  new_g.Init(unique_coms.size());

  // Set original node sets
  for (size_t u = 0; u < g.n; ++u) {
    size_t new_com = com_remap[node2com[u]];
    for (int64_t orig : g.nodes[u]) {
      new_g.nodes[new_com].push_back(orig);
    }
  }

  // Aggregate edge weights
  std::map<std::pair<size_t, size_t>, double> edge_weights;
  for (size_t u = 0; u < g.n; ++u) {
    size_t com_u = com_remap[node2com[u]];
    for (auto& [v, w] : g.adj[u]) {
      size_t com_v = com_remap[node2com[v]];
      edge_weights[{com_u, com_v}] += w;
    }
  }

  for (auto& [edge, w] : edge_weights) {
    auto [cu, cv] = edge;
    new_g.adj[cu].push_back({cv, w});
    new_g.in_adj[cv].push_back({cu, w});
  }

  return new_g;
}

/// @brief Compute modularity of a partition.
double compute_modularity(const InternalGraph& g,
                          const std::vector<size_t>& node2com,
                          double resolution, bool directed) {
  double m = 0;
  for (size_t u = 0; u < g.n; ++u) {
    for (auto& [v, w] : g.adj[u]) {
      m += w;
    }
  }
  if (!directed) m /= 2.0;
  if (m == 0) return 0;

  double Q = 0;
  if (directed) {
    for (size_t u = 0; u < g.n; ++u) {
      for (auto& [v, w] : g.adj[u]) {
        if (node2com[u] == node2com[v]) {
          double ki_out = out_degree(g, u);
          double kj_in = in_degree(g, v);
          Q += w / m - resolution * ki_out * kj_in / (m * m);
        }
      }
    }
  } else {
    for (size_t u = 0; u < g.n; ++u) {
      for (auto& [v, w] : g.adj[u]) {
        if (node2com[u] == node2com[v]) {
          double ki = weighted_degree(g, u);
          double kj = weighted_degree(g, v);
          Q += w / (2 * m) - resolution * ki * kj / (4 * m * m);
        }
      }
    }
    Q /= 2.0;
  }
  return Q;
}

}  // namespace

LeidenResult RunLeiden(const LeidenGraph& graph, bool directed,
                       double resolution, double threshold,
                       int n_iterations) {
  LeidenResult result;

  const auto& vertices = graph.vertex_ids();
  size_t n = vertices.size();

  if (n == 0) {
    return result;
  }

  // Map external vertex ids to internal indices [0, n)
  std::unordered_map<int64_t, size_t> ext2int;
  std::vector<int64_t> int2ext(n);
  for (size_t i = 0; i < n; ++i) {
    ext2int[vertices[i]] = i;
    int2ext[i] = vertices[i];
  }

  // Build internal graph
  InternalGraph g;
  g.Init(n);

  std::set<std::pair<int64_t, int64_t>> added_edges;
  for (int64_t v : vertices) {
    size_t u_idx = ext2int[v];
    for (auto& edge : graph.neighbors(v)) {
      size_t v_idx = ext2int[edge.target];
      if (directed) {
        g.adj[u_idx].push_back({v_idx, edge.weight});
        g.in_adj[v_idx].push_back({u_idx, edge.weight});
      } else {
        auto key = std::make_pair(std::min(v, edge.target),
                                  std::max(v, edge.target));
        if (added_edges.find(key) == added_edges.end()) {
          added_edges.insert(key);
          g.adj[u_idx].push_back({v_idx, edge.weight});
          g.adj[v_idx].push_back({u_idx, edge.weight});
          g.in_adj[u_idx].push_back({v_idx, edge.weight});
          g.in_adj[v_idx].push_back({u_idx, edge.weight});
        }
      }
    }
  }

  // Compute total edge weight m
  double m = 0;
  for (size_t u = 0; u < n; ++u) {
    for (auto& [v, w] : g.adj[u]) {
      m += w;
    }
  }
  if (!directed) m /= 2.0;

  if (m == 0) {
    for (size_t i = 0; i < n; ++i) {
      result.communities[int2ext[i]] = static_cast<int64_t>(i);
    }
    result.num_communities = static_cast<int64_t>(n);
    result.modularity = 0;
    return result;
  }

  // Initial partition: each node in its own community
  std::vector<size_t> node2com(n);
  std::iota(node2com.begin(), node2com.end(), 0);

  std::mt19937 rng(42);

  double prev_mod = -1.0;

  for (int iter = 0; iter < n_iterations; ++iter) {
    // Phase 1: Local moving
    std::vector<std::set<size_t>> inner_partition(n);
    for (size_t i = 0; i < n; ++i) {
      inner_partition[node2com[i]].insert(i);
    }

    bool improved = local_moving_phase(g, m, node2com, inner_partition,
                                        resolution, directed, rng);

    if (!improved) break;

    // Phase 2: Refinement (key difference from Louvain)
    std::vector<size_t> refined_com;
    refine_partition(g, node2com, refined_com, m, resolution, directed, rng);

    // Use refined partition for aggregation
    double new_mod = compute_modularity(g, refined_com, resolution, directed);

    if (prev_mod >= 0 && new_mod - prev_mod <= threshold) {
      // Use the refined partition for final result
      node2com = refined_com;
      break;
    }
    prev_mod = new_mod;

    // Phase 3: Aggregation
    g = gen_graph(g, refined_com);

    // Rebuild node2com for new graph
    std::set<size_t> unique_coms(refined_com.begin(), refined_com.end());
    std::unordered_map<size_t, size_t> com_remap;
    size_t idx = 0;
    for (size_t c : unique_coms) {
      com_remap[c] = idx++;
    }

    n = g.n;
    node2com.resize(n);
    for (size_t u = 0; u < g.n; ++u) {
      // Find which original node maps to this super-node
      if (!g.nodes[u].empty()) {
        // The super-node inherits the community from its first original node
        // But since we aggregated from refined_com, we need to track it
        // Actually, gen_graph already groups by refined_com, so each super-node
        // corresponds to one community. We just need to assign sequential IDs.
        node2com[u] = u;  // Each super-node is its own community in new graph
      }
    }
  }

  // Build result: map back to original vertex IDs
  // After aggregation, g.nodes[u] contains the original vertex indices
  // We need to trace back through the aggregation levels
  // Since we track original nodes in g.nodes, we can directly map

  // Rebuild the mapping from the final graph
  // g.nodes[u] contains original vertex IDs (from initial graph)
  std::set<int64_t> community_ids;
  std::unordered_map<int64_t, int64_t> orig_to_com;

  for (size_t u = 0; u < g.n; ++u) {
    int64_t com_id = static_cast<int64_t>(u);
    for (int64_t orig : g.nodes[u]) {
      auto idx = static_cast<size_t>(orig);
      if (idx < int2ext.size()) {
        result.communities[int2ext[idx]] = com_id;
      }
    }
  }

  // Reassign community IDs to be 0-based contiguous
  std::unordered_map<int64_t, int64_t> com_remap;
  int64_t next_id = 0;
  for (auto& [vid, com] : result.communities) {
    if (com_remap.find(com) == com_remap.end()) {
      com_remap[com] = next_id++;
    }
    com = com_remap[com];
  }

  result.num_communities = next_id;
  result.modularity = prev_mod >= 0 ? prev_mod : 0;

  return result;
}

}  // namespace leiden
}  // namespace neug
