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

#include "louvain_algorithm.h"

#include <algorithm>
#include <cmath>
#include <map>
#include <numeric>
#include <random>
#include <set>

namespace neug {
namespace louvain {

const std::vector<WeightedEdge> LouvainGraph::kEmptyEdges;

void LouvainGraph::AddVertex(int64_t id) {
  if (adj_out_.count(id) == 0) {
    vertices_.push_back(id);
    adj_out_[id];
    adj_in_[id];
  }
}

void LouvainGraph::AddEdge(int64_t src, int64_t dst, double weight,
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

const std::vector<WeightedEdge>& LouvainGraph::neighbors(int64_t v) const {
  auto it = adj_out_.find(v);
  if (it != adj_out_.end()) {
    return it->second;
  }
  return kEmptyEdges;
}

// ---------------------------------------------------------------------------
// Internal structures for the multi-level Louvain algorithm.
// ---------------------------------------------------------------------------

namespace {

/// @brief Internal graph used during Louvain iterations.
/// Nodes are identified by sequential integers [0, n).
struct InternalGraph {
  size_t n = 0;
  // adj[u] = list of (neighbor, weight)
  std::vector<std::vector<std::pair<size_t, double>>> adj;
  // For directed graphs: in_adj[u] = list of (neighbor, weight)
  std::vector<std::vector<std::pair<size_t, double>>> in_adj;
  // Original node sets: nodes[u] = set of original vertices merged into u
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

/// @brief Compute weighted degree (sum of edge weights) for undirected graph.
double weighted_degree(const InternalGraph& g, size_t u) {
  double deg = 0;
  for (auto& [v, w] : g.adj[u]) {
    deg += w;
  }
  return deg;
}

/// @brief Compute weighted out-degree for directed graph.
double out_degree(const InternalGraph& g, size_t u) {
  double deg = 0;
  for (auto& [v, w] : g.adj[u]) {
    deg += w;
  }
  return deg;
}

/// @brief Compute weighted in-degree for directed graph.
double in_degree(const InternalGraph& g, size_t u) {
  double deg = 0;
  for (auto& [v, w] : g.in_adj[u]) {
    deg += w;
  }
  return deg;
}

/// @brief Calculate weights between node u and its neighbor communities.
std::unordered_map<size_t, double> neighbor_weights_undirected(
    const InternalGraph& g, size_t u) {
  std::unordered_map<size_t, double> weights;
  // Exclude self-loops
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

/// @brief One level of Louvain: local moving phase.
///
/// @param g        The current (possibly aggregated) graph.
/// @param m        Total edge weight of the original graph.
/// @param partition  Current partition (maps super-node -> set of original
///                   vertices). Modified in place.
/// @param resolution Resolution parameter.
/// @param directed  Whether the graph is directed.
/// @param rng       Random number generator.
/// @return true if any improvement was made.
bool one_level(InternalGraph& g, double m,
               std::vector<std::set<int64_t>>& partition,
               double resolution, bool directed, std::mt19937& rng) {
  size_t n = g.n;
  // node2com: maps node index -> community index
  std::vector<size_t> node2com(n);
  std::iota(node2com.begin(), node2com.end(), 0);

  // inner_partition tracks the current-level community membership
  // (using node indices of g, not original vertex ids)
  std::vector<std::set<size_t>> inner_partition(n);
  for (size_t i = 0; i < n; ++i) {
    inner_partition[i].insert(i);
  }

  // Compute degrees
  std::vector<double> degrees(n, 0.0);
  std::vector<double> in_degrees(n, 0.0);
  std::vector<double> out_degrees(n, 0.0);
  std::vector<double> Stot(n, 0.0);       // undirected
  std::vector<double> Stot_in(n, 0.0);    // directed
  std::vector<double> Stot_out(n, 0.0);   // directed

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

  // Random order
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

      // Get neighbor weights (excluding self-loops)
      auto weights2com_raw =
          directed ? neighbor_weights_directed(g, u)
                   : neighbor_weights_undirected(g, u);

      // Aggregate weights by community
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
        // Move u's original nodes from old community to new community
        auto& com_nodes = g.nodes[u];
        for (int64_t node : com_nodes) {
          partition[node2com[u]].erase(node);
          partition[best_com].insert(node);
        }
        inner_partition[node2com[u]].erase(u);
        inner_partition[best_com].insert(u);
        improvement = true;
        nb_moves++;
        node2com[u] = best_com;
      }
    }
  }

  // Remove empty communities
  partition.erase(
      std::remove_if(partition.begin(), partition.end(),
                     [](const std::set<int64_t>& s) { return s.empty(); }),
      partition.end());

  return improvement;
}

/// @brief Generate a new (aggregated) graph based on the current partition.
InternalGraph gen_graph(const InternalGraph& g,
                        const std::vector<std::set<size_t>>& inner_partition) {
  // Build node -> community map
  std::unordered_map<size_t, size_t> node2com;
  for (size_t i = 0; i < inner_partition.size(); ++i) {
    for (size_t node : inner_partition[i]) {
      node2com[node] = i;
    }
  }

  // Count non-empty communities
  std::vector<size_t> non_empty;
  for (size_t i = 0; i < inner_partition.size(); ++i) {
    if (!inner_partition[i].empty()) {
      non_empty.push_back(i);
    }
  }
  std::unordered_map<size_t, size_t> com_remap;
  for (size_t i = 0; i < non_empty.size(); ++i) {
    com_remap[non_empty[i]] = i;
  }

  InternalGraph new_g;
  new_g.Init(non_empty.size());

  // Set the original node sets for each super-node
  for (size_t i = 0; i < non_empty.size(); ++i) {
    new_g.nodes[i].clear();
    for (size_t node : inner_partition[non_empty[i]]) {
      for (int64_t orig : g.nodes[node]) {
        new_g.nodes[i].push_back(orig);
      }
    }
  }

  // Aggregate edge weights
  // Use map to accumulate weights between community pairs
  std::map<std::pair<size_t, size_t>, double> edge_weights;
  for (size_t u = 0; u < g.n; ++u) {
    size_t com_u = com_remap.at(node2com.at(u));
    for (auto& [v, w] : g.adj[u]) {
      size_t com_v = com_remap.at(node2com.at(v));
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
/// @param partition Uses original vertex IDs (from g.nodes).
double compute_modularity(const InternalGraph& g,
                          const std::vector<std::set<int64_t>>& partition,
                          double resolution, bool directed) {
  double m = 0;
  for (size_t u = 0; u < g.n; ++u) {
    for (auto& [v, w] : g.adj[u]) {
      m += w;
    }
  }
  if (!directed) {
    m /= 2.0;
  }
  if (m == 0) return 0;

  // Build node_index -> community map.
  // partition[i] contains original vertex IDs; g.nodes[u] also contains
  // original vertex IDs. Map each node index u to its community.
  std::unordered_map<int64_t, size_t> orig_to_com;
  for (size_t i = 0; i < partition.size(); ++i) {
    for (int64_t node : partition[i]) {
      orig_to_com[node] = i;
    }
  }
  // Map node index -> community via first original vertex
  std::vector<size_t> node2com(g.n, SIZE_MAX);
  for (size_t u = 0; u < g.n; ++u) {
    if (!g.nodes[u].empty()) {
      auto it = orig_to_com.find(g.nodes[u][0]);
      if (it != orig_to_com.end()) {
        node2com[u] = it->second;
      }
    }
  }

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
    // Iterate over all directed edges; each undirected edge is seen twice,
    // so we divide the final sum by 2 to get the standard Newman modularity:
    //   Q = (1/2m) * Σ [A_{ij} - γ k_i k_j / (2m)] δ(c_i, c_j)
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

LouvainResult RunLouvain(const LouvainGraph& graph, bool directed,
                         double resolution, double threshold) {
  LouvainResult result;

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

  // Add edges (avoid duplicates for undirected: use src < dst check)
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
  if (!directed) {
    m /= 2.0;
  }

  if (m == 0) {
    // No edges: each vertex is its own community
    for (size_t i = 0; i < n; ++i) {
      result.communities[int2ext[i]] = static_cast<int64_t>(i);
    }
    result.num_communities = static_cast<int64_t>(n);
    result.modularity = 0;
    return result;
  }

  // Initial partition: each node in its own community
  // partition[i] = set of original vertex ids in community i
  std::vector<std::set<int64_t>> partition(n);
  for (size_t i = 0; i < n; ++i) {
    partition[i].insert(static_cast<int64_t>(i));
  }

  std::mt19937 rng(42);  // Fixed seed for reproducibility

  // Iterative Louvain
  double prev_mod =
      compute_modularity(g, partition, resolution, directed);
  bool improving = true;

  while (improving) {
    // Phase 1: local moving
    improving = one_level(g, m, partition, resolution, directed, rng);

    if (!improving) break;

    // Build inner_partition for gen_graph:
    // inner_partition[i] = set of node indices in current g that belong to
    // community i. We need to reconstruct from partition.
    // First, find which communities are non-empty
    // Re-map partition sets to inner_partition
    std::unordered_map<int64_t, size_t> orig_node_to_com;
    for (size_t i = 0; i < partition.size(); ++i) {
      for (int64_t node : partition[i]) {
        orig_node_to_com[node] = i;
      }
    }

    // inner_partition: for each community, which node indices of g are in it
    // Since g.nodes[u] contains original vertex indices, we check:
    //   for each node u in g, find which community its original vertices
    //   belong to.
    std::vector<std::set<size_t>> inner_partition(partition.size());
    for (size_t u = 0; u < g.n; ++u) {
      if (!g.nodes[u].empty()) {
        int64_t first_orig = g.nodes[u][0];
        auto it = orig_node_to_com.find(first_orig);
        if (it != orig_node_to_com.end()) {
          inner_partition[it->second].insert(u);
        }
      }
    }

    double new_mod =
        compute_modularity(g, partition, resolution, directed);
    if (new_mod - prev_mod <= threshold) {
      break;
    }
    prev_mod = new_mod;

    // Phase 2: aggregate graph
    g = gen_graph(g, inner_partition);

    // Rebuild partition to match new graph's node indexing
    std::vector<std::set<int64_t>> new_partition(g.n);
    for (size_t i = 0; i < g.n; ++i) {
      for (int64_t orig : g.nodes[i]) {
        new_partition[i].insert(orig);
      }
    }
    partition = std::move(new_partition);
  }

  // Build result: assign community IDs
  // After aggregation, partition[i] contains original vertex IDs from the
  // initial graph (which are internal indices 0..n-1).
  std::set<int64_t> community_ids;
  for (size_t i = 0; i < partition.size(); ++i) {
    if (!partition[i].empty()) {
      int64_t com_id = static_cast<int64_t>(community_ids.size());
      community_ids.insert(com_id);
      for (int64_t node : partition[i]) {
        auto idx = static_cast<size_t>(node);
        if (idx < n) {
          result.communities[int2ext[idx]] = com_id;
        }
      }
    }
  }
  result.num_communities = static_cast<int64_t>(community_ids.size());
  result.modularity = prev_mod;

  return result;
}

}  // namespace louvain
}  // namespace neug
