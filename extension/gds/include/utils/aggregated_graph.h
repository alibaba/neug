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

#include <algorithm>
#include <cstdint>
#include <random>
#include <unordered_map>
#include <vector>

namespace neug {
namespace gds {
namespace community {

/// Aggregated (contracted) graph where each node represents a community
/// from the previous level. Self-loops encode intra-community edge weight.
struct AggregatedGraph {
  size_t num_nodes = 0;
  size_t num_edges = 0;  // total entries in adj/weights (includes self-loops)
  std::vector<uint32_t> nodes;      // active super-node IDs (community IDs)
  std::vector<size_t> offsets;      // CSR offsets, size = num_nodes + 1
  std::vector<uint32_t> adj;        // neighbor super-node IDs
  std::vector<double> weights;      // edge weights to neighbors
  std::vector<double> degree;       // weighted degree per super-node
  std::vector<double> self_loop;    // intra-community edge weight
  std::vector<uint32_t> community;  // community assignment (init: identity)
  std::vector<double> stot;         // community degree totals
};

/// Build an aggregated graph from the current community assignment.
/// @param valid_vertices  Active vertex IDs in the original graph.
/// @param community       Community assignment per vertex (indexed by gid).
/// @param degree          Degree per vertex (indexed by gid).
/// @param adj_offsets     CSR offsets of the original graph (over valid verts).
/// @param adj             CSR adjacency of the original graph.
/// @param adj_weights     CSR edge weights of the original graph.
inline AggregatedGraph build_aggregated_graph(
    const std::vector<uint32_t>& valid_vertices, const uint32_t* community,
    const double* degree, const std::vector<size_t>& adj_offsets,
    const std::vector<uint32_t>& adj, const std::vector<double>& adj_weights) {
  AggregatedGraph agg;

  // Collect unique communities
  std::unordered_map<uint32_t, size_t> com_to_idx;
  com_to_idx.reserve(valid_vertices.size());
  for (uint32_t v : valid_vertices) {
    uint32_t c = community[v];
    if (com_to_idx.find(c) == com_to_idx.end()) {
      size_t idx = agg.nodes.size();
      com_to_idx[c] = idx;
      agg.nodes.push_back(c);
    }
  }
  agg.num_nodes = agg.nodes.size();

  // Degree and self-loop from original vertex data
  agg.degree.resize(agg.num_nodes, 0.0);
  agg.self_loop.resize(agg.num_nodes, 0.0);
  for (size_t i = 0; i < agg.num_nodes; ++i) {
    uint32_t com_id = agg.nodes[i];
    // self_loop will be computed from edges below
    (void) com_id;
  }
  // Sum degrees per community
  for (uint32_t v : valid_vertices) {
    size_t idx = com_to_idx[community[v]];
    agg.degree[idx] += degree[v];
  }

  // Build inter-community edge weights
  // Use a map-of-maps approach for correctness, then convert to CSR
  std::vector<std::unordered_map<uint32_t, double>> edge_map(agg.num_nodes);
  for (size_t vi = 0; vi < valid_vertices.size(); ++vi) {
    uint32_t v = valid_vertices[vi];
    uint32_t cv = community[v];
    size_t ci = com_to_idx[cv];
    for (size_t e = adj_offsets[vi]; e < adj_offsets[vi + 1]; ++e) {
      uint32_t u = adj[e];
      uint32_t cu = community[u];
      size_t cj = com_to_idx[cu];
      if (ci == cj) {
        agg.self_loop[ci] += adj_weights[e];
      } else {
        edge_map[ci][static_cast<uint32_t>(cj)] += adj_weights[e];
      }
    }
  }

  // Convert to CSR
  agg.offsets.resize(agg.num_nodes + 1, 0);
  for (size_t i = 0; i < agg.num_nodes; ++i)
    agg.offsets[i + 1] = agg.offsets[i] + edge_map[i].size();
  agg.num_edges = agg.offsets[agg.num_nodes];
  agg.adj.resize(agg.num_edges);
  agg.weights.resize(agg.num_edges);
  for (size_t i = 0; i < agg.num_nodes; ++i) {
    size_t pos = agg.offsets[i];
    for (auto& [nbr, w] : edge_map[i]) {
      agg.adj[pos] = nbr;
      agg.weights[pos] = w;
      ++pos;
    }
  }

  // Initialize community assignment (identity) and stot
  agg.community.resize(agg.num_nodes);
  agg.stot.resize(agg.num_nodes, 0.0);
  for (size_t i = 0; i < agg.num_nodes; ++i) {
    agg.community[i] = static_cast<uint32_t>(i);
    agg.stot[i] = agg.degree[i];
  }

  return agg;
}

/// Perform one level of local moving on the aggregated graph.
/// Uses the same modularity-gain formula as the original graph.
/// @return true if any node was moved.
inline bool one_level_aggregated(AggregatedGraph& agg, double m,
                                 double resolution,
                                 std::vector<uint32_t>& scratch_gen,
                                 std::vector<double>& scratch_cw) {
  const size_t n = agg.num_nodes;
  std::vector<uint32_t> order(n);
  for (size_t i = 0; i < n; ++i)
    order[i] = static_cast<uint32_t>(i);
  std::mt19937 rng(42);
  std::shuffle(order.begin(), order.end(), rng);

  bool improved = false;
  uint32_t gen_val = 0;
  std::vector<uint32_t> touched;
  touched.reserve(64);

  for (int pass = 0; pass < 10; ++pass) {
    bool moved = false;
    for (size_t oi = 0; oi < n; ++oi) {
      uint32_t u = order[oi];
      uint32_t cur_com = agg.community[u];
      double du = agg.degree[u];
      ++gen_val;
      if (gen_val == 0) {  // overflow guard
        std::fill(scratch_gen.begin(), scratch_gen.end(), 0);
        gen_val = 1;
      }
      touched.clear();

      for (size_t e = agg.offsets[u]; e < agg.offsets[u + 1]; ++e) {
        uint32_t v = agg.adj[e];
        uint32_t com = agg.community[v];
        if (scratch_gen[com] != gen_val) {
          scratch_gen[com] = gen_val;
          scratch_cw[com] = 0.0;
          touched.push_back(com);
        }
        scratch_cw[com] += agg.weights[e];
      }

      double ws = (scratch_gen[cur_com] == gen_val) ? scratch_cw[cur_com] : 0.0;
      double sm = agg.stot[cur_com] - du;
      uint32_t best = cur_com;
      double best_gain = 0.0;
      for (uint32_t com : touched) {
        if (com == cur_com)
          continue;
        double wc = scratch_cw[com];
        double gain = (wc - ws) / m -
                      resolution * agg.stot[com] * du / (2.0 * m * m) +
                      resolution * sm * du / (2.0 * m * m);
        if (gain > best_gain) {
          best_gain = gain;
          best = com;
        }
      }
      if (best != cur_com) {
        agg.stot[cur_com] -= du;
        agg.stot[best] += du;
        agg.community[u] = best;
        moved = true;
        improved = true;
      }
    }
    if (!moved)
      break;
  }
  return improved;
}

/// Propagate aggregated-graph community assignments back to original vertices.
/// @param valid_vertices  Active vertex IDs in the original graph.
/// @param community       Original community array (modified in place).
/// @param agg             The aggregated graph with updated community[].
inline void propagate_aggregated_communities(
    const std::vector<uint32_t>& valid_vertices, uint32_t* community,
    const AggregatedGraph& agg) {
  // Build mapping: original community ID → aggregated community index
  // agg.nodes[i] = original community ID for super-node i
  // agg.community[i] = new community index for super-node i
  // We need: original_com_id → new_original_com_id
  // new_original_com_id = agg.nodes[agg.community[i]]
  std::unordered_map<uint32_t, uint32_t> remap;
  remap.reserve(agg.num_nodes);
  for (size_t i = 0; i < agg.num_nodes; ++i) {
    uint32_t orig_com = agg.nodes[i];
    uint32_t new_com_idx = agg.community[i];
    uint32_t new_com_id = agg.nodes[new_com_idx];
    remap[orig_com] = new_com_id;
  }
  for (uint32_t v : valid_vertices) {
    uint32_t old_com = community[v];
    auto it = remap.find(old_com);
    if (it != remap.end())
      community[v] = it->second;
  }
}

}  // namespace community
}  // namespace gds
}  // namespace neug
