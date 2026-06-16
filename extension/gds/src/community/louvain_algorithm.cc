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

#include "community/louvain_algorithm.h"

#include <algorithm>
#include <cmath>
#include <numeric>
#include <random>
#include <unordered_map>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "utils/parallel_utils.h"

namespace neug {
namespace gds {
namespace community {

Louvain::Louvain(const StorageReadInterface& graph, label_t vertex_label,
                 label_t edge_label, double resolution, double threshold,
                 int concurrency)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_label_(edge_label),
      resolution_(resolution),
      threshold_(threshold),
      concurrency_(concurrency) {
  const auto& vertex_set = graph.GetVertexSet(vertex_label);
  valid_vertices_.reserve(vertex_set.size());
  vid_t max_vid = 0;
  for (const auto& v : vertex_set) {
    valid_vertices_.push_back(v);
    if (v > max_vid) max_vid = v;
  }
  vertex_count_ = valid_vertices_.size();

  // Allocate arrays indexed by vid_t
  // For contiguous vid_t values, size = max_vid + 1 ≈ vertex_count_
  size_t array_size = static_cast<size_t>(max_vid) + 1;
  community_ = std::make_unique<uint32_t[]>(array_size);
  degree_ = std::make_unique<double[]>(array_size);
  stot_ = std::make_unique<double[]>(array_size);

  // Initialize: each vertex in its own community
  for (vid_t v : valid_vertices_) {
    community_[v] = v;
    stot_[v] = 0;
    degree_[v] = 0;
  }
}

void Louvain::compute() {
  auto oe_view = graph_.GetGenericOutgoingGraphView(
      vertex_label_, vertex_label_, edge_label_);
  auto ie_view = graph_.GetGenericIncomingGraphView(
      vertex_label_, vertex_label_, edge_label_);

  // Compute degrees: for undirected graph, degree = out_degree + in_degree
  ParallelUtils::parallel_for(
      valid_vertices_.data(), valid_vertices_.size(),
      [&](vid_t v, int /*tid*/) {
        double deg = 0;
        auto oes = oe_view.get_edges(v);
        for (auto it = oes.begin(); it != oes.end(); ++it) deg += 1.0;
        auto ies = ie_view.get_edges(v);
        for (auto it = ies.begin(); it != ies.end(); ++it) deg += 1.0;
        degree_[v] = deg;
      },
      concurrency_);

  // Compute total edge weight m (for undirected: each edge counted once)
  // We count out-edges only, which gives us m for undirected graphs
  m_ = 0;
  for (vid_t v : valid_vertices_) {
    auto oes = oe_view.get_edges(v);
    for (auto it = oes.begin(); it != oes.end(); ++it) m_ += 1.0;
  }
  // For undirected: m = total_edges (not 2 * total_edges)
  // The out-degree counts each edge once, so m_ is correct.

  if (m_ == 0) {
    modularity_ = 0;
    return;
  }

  // Initialize stot
  for (vid_t v : valid_vertices_) {
    stot_[v] = degree_[v];
  }

  // Iterate
  for (int level = 0; level < 100; ++level) {
    bool improved = one_level();
    if (!improved) break;
  }

  // Compute final modularity
  modularity_ = 0;
  for (vid_t v : valid_vertices_) {
    auto oes = oe_view.get_edges(v);
    for (auto it = oes.begin(); it != oes.end(); ++it) {
      vid_t u = *it;
      if (community_[v] == community_[u]) {
        modularity_ += 1.0 / (2.0 * m_) -
                       degree_[v] * degree_[u] / (4.0 * m_ * m_);
      }
    }
  }
}

bool Louvain::one_level() {
  auto oe_view = graph_.GetGenericOutgoingGraphView(
      vertex_label_, vertex_label_, edge_label_);
  auto ie_view = graph_.GetGenericIncomingGraphView(
      vertex_label_, vertex_label_, edge_label_);

  std::vector<vid_t> order = valid_vertices_;
  std::mt19937 rng(42);
  std::shuffle(order.begin(), order.end(), rng);

  bool improved = false;

  for (int pass = 0; pass < 10; ++pass) {
    bool moved = false;
    for (vid_t u : order) {
      uint32_t cur_com = community_[u];
      double deg_u = degree_[u];

      // Remove u from its community
      stot_[cur_com] -= deg_u;

      // Aggregate neighbor weights by community
      std::unordered_map<uint32_t, double> nbr_com_wt;

      auto oes = oe_view.get_edges(u);
      for (auto it = oes.begin(); it != oes.end(); ++it) {
        vid_t v = *it;
        if (v == u) continue;
        nbr_com_wt[community_[v]] += 1.0;
      }
      auto ies = ie_view.get_edges(u);
      for (auto it = ies.begin(); it != ies.end(); ++it) {
        vid_t v = *it;
        if (v == u) continue;
        nbr_com_wt[community_[v]] += 1.0;
      }

      double w_self = nbr_com_wt.count(cur_com) ? nbr_com_wt[cur_com] : 0.0;

      // Find best community
      uint32_t best_com = cur_com;
      double best_gain = 0.0;

      for (auto& [com, w_com] : nbr_com_wt) {
        double gain = (w_com - w_self) / m_ +
                      resolution_ * (stot_[cur_com] - stot_[com]) * deg_u /
                          (2.0 * m_ * m_);
        if (gain > best_gain) {
          best_gain = gain;
          best_com = com;
        }
      }

      stot_[best_com] += deg_u;
      if (best_com != cur_com) {
        community_[u] = best_com;
        moved = true;
        improved = true;
      }
    }
    if (!moved) break;
  }

  return improved;
}

void Louvain::sink(execution::Context& ctx, int node_alias,
                   int community_alias) {
  execution::MSVertexColumnBuilder builder(vertex_label_);
  builder.reserve(valid_vertices_.size());
  execution::ValueColumnBuilder<int64_t> community_builder;
  community_builder.reserve(valid_vertices_.size());

  // Remap communities to contiguous [0, num_coms)
  std::unordered_map<uint32_t, uint32_t> com_remap;
  uint32_t next_id = 0;

  for (vid_t v : valid_vertices_) {
    uint32_t c = community_[v];
    if (com_remap.find(c) == com_remap.end()) com_remap[c] = next_id++;
    builder.push_back_opt(v);
    community_builder.push_back_opt(static_cast<int64_t>(com_remap[c]));
  }

  execution::DataChunk chunk;
  chunk.set(node_alias, builder.finish());
  chunk.set(community_alias, community_builder.finish());
  ctx.append_chunk(std::move(chunk));
}

LouvainResult RunLouvain(const StorageReadInterface& graph,
                         label_t vertex_label, label_t edge_label,
                         bool directed, double resolution, double threshold,
                         int concurrency) {
  (void)directed;  // TODO: directed support

  Louvain louvain(graph, vertex_label, edge_label, resolution, threshold,
                  concurrency);
  louvain.compute();

  LouvainResult result;
  // Result is stored internally; sink() method is used for output
  result.modularity = 0;
  result.num_communities = 0;
  return result;
}

}  // namespace community
}  // namespace gds
}  // namespace neug
