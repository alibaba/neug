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

#include "community/leiden_algorithm.h"

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

Leiden::Leiden(const StorageReadInterface& graph, label_t vertex_label,
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

  size_t array_size = static_cast<size_t>(max_vid) + 1;
  community_ = std::make_unique<uint32_t[]>(array_size);
  degree_ = std::make_unique<double[]>(array_size);
  stot_ = std::make_unique<double[]>(array_size);

  for (vid_t v : valid_vertices_) {
    community_[v] = v;
    stot_[v] = 0;
    degree_[v] = 0;
  }
}

void Leiden::compute() {
  auto oe_view = graph_.GetGenericOutgoingGraphView(
      vertex_label_, vertex_label_, edge_label_);
  auto ie_view = graph_.GetGenericIncomingGraphView(
      vertex_label_, vertex_label_, edge_label_);

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

  m_ = 0;
  for (vid_t v : valid_vertices_) {
    auto oes = oe_view.get_edges(v);
    for (auto it = oes.begin(); it != oes.end(); ++it) m_ += 1.0;
  }

  if (m_ == 0) {
    modularity_ = 0;
    return;
  }

  for (vid_t v : valid_vertices_) stot_[v] = degree_[v];

  for (int level = 0; level < 100; ++level) {
    bool improved = local_moving_phase();
    if (!improved) break;

    refine();

    double new_mod = 0;
    for (vid_t v : valid_vertices_) {
      auto oes = oe_view.get_edges(v);
      for (auto it = oes.begin(); it != oes.end(); ++it) {
        vid_t u = *it;
        if (community_[v] == community_[u]) {
          new_mod += 1.0 / (2.0 * m_) -
                     degree_[v] * degree_[u] / (4.0 * m_ * m_);
        }
      }
    }
    if (std::abs(new_mod - modularity_) < threshold_) break;
    modularity_ = new_mod;
  }
}

bool Leiden::local_moving_phase() {
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
      stot_[cur_com] -= deg_u;

      std::unordered_map<uint32_t, double> nbr_com_wt;
      auto oes = oe_view.get_edges(u);
      for (auto it = oes.begin(); it != oes.end(); ++it) {
        vid_t v = *it;
        if (v != u) nbr_com_wt[community_[v]] += 1.0;
      }
      auto ies = ie_view.get_edges(u);
      for (auto it = ies.begin(); it != ies.end(); ++it) {
        vid_t v = *it;
        if (v != u) nbr_com_wt[community_[v]] += 1.0;
      }

      double w_self = nbr_com_wt.count(cur_com) ? nbr_com_wt[cur_com] : 0.0;

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

void Leiden::refine() {
  std::unordered_map<uint32_t, std::vector<vid_t>> com_nodes;
  for (vid_t v : valid_vertices_) {
    com_nodes[community_[v]].push_back(v);
  }

  auto oe_view = graph_.GetGenericOutgoingGraphView(
      vertex_label_, vertex_label_, edge_label_);

  uint32_t next_com = 0;

  for (auto& [com_id, nodes] : com_nodes) {
    if (nodes.size() <= 1) {
      for (vid_t v : nodes) community_[v] = next_com++;
      continue;
    }

    // Build sub-community map
    std::unordered_map<vid_t, uint32_t> sub_com;
    for (vid_t v : nodes) sub_com[v] = 0;

    std::vector<vid_t> order = nodes;
    std::mt19937 rng(42);
    std::shuffle(order.begin(), order.end(), rng);

    bool improved = true;
    uint32_t next_sub = 1;

    while (improved && next_sub < 50) {
      improved = false;
      for (vid_t u : order) {
        uint32_t cur_sc = sub_com[u];

        std::unordered_map<uint32_t, double> nbr_sc_wt;
        auto oes = oe_view.get_edges(u);
        for (auto it = oes.begin(); it != oes.end(); ++it) {
          vid_t v = *it;
          if (v != u && sub_com.count(v)) {
            nbr_sc_wt[sub_com[v]] += 1.0;
          }
        }

        double w_self = nbr_sc_wt.count(cur_sc) ? nbr_sc_wt[cur_sc] : 0.0;

        uint32_t best_sc = cur_sc;
        double best_gain = 0.0;

        for (auto& [sc, w_sc] : nbr_sc_wt) {
          double gain = w_sc - w_self;
          if (gain > best_gain) {
            best_gain = gain;
            best_sc = sc;
          }
        }

        // Try new sub-community
        double gain_new = -w_self;
        if (gain_new > best_gain) {
          best_gain = gain_new;
          best_sc = next_sub;
        }

        if (best_sc != cur_sc) {
          sub_com[u] = best_sc;
          if (best_sc == next_sub) next_sub++;
          improved = true;
        }
      }
    }

    // Map sub-communities to new community IDs
    std::unordered_map<uint32_t, uint32_t> sc_to_new;
    for (vid_t v : nodes) {
      uint32_t sc = sub_com[v];
      if (sc_to_new.find(sc) == sc_to_new.end()) {
        sc_to_new[sc] = next_com++;
      }
      community_[v] = sc_to_new[sc];
    }
  }
}

void Leiden::sink(execution::Context& ctx, int node_alias,
                  int community_alias) {
  execution::MSVertexColumnBuilder builder(vertex_label_);
  builder.reserve(valid_vertices_.size());
  execution::ValueColumnBuilder<int64_t> community_builder;
  community_builder.reserve(valid_vertices_.size());

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

LeidenResult RunLeiden(const StorageReadInterface& graph,
                       label_t vertex_label, label_t edge_label,
                       bool directed, double resolution, double threshold,
                       int concurrency) {
  (void)directed;

  Leiden leiden(graph, vertex_label, edge_label, resolution, threshold,
                concurrency);
  leiden.compute();

  LeidenResult result;
  result.modularity = 0;
  result.num_communities = 0;
  return result;
}

}  // namespace community
}  // namespace gds
}  // namespace neug
