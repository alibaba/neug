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

#include "impl/lcc_directed_impl.h"

#include <algorithm>
#include <cstdint>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "utils/parallel_utils.h"

namespace neug {
namespace gds {

LCCDirected::LCCDirected(const StorageReadInterface& graph,
                         label_t vertex_label, label_t edge_label,
                         int degree_threshold, int concurrency)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_label_(edge_label),
      degree_threshold_(degree_threshold),
      concurrency_(concurrency) {
  concurrency_ = std::max(concurrency_, 1);

  const auto& vertex_set = graph_.GetVertexSet(vertex_label_);
  vertices_.reserve(vertex_set.size());
  for (vid_t v : vertex_set) {
    vertices_.push_back(v);
  }

  degrees_.resize(vertex_set.size(), 0);
  unique_degrees_.resize(vertex_set.size(), 0);
  neighbors_.resize(vertex_set.size());
  neighbor_weights_.resize(vertex_set.size());
  triangles_.resize(vertex_set.size(), 0);
  lcc_.resize(vertex_set.size(), 0.0);
}

void LCCDirected::compute() {
  auto oe_view = graph_.GetGenericOutgoingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);
  auto ie_view = graph_.GetGenericIncomingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);

  ParallelUtils::parallel_for(
      vertices_.data(), vertices_.size(),
      [&](vid_t v, int tid) {
        const auto& oe_edges = oe_view.get_edges(v);
        int32_t degree = 0;
        for ([[maybe_unused]] auto it = oe_edges.begin(); it != oe_edges.end();
             ++it) {
          degree++;
        }

        const auto& ie_edges = ie_view.get_edges(v);
        for ([[maybe_unused]] auto it = ie_edges.begin(); it != ie_edges.end();
             ++it) {
          degree++;
        }

        degrees_[v] = degree;
      },
      concurrency_);

  ParallelUtils::parallel_for(
      vertices_.data(), vertices_.size(),
      [&](vid_t v, int tid) {
        if (degrees_[v] > degree_threshold_) {
          return;
        }

        std::vector<vid_t> merged;
        merged.reserve(degrees_[v]);

        const auto& oe = oe_view.get_edges(v);
        for (auto it = oe.begin(); it != oe.end(); ++it) {
          vid_t u = *it;
          if (u != v && degrees_[u] <= degree_threshold_) {
            merged.push_back(u);
          }
        }
        const auto& ie = ie_view.get_edges(v);
        for (auto it = ie.begin(); it != ie.end(); ++it) {
          vid_t u = *it;
          if (u != v && degrees_[u] <= degree_threshold_) {
            merged.push_back(u);
          }
        }

        std::sort(merged.begin(), merged.end());
        auto& nbrs = neighbors_[v];
        auto& weights = neighbor_weights_[v];
        nbrs.clear();
        weights.clear();
        nbrs.reserve(merged.size());
        weights.reserve(merged.size());

        for (size_t i = 0; i < merged.size();) {
          vid_t u = merged[i];
          size_t j = i + 1;
          while (j < merged.size() && merged[j] == u) {
            ++j;
          }
          nbrs.push_back(u);
          weights.push_back(static_cast<int32_t>(j - i));
          i = j;
        }
        unique_degrees_[v] = static_cast<int32_t>(nbrs.size());
      },
      concurrency_);

  // The directed local clustering coefficient of v is the number of directed
  // edges a->b between distinct members a, b of its neighborhood N(v), divided
  // by |N(v)| * (|N(v)| - 1).  N(v) is the set of vertices adjacent to v via an
  // in- or out-edge (neighbors_[v], already deduplicated and sorted).  Counting
  // directed edges among the neighborhood cannot be reduced to undirected
  // triangle enumeration, so we evaluate the definition directly.
  ParallelUtils::parallel_for(
      vertices_.data(), vertices_.size(),
      [&](vid_t v, int tid) {
        int32_t d = unique_degrees_[v];
        if (d <= 1 || degrees_[v] > degree_threshold_) {
          triangles_[v] = 0;
          lcc_[v] = 0.0;
          return;
        }
        const auto& nbrs = neighbors_[v];
        int64_t edge_count = 0;
        for (vid_t a : nbrs) {
          const auto& a_oe = oe_view.get_edges(a);
          for (auto it = a_oe.begin(); it != a_oe.end(); ++it) {
            vid_t b = *it;
            if (b != a && std::binary_search(nbrs.begin(), nbrs.end(), b)) {
              ++edge_count;
            }
          }
        }
        triangles_[v] = edge_count;
        lcc_[v] = static_cast<double>(edge_count) /
                  (static_cast<double>(d) * static_cast<double>(d - 1));
      },
      concurrency_);
}

void LCCDirected::sink(execution::Context& ctx, int node_alias, int lcc_alias) {
  execution::MSVertexColumnBuilder node_builder(vertex_label_);
  execution::ValueColumnBuilder<double> lcc_builder;
  lcc_builder.reserve(vertices_.size());

  for (vid_t v : vertices_) {
    lcc_builder.push_back_opt(lcc_[v]);
  }
  node_builder.append(vertex_label_, std::move(vertices_));

  execution::ContextChunk chunk;
  chunk.set(node_alias, node_builder.finish());
  chunk.set(lcc_alias, lcc_builder.finish());
  ctx.append_chunk(std::move(chunk));
}

}  // namespace gds
}  // namespace neug
