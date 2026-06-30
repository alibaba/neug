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

#include "impl/cdlp_impl.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <thread>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "utils/parallel_utils.h"

namespace neug {
namespace gds {

CDLP::CDLP(const StorageReadInterface& graph, label_t vertex_label,
           const execution::LabelTriplet& edge_triplet, int max_iterations,
           int concurrency)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_triplet_(edge_triplet),
      max_iterations_(max_iterations),
      concurrency_(concurrency) {}

// The plain CDLP runs over the whole projected graph; predicate filtering is
// handled by the separate CDLPPred variant.
void CDLP::init_communities() {
  auto vertex_set = graph_.GetVertexSet(vertex_label_);
  community_.reset(new int64_t[vertex_set.size()]);
  next_community_.reset(new int64_t[vertex_set.size()]);
  vertices_.clear();
  vertices_.reserve(vertex_set.size());

  auto id_column_holder = graph_.GetVertexPropColumn(vertex_label_, "id");
  auto id_column =
      dynamic_cast<const TypedRefColumn<int64_t>*>(id_column_holder.get());

  if (id_column != nullptr) {
    ParallelUtils::parallel_for(
        vertex_set,
        [&](vid_t v, int tid) {
          int64_t id = id_column->get_view(v);
          community_[v] = id;
          next_community_[v] = id;
        },
        concurrency_);
  } else {
    ParallelUtils::parallel_for(
        vertex_set,
        [&](vid_t v, int tid) {
          community_[v] = v;
          next_community_[v] = v;
        },
        concurrency_);
  }

  for (vid_t v : vertex_set) {
    vertices_.push_back(v);
  }
}

int64_t CDLP::get_majority_community(const CsrView& ie_view,
                                     const CsrView& oe_view, vid_t dst_vid,
                                     int64_t* buffer) const {
  size_t neighbor_count = 0;
  int64_t best = -1;
  int32_t max_count = 0;
  const auto ie_edges = ie_view.get_edges(dst_vid);

  for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
    const auto nbr = *it;
    if (community_[nbr] != std::numeric_limits<int64_t>::max()) {
      int64_t comm = community_[nbr];
      buffer[neighbor_count++] = comm;
      if (comm == best) {
        max_count++;
      } else {
        max_count--;
        if (max_count <= 0) {
          best = comm;
          max_count = 1;
        }
      }
    }
  }
  const auto oe_edges = oe_view.get_edges(dst_vid);
  for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
    const auto nbr = *it;
    if (community_[nbr] != std::numeric_limits<int64_t>::max()) {
      int64_t comm = community_[nbr];
      buffer[neighbor_count++] = comm;
      if (comm == best) {
        max_count++;
      } else {
        max_count--;
        if (max_count <= 0) {
          best = comm;
          max_count = 1;
        }
      }
    }
  }
  if (neighbor_count == 0) {
    return community_[dst_vid];
  }
  max_count = 0;
  for (size_t i = 0; i < neighbor_count; ++i) {
    max_count += (buffer[i] == best) ? 1 : 0;
  }
  if (max_count * 2 > neighbor_count) {
    return best;
  }

  max_count = 0;
  std::sort(buffer, buffer + neighbor_count);

  best = buffer[0];
  int32_t count = 1;
  for (size_t i = 1; i < neighbor_count; ++i) {
    if (buffer[i] == buffer[i - 1]) {
      ++count;
    } else {
      if (count > max_count) {
        max_count = count;
        best = buffer[i - 1];
      }
      count = 1;
    }
  }
  if (count > max_count) {
    best = buffer[neighbor_count - 1];
  }
  return best;
}

bool CDLP::run_single_iteration(int64_t* buffer, const size_t* offsets,
                                int iteration, const CsrView& ie_view,
                                const CsrView& oe_view) {
  std::vector<size_t> updateds(concurrency_, 0);
  ParallelUtils::parallel_for(
      vertices_.data(), vertices_.size(),
      [&](vid_t idx, int tid) {
        int64_t best = get_majority_community(ie_view, oe_view, idx,
                                              buffer + offsets[idx]);
        next_community_[idx] = best;
        if (best != community_[idx]) {
          ++updateds[tid];
        }
      },
      concurrency_);

  size_t updated = 0;
  for (int i = 0; i < concurrency_; ++i) {
    updated += updateds[i];
  }

  return updated > 0;
}

void CDLP::compute() {
  init_communities();

  const auto& ie_view = graph_.GetGenericIncomingGraphView(
      edge_triplet_.dst_label, edge_triplet_.src_label,
      edge_triplet_.edge_label);
  const auto& oe_view = graph_.GetGenericOutgoingGraphView(
      edge_triplet_.src_label, edge_triplet_.dst_label,
      edge_triplet_.edge_label);
  std::unique_ptr<size_t[]> offsets(
      new size_t[graph_.GetVertexSet(vertex_label_).size()]);
  ParallelUtils::parallel_for(
      vertices_.data(), vertices_.size(),
      [&](vid_t vid, int tid) {
        size_t deg = 0;

        auto ie_edges = ie_view.get_edges(vid);
        for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
          deg++;
        }

        auto oe_edges = oe_view.get_edges(vid);
        for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
          deg++;
        }
        offsets[vid] = deg;
      },
      concurrency_);

  size_t off = 0;
  for (vid_t i : vertices_) {
    size_t deg = offsets[i];
    offsets[i] = off;
    off += deg;
  }
  std::unique_ptr<int64_t[]> buffer(new int64_t[off]);

  for (int iteration = 0; iteration < max_iterations_; ++iteration) {
    bool updated = run_single_iteration(buffer.get(), offsets.get(), iteration,
                                        ie_view, oe_view);
    if (!updated) {
      break;
    }
    community_.swap(next_community_);
  }
}

void CDLP::sink(execution::Context& ctx, int32_t node_alias,
                int32_t label_alias) {
  execution::MSVertexColumnBuilder node_builder(vertex_label_);
  execution::ValueColumnBuilder<int64_t> label_builder;
  label_builder.reserve(vertices_.size());

  for (vid_t v : vertices_) {
    label_builder.push_back_opt(community_[v]);
  }
  node_builder.append(vertex_label_, std::move(vertices_));

  execution::ContextChunk chunk;
  chunk.set(node_alias, node_builder.finish());
  chunk.set(label_alias, label_builder.finish());
  ctx.append_chunk(std::move(chunk));
}

}  // namespace gds
}  // namespace neug
