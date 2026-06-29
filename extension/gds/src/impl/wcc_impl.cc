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

#include "impl/wcc_impl.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <unordered_map>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/storages/csr/csr_view.h"
#include "utils/parallel_utils.h"

namespace neug {
namespace gds {

namespace {

constexpr int32_t kNeighborRounds = 4;
constexpr int64_t kSampleCount = 1024;

// Afforest Link (GAPBS cc.cc), adapted for neug atomics.
void Link(vid_t u, vid_t v, std::atomic<vid_t>* parent) {
  vid_t p1 = parent[u].load(std::memory_order_relaxed);
  vid_t p2 = parent[v].load(std::memory_order_relaxed);
  while (p1 != p2) {
    vid_t high = p1 > p2 ? p1 : p2;
    vid_t low = p1 + (p2 - high);
    vid_t p_high = parent[high].load(std::memory_order_relaxed);
    if (p_high == low) {
      break;
    }
    if (p_high == high) {
      vid_t expected = high;
      if (parent[high].compare_exchange_weak(expected, low,
                                             std::memory_order_release,
                                             std::memory_order_relaxed)) {
        break;
      }
    }
    p1 = parent[parent[high].load(std::memory_order_relaxed)].load(
        std::memory_order_relaxed);
    p2 = parent[low].load(std::memory_order_relaxed);
  }
}

void Compress(vid_t* vertices, size_t vertex_count, std::atomic<vid_t>* parent,
              int concurrency) {
  ParallelUtils::parallel_for(
      vertices, vertex_count,
      [&](vid_t v, int tid) {
        while (true) {
          vid_t p = parent[v].load(std::memory_order_acquire);
          vid_t pp = parent[p].load(std::memory_order_acquire);
          if (p == pp) {
            break;
          }
          parent[v].compare_exchange_weak(p, pp, std::memory_order_release,
                                          std::memory_order_relaxed);
        }
      },
      concurrency);
}

// Merged neighbor list: ie[0..] then oe[0..] (ie/oe do not duplicate at u).
void LinkMergedFirstN(const CsrView& ie_view, const CsrView& oe_view, vid_t u,
                      size_t n, std::atomic<vid_t>* parent) {
  auto ie = ie_view.get_edges(u);
  size_t linked = 0;
  for (auto it = ie.begin(); it != ie.end() && linked < n; ++it, ++linked) {
    Link(u, *it, parent);
  }
  if (linked < n) {
    auto oe = oe_view.get_edges(u);
    for (auto it = oe.begin(); it != oe.end() && linked < n; ++it, ++linked) {
      Link(u, *it, parent);
    }
  }
}

void ForMergedNeighborsFrom(const CsrView& ie_view, const CsrView& oe_view,
                            vid_t u, size_t start, std::atomic<vid_t>* parent) {
  auto ie = ie_view.get_edges(u);
  auto it = ie.begin();
  const auto ie_end = ie.end();
  size_t skipped = 0;
  while (skipped < start && it != ie_end) {
    ++it;
    ++skipped;
  }
  for (; it != ie_end; ++it) {
    Link(u, *it, parent);
  }

  size_t oe_skip = start - skipped;
  auto oe = oe_view.get_edges(u);
  auto oit = oe.begin();
  const auto oe_end = oe.end();
  while (oe_skip > 0 && oit != oe_end) {
    ++oit;
    --oe_skip;
  }
  for (; oit != oe_end; ++oit) {
    Link(u, *oit, parent);
  }
}

vid_t SampleFrequentRoot(vid_t* vertices, size_t vertex_count,
                         std::atomic<vid_t>* parent) {
  std::unordered_map<vid_t, int> sample_counts;
  sample_counts.reserve(32);

  if (vertex_count == 0) {
    return 0;
  }

  const size_t samples = static_cast<size_t>(
      std::min<int64_t>(kSampleCount, static_cast<int64_t>(vertex_count)));
  const size_t stride = std::max<size_t>(1, vertex_count / samples);

  for (size_t i = 0; i < samples; ++i) {
    vid_t v = vertices[(i * stride) % vertex_count];
    vid_t root = parent[v].load(std::memory_order_relaxed);
    ++sample_counts[root];
  }

  vid_t best_root = parent[vertices[0]].load(std::memory_order_relaxed);
  int best_count = 0;
  for (const auto& entry : sample_counts) {
    if (entry.second > best_count) {
      best_count = entry.second;
      best_root = entry.first;
    }
  }
  return best_root;
}

}  // namespace

WCC::WCC(const StorageReadInterface& graph, label_t vertex_label,
         label_t edge_label, int concurrency)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_label_(edge_label),
      concurrency_(std::max(concurrency, 1)) {
  const auto& vertex_set = graph.GetVertexSet(vertex_label);
  const size_t vertex_count = vertex_set.size();
  parent_.reset(new std::atomic<vid_t>[vertex_count]);
  ext_id_.reset(new int64_t[vertex_count]);
  comps_.reset(new int64_t[vertex_count]);
  vertices_.reserve(vertex_count);

  auto id_column_holder = graph.GetVertexPropColumn(vertex_label, "id");
  auto id_column =
      dynamic_cast<const TypedRefColumn<int64_t>*>(id_column_holder.get());

  if (id_column) {
    ParallelUtils::parallel_for(
        vertex_set,
        [&](vid_t v, int tid) {
          parent_[v].store(v, std::memory_order_relaxed);
          ext_id_[v] = id_column->get_view(v);
        },
        concurrency_);
  } else {
    ParallelUtils::parallel_for(
        vertex_set,
        [&](vid_t v, int tid) {
          parent_[v].store(v, std::memory_order_relaxed);
          ext_id_[v] = v;
        },
        concurrency_);
  }

  for (vid_t v : vertex_set) {
    vertices_.push_back(v);
  }
}

void WCC::compute() {
  auto oe_view = graph_.GetGenericOutgoingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);
  auto ie_view = graph_.GetGenericIncomingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);

  // Phase 1: sample a sparse subgraph (first kNeighborRounds merged edges).
  ParallelUtils::parallel_for(
      vertices_.data(), vertices_.size(),
      [&](vid_t u, int tid) {
        LinkMergedFirstN(ie_view, oe_view, u,
                         static_cast<size_t>(kNeighborRounds), parent_.get());
      },
      concurrency_);
  Compress(vertices_.data(), vertices_.size(), parent_.get(), concurrency_);

  const vid_t largest_root =
      SampleFrequentRoot(vertices_.data(), vertices_.size(), parent_.get());
  const size_t final_offset = static_cast<size_t>(kNeighborRounds);

  // Phase 2: process remaining edges, skipping the largest component.
  ParallelUtils::parallel_for(
      vertices_.data(), vertices_.size(),
      [&](vid_t u, int tid) {
        if (parent_[u].load(std::memory_order_relaxed) == largest_root) {
          return;
        }
        ForMergedNeighborsFrom(ie_view, oe_view, u, final_offset,
                               parent_.get());
      },
      concurrency_);

  Compress(vertices_.data(), vertices_.size(), parent_.get(), concurrency_);

  // Map each component root to the minimum external vertex id (LDBC semantics).
  const int64_t kMax = std::numeric_limits<int64_t>::max();
  std::unique_ptr<int64_t[]> min_label(new int64_t[vertices_.size()]);
  ParallelUtils::parallel_for(
      vertices_.data(), vertices_.size(),
      [&](vid_t v, int tid) { min_label[v] = kMax; }, concurrency_);

  ParallelUtils::parallel_for(
      vertices_.data(), vertices_.size(),
      [&](vid_t v, int tid) {
        vid_t root = parent_[v].load(std::memory_order_relaxed);
        int64_t id = ext_id_[v];
        int64_t* slot = &min_label[root];
        int64_t old = __atomic_load_n(slot, __ATOMIC_ACQUIRE);
        while (id < old) {
          if (__atomic_compare_exchange_n(slot, &old, id, true,
                                          __ATOMIC_RELEASE, __ATOMIC_ACQUIRE)) {
            return;
          }
        }
      },
      concurrency_);

  ParallelUtils::parallel_for(
      vertices_.data(), vertices_.size(),
      [&](vid_t v, int tid) {
        vid_t root = parent_[v].load(std::memory_order_relaxed);
        comps_[v] = min_label[root];
      },
      concurrency_);
}

void WCC::sink(execution::Context& ctx, int node_alias, int component_alias) {
  execution::MSVertexColumnBuilder node_builder(vertex_label_);
  execution::ValueColumnBuilder<int64_t> component_builder;
  size_t vertex_count = vertices_.size();

  component_builder.reserve(vertex_count);
  for (vid_t v : vertices_) {
    component_builder.push_back_opt(comps_[v]);
  }
  node_builder.append(vertex_label_, std::move(vertices_));

  execution::ContextChunk chunk;
  chunk.set(node_alias, node_builder.finish());
  chunk.set(component_alias, component_builder.finish());
  ctx.append_chunk(std::move(chunk));
}
}  // namespace gds
}  // namespace neug
