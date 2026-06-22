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

#include "impl/kcore_impl.h"

#include <chrono>
#include <limits>
#include <thread>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "utils/parallel_utils.h"

namespace neug {
namespace gds {

KCore::KCore(const StorageReadInterface& graph, label_t vertex_label,
             label_t edge_label, int32_t k, int32_t concurrency)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_label_(edge_label),
      k_(k) {
  concurrency_ =
      (concurrency <= 0) ? std::thread::hardware_concurrency() : concurrency;
  const auto& vertex_set = graph_.GetVertexSet(vertex_label_);
  vertices_.reserve(vertex_set.size());
  for (vid_t v : vertex_set) {
    vertices_.push_back(v);
  }

  degree_ = std::vector<std::atomic<int32_t>>(vertex_set.size());
  removed_ = std::vector<std::atomic<uint8_t>>(vertex_set.size());
  for (auto& d : degree_) {
    d.store(0, std::memory_order_relaxed);
  }
  for (auto& r : removed_) {
    r.store(0, std::memory_order_relaxed);
  }
}

void KCore::compute() {
  auto oe_view = graph_.GetGenericOutgoingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);
  auto ie_view = graph_.GetGenericIncomingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);

  ParallelUtils::parallel_for(
      vertices_.data(), vertices_.size(),
      [&](vid_t v, int tid) {
        int32_t deg = 0;
        auto oe_edges = oe_view.get_edges(v);
        for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
          ++deg;
        }
        auto ie_edges = ie_view.get_edges(v);
        for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
          ++deg;
        }
        degree_[v].store(deg, std::memory_order_relaxed);
      },
      concurrency_);

  std::vector<vid_t> frontier;
  frontier.reserve(vertices_.size());
  for (vid_t v : vertices_) {
    if (degree_[v].load(std::memory_order_relaxed) < k_) {
      removed_[v].store(1, std::memory_order_relaxed);
      frontier.push_back(v);
    }
  }

  int rounds = 0;
  while (!frontier.empty()) {
    std::vector<std::vector<vid_t>> local_next(concurrency_);

    ParallelUtils::parallel_for(
        frontier.data(), frontier.size(),
        [&](vid_t v, int tid) {
          auto oe_edges = oe_view.get_edges(v);
          for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
            vid_t u = *it;
            if (removed_[u].load(std::memory_order_relaxed) != 0) {
              continue;
            }

            int32_t old_degree =
                degree_[u].fetch_sub(1, std::memory_order_relaxed);
            int32_t new_degree = old_degree - 1;
            if (old_degree >= k_ && new_degree < k_) {
              uint8_t expected = 0;
              if (removed_[u].compare_exchange_strong(
                      expected, 1, std::memory_order_relaxed,
                      std::memory_order_relaxed)) {
                local_next[tid].push_back(u);
              }
            }
          }
          auto ie_edges = ie_view.get_edges(v);
          for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
            vid_t u = *it;
            if (removed_[u].load(std::memory_order_relaxed) != 0) {
              continue;
            }

            int32_t old_degree =
                degree_[u].fetch_sub(1, std::memory_order_relaxed);
            int32_t new_degree = old_degree - 1;
            if (old_degree >= k_ && new_degree < k_) {
              uint8_t expected = 0;
              if (removed_[u].compare_exchange_strong(
                      expected, 1, std::memory_order_relaxed,
                      std::memory_order_relaxed)) {
                local_next[tid].push_back(u);
              }
            }
          }
        },
        concurrency_);

    std::vector<vid_t> next_frontier;
    size_t total = 0;
    for (const auto& bucket : local_next) {
      total += bucket.size();
    }
    next_frontier.reserve(total);
    for (auto& bucket : local_next) {
      next_frontier.insert(next_frontier.end(), bucket.begin(), bucket.end());
    }

    frontier.swap(next_frontier);
    ++rounds;
  }
  (void)rounds;
}

void KCore::sink(execution::Context& ctx, int node_alias, int core_alias) {
  execution::MSVertexColumnBuilder node_builder(vertex_label_);
  execution::ValueColumnBuilder<int64_t> core_builder;

  node_builder.reserve(vertices_.size());
  core_builder.reserve(vertices_.size());

  for (vid_t v : vertices_) {
    node_builder.push_back_opt(v);
    if (removed_[v].load(std::memory_order_relaxed) != 0) {
      core_builder.push_back_opt(-1);
    } else {
      core_builder.push_back_opt(
          static_cast<int64_t>(degree_[v].load(std::memory_order_relaxed)));
    }
  }

  execution::DataChunk chunk;
  chunk.set(node_alias, node_builder.finish());
  chunk.set(core_alias, core_builder.finish());
  ctx.append_chunk(std::move(chunk));
}

}  // namespace gds
}  // namespace neug
