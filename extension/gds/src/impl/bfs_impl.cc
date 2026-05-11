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

#include "impl/bfs_impl.h"

#include <chrono>
#include <limits>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "parallel_utils.h"

namespace neug {
namespace gds {

BFS::BFS(const StorageReadInterface& graph, label_t vertex_label,
         label_t edge_label, vid_t source, bool directed, int concurrency)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_label_(edge_label),
      source_(source),
      directed_(directed),
      concurrency_(concurrency) {
  size_t vertex_count = graph_.GetVertexSet(vertex_label_).size();
  auto start = std::chrono::high_resolution_clock::now();
  distances_ = std::make_unique<uint32_t[]>(vertex_count);
  for (size_t i = 0; i < vertex_count; ++i) {
    distances_[i] = std::numeric_limits<uint32_t>::max();
  }
  distances_[source_] = 0;
  auto end = std::chrono::high_resolution_clock::now();
  LOG(INFO) << "Initialized BFS with vertex count: " << vertex_count
            << ", initialization time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                     start)
                   .count()
            << " ms";
}

void BFS::compute() {
  auto oe_view = graph_.GetGenericOutgoingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);
  auto ie_view = graph_.GetGenericIncomingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);
  std::vector<vid_t> frontier;
  frontier.reserve(1024);
  frontier.push_back(source_);

  auto start = std::chrono::high_resolution_clock::now();
  uint32_t level = 1;
  while (!frontier.empty()) {
    std::vector<std::vector<vid_t>> local_next(concurrency_ > 0 ? concurrency_
                                                                : 1);

    ParallelUtils::parallel_for(
        frontier.data(), frontier.size(),
        [&](vid_t src, int tid) {
          auto relax = [&](vid_t dst) {
            uint32_t expected = std::numeric_limits<uint32_t>::max();
            if (__atomic_compare_exchange_n(&distances_[dst], &expected, level,
                                            false, __ATOMIC_RELAXED,
                                            __ATOMIC_RELAXED)) {
              local_next[tid].push_back(dst);
            }
          };

          auto oe_edges = oe_view.get_edges(src);
          for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
            relax(*it);
          }

          if (!directed_) {
            auto ie_edges = ie_view.get_edges(src);
            for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
              relax(*it);
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
    ++level;
  }
  LOG(INFO) << "BFS computation completed, total levels: " << (level - 1)
            << ", time taken: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - start)
                   .count()
            << " ms";
}

void BFS::sink(execution::Context& ctx, int node_alias, int distance_alias) {
  execution::MSVertexColumnBuilder node_builder(vertex_label_);
  execution::ValueColumnBuilder<int64_t> distance_builder;

  const auto& vertex_set = graph_.GetVertexSet(vertex_label_);
  node_builder.reserve(vertex_set.size());
  distance_builder.reserve(vertex_set.size());

  for (vid_t v : vertex_set) {
    node_builder.push_back_opt(v);
    distance_builder.push_back_opt(distances_[v] ==
                                           std::numeric_limits<uint32_t>::max()
                                       ? -1
                                       : static_cast<int64_t>(distances_[v]));
  }

  ctx.set(node_alias, node_builder.finish());
  ctx.set(distance_alias, distance_builder.finish());
}

}  // namespace gds
}  // namespace neug
