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

#include <algorithm>
#include <chrono>
#include <limits>
#include <thread>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "utils/parallel_utils.h"
#include "utils/path_utils.h"
#include "utils/subgraph_utils.h"

namespace neug {
namespace gds {

BFS::BFS(const StorageReadInterface& graph, label_t vertex_label,
         label_t edge_label, vid_t source, bool directed, int concurrency,
         bool return_path)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_label_(edge_label),
      source_(source),
      directed_(directed),
      concurrency_(concurrency),
      return_path_(return_path) {
  const auto& vertex_set = graph_.GetVertexSet(vertex_label_);
  size_t vertex_count = vertex_set.size();
  distances_.reset(new uint32_t[vertex_count]);
  ParallelUtils::parallel_for(
      vertex_set,
      [&](vid_t v, int tid) {
        distances_[v] = std::numeric_limits<uint32_t>::max();
      },
      concurrency_);
  distances_[source_] = 0;

  vertices_.reserve(vertex_count);
  for (vid_t v : vertex_set) {
    vertices_.push_back(v);
  }
}

void BFS::compute() {
  auto oe_view = graph_.GetGenericOutgoingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);
  auto ie_view = graph_.GetGenericIncomingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);

  std::vector<vid_t> frontier;
  frontier.reserve(1024);
  frontier.push_back(source_);

  uint32_t level = 1;
  bool use_dense = false;
  while ((!frontier.empty()) || use_dense) {
    std::vector<std::vector<vid_t>> local_next(concurrency_);

    if (!use_dense) {
      ParallelUtils::parallel_for(
          frontier.data(), frontier.size(),
          [&](vid_t src, int tid) {
            auto relax = [&](vid_t dst) {
              uint32_t expected = std::numeric_limits<uint32_t>::max();
              if (__atomic_compare_exchange_n(&distances_[dst], &expected,
                                              level, false, __ATOMIC_RELAXED,
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
    } else {
      use_dense = false;
      ParallelUtils::parallel_for(
          vertices_.data(), vertices_.size(),
          [&](vid_t dst, int tid) {
            if (distances_[dst] != std::numeric_limits<uint32_t>::max()) {
              return;
            }

            bool reachable = false;
            auto ie_edges = ie_view.get_edges(dst);
            for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
              if (distances_[*it] == level - 1) {
                reachable = true;
                break;
              }
            }

            if (!reachable && !directed_) {
              auto oe_edges = oe_view.get_edges(dst);
              for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
                if (distances_[*it] == level - 1) {
                  reachable = true;
                  break;
                }
              }
            }

            if (!reachable) {
              return;
            }
            distances_[dst] = level;
            local_next[tid].push_back(dst);
          },
          concurrency_);
    }

    std::vector<vid_t> next_frontier;
    size_t total = 0;
    for (const auto& bucket : local_next) {
      total += bucket.size();
    }
    if (total * 20 > vertices_.size()) {
      use_dense = true;
    }
    if (!use_dense) {
      next_frontier.reserve(total);
      for (auto& bucket : local_next) {
        next_frontier.insert(next_frontier.end(), bucket.begin(), bucket.end());
      }
    }

    frontier.swap(next_frontier);
    ++level;
  }
}

void BFS::sink(execution::Context& ctx, int node_alias, int distance_alias,
               int path_alias) {
  execution::MSVertexColumnBuilder node_builder(vertex_label_);
  execution::ValueColumnBuilder<int64_t> distance_builder;

  distance_builder.reserve(vertices_.size());

  std::shared_ptr<execution::IContextColumn> path_column;
  if (return_path_) {
    auto oe_view = graph_.GetGenericOutgoingGraphView(
        vertex_label_, vertex_label_, edge_label_);
    auto ie_view = graph_.GetGenericIncomingGraphView(
        vertex_label_, vertex_label_, edge_label_);

    auto find_pred = [&](vid_t v) -> vid_t {
      auto ie_edges = ie_view.get_edges(v);
      for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
        if (distances_[*it] == distances_[v] - 1) {
          return *it;
        }
      }
      if (!directed_) {
        auto oe_edges = oe_view.get_edges(v);
        for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
          if (distances_[*it] == distances_[v] - 1) {
            return *it;
          }
        }
      }
      return source_;
    };

    execution::PathColumnBuilder path_builder;
    for (vid_t v : vertices_) {
      if (distances_[v] == std::numeric_limits<uint32_t>::max()) {
        path_builder.push_back_null();
      } else {
        auto path = reconstruct_path(v, source_, find_pred, vertex_label_,
                                     edge_label_, directed_, graph_);
        path_builder.push_back_opt(std::move(path));
      }
    }
    path_column = path_builder.finish();
  }

  for (vid_t v : vertices_) {
    distance_builder.push_back_opt(distances_[v] ==
                                           std::numeric_limits<uint32_t>::max()
                                       ? -1
                                       : static_cast<int64_t>(distances_[v]));
  }
  node_builder.append(vertex_label_, std::move(vertices_));

  execution::DataChunk chunk;
  chunk.set(node_alias, node_builder.finish());
  chunk.set(distance_alias, distance_builder.finish());

  if (path_column) {
    chunk.set(path_alias, path_column);
  }

  ctx.append_chunk(std::move(chunk));
}

}  // namespace gds
}  // namespace neug
