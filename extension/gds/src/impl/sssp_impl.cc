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

#include "impl/sssp_impl.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <limits>
#include <thread>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "utils/parallel_utils.h"
#include "utils/subgraph_utils.h"

namespace neug {
namespace gds {
namespace {

inline bool relax_distance(std::atomic<double>* ptr, double candidate) {
  double old = ptr->load(std::memory_order_relaxed);
  while (candidate < old || old < 0) {
    if (ptr->compare_exchange_weak(old, candidate, std::memory_order_relaxed,
                                   std::memory_order_relaxed)) {
      return true;
    }
  }
  return false;
}

}  // namespace

SSSP::SSSP(const StorageReadInterface& graph, label_t vertex_label,
           label_t edge_label, vid_t source, bool directed,
           const std::string& edge_weight_prop, int concurrency)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_label_(edge_label),
      source_(source),
      directed_(directed),
      has_edge_weight_(!edge_weight_prop.empty()),
      concurrency_(concurrency) {
  concurrency_ = std::max(concurrency_, 1);

  size_t vertex_count = graph_.GetVertexSet(vertex_label_).size();
  distances_.reset(new std::atomic<double>[vertex_count]);
  for (size_t i = 0; i < vertex_count; ++i) {
    distances_[i] = -1.0;
  }
  distances_[source_] = 0.0;

  if (has_edge_weight_) {
    edge_weight_accessor_ =
        std::make_unique<EdgeDataAccessor>(graph_.GetEdgeDataAccessor(
            vertex_label_, vertex_label_, edge_label_, edge_weight_prop));
  }

  vertices_.reserve(vertex_count);
  for (vid_t v : graph_.GetVertexSet(vertex_label_)) {
    vertices_.push_back(v);
  }
}

void SSSP::compute() {
  auto oe_view = graph_.GetGenericOutgoingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);
  auto ie_view = graph_.GetGenericIncomingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);

  std::vector<vid_t> frontier;
  frontier.reserve(1024);
  frontier.push_back(source_);
  std::vector<uint8_t> dedup_buffer(graph_.GetVertexSet(vertex_label_).size(),
                                    0);

  size_t rounds = 0;
  bool need_continue = false;
  size_t vertex_count = graph_.GetVertexSet(vertex_label_).size();
  while (!frontier.empty() || need_continue) {
    std::vector<std::vector<vid_t>> local_next(concurrency_);
    if (need_continue) {
      need_continue = false;
      ParallelUtils::parallel_for(
          vertices_.data(), vertices_.size(),
          [&](vid_t v, int tid) {
            if (dedup_buffer[v] == 1) {
              dedup_buffer[v] = 0;

              double dist = distances_[v].load(std::memory_order_relaxed);
              if (dist < 0) {
                return;
              }
              auto relax = [&](auto& it, vid_t dst) {
                double weight = 1.0;
                if (has_edge_weight_) {
                  weight = edge_weight_accessor_->get_typed_data<double>(it);
                }
                double cand = dist + weight;
                if (relax_distance(&distances_[dst], cand)) {
                  local_next[tid].push_back(dst);
                }
              };
              auto oe = oe_view.get_edges(v);
              for (auto it = oe.begin(); it != oe.end(); ++it) {
                relax(it, *it);
              }
              if (!directed_) {
                auto ie = ie_view.get_edges(v);
                for (auto it = ie.begin(); it != ie.end(); ++it) {
                  relax(it, *it);
                }
              }
            }
          },
          concurrency_);
    } else {
      ParallelUtils::parallel_for(
          frontier.data(), frontier.size(),
          [&](vid_t src, int tid) {
            double src_dist = distances_[src].load(std::memory_order_relaxed);
            if (src_dist < 0) {
              return;
            }

            auto relax = [&](auto& it, vid_t dst) {
              double weight = 1.0;
              if (has_edge_weight_) {
                weight = edge_weight_accessor_->get_typed_data<double>(it);
              }
              double cand = src_dist + weight;
              if (relax_distance(&distances_[dst], cand)) {
                local_next[tid].push_back(dst);
              }
            };

            auto oe = oe_view.get_edges(src);
            for (auto it = oe.begin(); it != oe.end(); ++it) {
              relax(it, *it);
            }

            if (!directed_) {
              auto ie = ie_view.get_edges(src);
              for (auto it = ie.begin(); it != ie.end(); ++it) {
                relax(it, *it);
              }
            }
          },
          concurrency_);
    }
    size_t total = 0;
    for (const auto& bucket : local_next) {
      total += bucket.size();
    }
    for (vid_t v : frontier) {
      dedup_buffer[v] = 0;
    }

    frontier.clear();

    if (total >= vertex_count * 0.05) {
      need_continue = true;
      for (vid_t v = 0; v < vertex_count; ++v) {
        if (dedup_buffer[v] == 0) {
          dedup_buffer[v] = 1;
        }
      }
    } else {
      for (const auto& bucket : local_next) {
        for (vid_t v : bucket) {
          if (dedup_buffer[v] == 0) {
            dedup_buffer[v] = 1;
            frontier.push_back(v);
          }
        }
      }
    }

    ++rounds;
  }
  (void)rounds;
}

void SSSP::sink(execution::Context& ctx, int node_alias, int distance_alias) {
  execution::MSVertexColumnBuilder node_builder(vertex_label_);
  execution::ValueColumnBuilder<double> distance_builder;

  distance_builder.reserve(vertices_.size());

  for (vid_t v : vertices_) {
    distance_builder.push_back_opt(distances_[v]);
  }
  node_builder.append(vertex_label_, std::move(vertices_));

  execution::DataChunk chunk;
  chunk.set(node_alias, node_builder.finish());
  chunk.set(distance_alias, distance_builder.finish());
  ctx.append_chunk(std::move(chunk));
}

}  // namespace gds
}  // namespace neug
