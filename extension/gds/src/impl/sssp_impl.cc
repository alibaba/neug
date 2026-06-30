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

constexpr double kInf = std::numeric_limits<double>::infinity();

inline bool relax_distance(std::atomic<double>* ptr, double candidate) {
  double old = ptr->load(std::memory_order_relaxed);
  while (candidate < old) {
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
    distances_[i] = kInf;
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

  const size_t vertex_count = vertices_.size();
  std::vector<vid_t> frontier;
  frontier.reserve(1024);
  frontier.push_back(source_);

  while (!frontier.empty()) {
    std::vector<std::vector<vid_t>> local_next(concurrency_);
    const bool use_dense = frontier.size() * 20 > vertex_count;

    if (!use_dense) {
      ParallelUtils::parallel_for(
          frontier.data(), frontier.size(),
          [&](vid_t src, int tid) {
            const double src_dist =
                distances_[src].load(std::memory_order_relaxed);
            if (src_dist >= kInf) {
              return;
            }

            auto relax = [&](auto& it, vid_t dst) {
              double weight = 1.0;
              if (has_edge_weight_) {
                weight = edge_weight_accessor_->get_typed_data<double>(it);
              }
              const double cand = src_dist + weight;
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
    } else {
      ParallelUtils::parallel_for(
          vertices_.data(), vertices_.size(),
          [&](vid_t dst, int tid) {
            double best = distances_[dst].load(std::memory_order_relaxed);

            auto relax_in = [&](auto& it, vid_t src) {
              const double src_dist =
                  distances_[src].load(std::memory_order_relaxed);
              if (src_dist >= kInf) {
                return;
              }
              double weight = 1.0;
              if (has_edge_weight_) {
                weight = edge_weight_accessor_->get_typed_data<double>(it);
              }
              best = std::min(best, src_dist + weight);
            };

            auto ie = ie_view.get_edges(dst);
            for (auto it = ie.begin(); it != ie.end(); ++it) {
              relax_in(it, *it);
            }
            if (!directed_) {
              auto oe = oe_view.get_edges(dst);
              for (auto it = oe.begin(); it != oe.end(); ++it) {
                relax_in(it, *it);
              }
            }

            if (best < distances_[dst].load(std::memory_order_relaxed) &&
                relax_distance(&distances_[dst], best)) {
              local_next[tid].push_back(dst);
            }
          },
          concurrency_);
    }

    size_t total = 0;
    for (const auto& bucket : local_next) {
      total += bucket.size();
    }

    std::vector<vid_t> next_frontier;
    next_frontier.reserve(total);
    for (const auto& bucket : local_next) {
      next_frontier.insert(next_frontier.end(), bucket.begin(), bucket.end());
    }
    frontier.swap(next_frontier);
  }
}

void SSSP::sink(execution::Context& ctx, int node_alias, int distance_alias) {
  execution::MSVertexColumnBuilder node_builder(vertex_label_);
  execution::ValueColumnBuilder<double> distance_builder;

  distance_builder.reserve(vertices_.size());

  for (vid_t v : vertices_) {
    const double dist = distances_[v].load(std::memory_order_relaxed);
    distance_builder.push_back_opt(std::isinf(dist) ? -1.0 : dist);
  }
  node_builder.append(vertex_label_, std::move(vertices_));

  execution::ContextChunk chunk;
  chunk.set(node_alias, node_builder.finish());
  chunk.set(distance_alias, distance_builder.finish());

  ctx.append_chunk(std::move(chunk));
}

}  // namespace gds
}  // namespace neug
