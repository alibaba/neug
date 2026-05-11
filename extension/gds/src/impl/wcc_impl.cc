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

#include <atomic>
#include <thread>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "parallel_utils.h"

namespace neug {
namespace gds {
WCC::WCC(const StorageReadInterface& graph, label_t vertex_label,
         label_t edge_label, int concurrency)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_label_(edge_label),
      concurrency_(concurrency) {
  if (concurrency_ <= 0) {
    concurrency_ = static_cast<int>(std::thread::hardware_concurrency());
    if (concurrency_ <= 0) {
      concurrency_ = 1;
    }
  }
  parents_ = std::make_unique<vid_t[]>(graph.GetVertexSet(vertex_label).size());
  comps_ = std::make_unique<int64_t[]>(graph.GetVertexSet(vertex_label).size());
  auto id_column = dynamic_cast<const TypedRefColumn<int64_t>*>(
      graph.GetVertexPropColumn(vertex_label, "id").get());
  vertices_.reserve(graph.GetVertexSet(vertex_label).size());
  if (id_column) {
    for (vid_t v : graph.GetVertexSet(vertex_label)) {
      parents_[v] = v;
      comps_[v] = id_column->get_view(v);
      vertices_.push_back(v);
    }
  } else {
    for (vid_t v : graph.GetVertexSet(vertex_label)) {
      parents_[v] = v;
      comps_[v] = v;
      vertices_.push_back(v);
    }
  }
}

bool atomic_min_vid(vid_t* addr, vid_t val) {
  vid_t cur = __atomic_load_n(addr, __ATOMIC_RELAXED);
  while (val < cur) {
    if (__atomic_compare_exchange_n(addr, &cur, val, true, __ATOMIC_RELAXED,
                                    __ATOMIC_RELAXED)) {
      return true;
    }
  }
  return false;
}

void WCC::compute() {
  auto begin_total = std::chrono::high_resolution_clock::now();
  auto oe_view = graph_.GetGenericOutgoingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);
  auto ie_view = graph_.GetGenericIncomingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);

  std::vector<vid_t> frontier = vertices_;
  std::vector<std::atomic<uint8_t>> marked(
      graph_.GetVertexSet(vertex_label_).size());
  for (auto& m : marked) {
    m.store(0, std::memory_order_relaxed);
  }

  int rounds = 0;
  while (!frontier.empty()) {
    std::vector<std::vector<vid_t>> local_next(concurrency_);
    std::atomic<bool> updated(false);

    ParallelUtils::parallel_for(
        frontier.data(), frontier.size(),
        [&](vid_t src, int tid) {
          const vid_t src_comp =
              __atomic_load_n(&parents_[src], __ATOMIC_RELAXED);

          auto relax = [&](vid_t dst) {
            if (atomic_min_vid(&parents_[dst], src_comp)) {
              updated.store(true, std::memory_order_relaxed);
              uint8_t expected = 0;
              if (marked[dst].compare_exchange_strong(
                      expected, 1, std::memory_order_relaxed,
                      std::memory_order_relaxed)) {
                local_next[tid].push_back(dst);
              }
            }
          };

          auto oe_edges = oe_view.get_edges(src);
          for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
            relax(*it);
          }

          auto ie_edges = ie_view.get_edges(src);
          for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
            relax(*it);
          }
        },
        concurrency_);

    if (!updated.load(std::memory_order_relaxed)) {
      break;
    }

    std::vector<vid_t> next_frontier;
    size_t total = 0;
    for (const auto& bucket : local_next) {
      total += bucket.size();
    }
    next_frontier.reserve(total);
    for (auto& bucket : local_next) {
      next_frontier.insert(next_frontier.end(), bucket.begin(), bucket.end());
    }

    for (vid_t v : next_frontier) {
      marked[v].store(0, std::memory_order_relaxed);
    }

    frontier.swap(next_frontier);
    ++rounds;
  }

  for (vid_t v : vertices_) {
    const vid_t root = parents_[v];
    comps_[root] = std::min(comps_[root], comps_[v]);
  }
  for (vid_t v : vertices_) {
    comps_[v] = comps_[parents_[v]];
  }

  LOG(INFO) << "WCC compute phase took "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - begin_total)
                   .count()
            << " ms, rounds=" << rounds;
}
void WCC::sink(execution::Context& ctx, int node_alias, int component_alias) {
  execution::MSVertexColumnBuilder node_builder(vertex_label_);
  execution::ValueColumnBuilder<int64_t> component_builder;
  node_builder.reserve(vertices_.size());
  component_builder.reserve(vertices_.size());
  for (vid_t v : vertices_) {
    node_builder.push_back_opt(v);
    component_builder.push_back_opt(comps_[v]);
  }
  ctx.set(node_alias, node_builder.finish());
  ctx.set(component_alias, component_builder.finish());
}
}  // namespace gds
}  // namespace neug