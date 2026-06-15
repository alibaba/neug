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
#include "utils/parallel_utils.h"

namespace neug {
namespace gds {
WCC::WCC(const StorageReadInterface& graph, label_t vertex_label,
         label_t edge_label, int concurrency)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_label_(edge_label),
      concurrency_(concurrency) {
  concurrency = std::max(concurrency, 1);
  parents_.reset(
      new std::atomic<vid_t>[graph.GetVertexSet(vertex_label).size()]);
  comps_.reset(new int64_t[graph.GetVertexSet(vertex_label).size()]);
  auto id_column_holder = graph.GetVertexPropColumn(vertex_label, "id");
  auto id_column =
      dynamic_cast<const TypedRefColumn<int64_t>*>(id_column_holder.get());
  vertices_.reserve(graph.GetVertexSet(vertex_label).size());
  const auto& vertex_set = graph.GetVertexSet(vertex_label);

  if (id_column) {
    ParallelUtils::parallel_for(
        vertex_set,
        [&](vid_t v, int tid) {
          parents_[v] = v;
          comps_[v] = id_column->get_view(v);
        },
        concurrency_);
  } else {
    const auto& vertex_set = graph.GetVertexSet(vertex_label);
    ParallelUtils::parallel_for(
        vertex_set,
        [&](vid_t v, int tid) {
          parents_[v] = v;
          comps_[v] = v;
        },
        concurrency_);
  }

  for (vid_t v : vertex_set) {
    vertices_.push_back(v);
  }
}

vid_t find(vid_t v, std::atomic<vid_t>* parents) {
  while (true) {
    vid_t p = parents[v].load(std::memory_order_acquire);
    if (p == v) {
      return v;
    }
    vid_t gp = parents[p].load(std::memory_order_acquire);

    parents[v].compare_exchange_weak(p, gp, std::memory_order_release,
                                     std::memory_order_relaxed);
    v = p;
  }
  return v;
}

void union_comp(vid_t a, vid_t b, std::atomic<vid_t>* buffer) {
  do {
    vid_t root_a = find(a, buffer);
    vid_t root_b = find(b, buffer);
    if (root_a == root_b) {
      return;
    }
    if (root_a < root_b) {
      if (buffer[root_b].compare_exchange_weak(root_b, root_a,
                                               std::memory_order_release,
                                               std::memory_order_relaxed)) {
        return;
      }
    } else {
      if (buffer[root_a].compare_exchange_weak(root_a, root_b,
                                               std::memory_order_release,
                                               std::memory_order_relaxed)) {
        return;
      }
    }
  } while (true);
}

inline void atomic_min_i64(int64_t* addr, int64_t value) {
  int64_t old = __atomic_load_n(addr, __ATOMIC_ACQUIRE);
  while (value < old) {
    if (__atomic_compare_exchange_n(addr, &old, value, true, __ATOMIC_RELEASE,
                                    __ATOMIC_ACQUIRE)) {
      return;
    }
  }
}

void WCC::compute() {
  auto oe_view = graph_.GetGenericOutgoingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);

  ParallelUtils::parallel_for(
      vertices_.data(), vertices_.size(),
      [&](vid_t v, int tid) {
        const auto& oe_edges = oe_view.get_edges(v);
        for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
          union_comp(v, *it, parents_.get());
        }
      },
      concurrency_);
  ParallelUtils::parallel_for(
      vertices_.data(), vertices_.size(),
      [&](vid_t v, int tid) {
        parents_[v] = find(v, parents_.get());
        vid_t root = parents_[v];
        atomic_min_i64(&comps_[root], comps_[v]);
      },
      concurrency_);

  ParallelUtils::parallel_for(
      vertices_.data(), vertices_.size(),
      [&](vid_t v, int tid) { comps_[v] = comps_[parents_[v]]; }, concurrency_);
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

  execution::DataChunk chunk;
  chunk.set(node_alias, node_builder.finish());
  chunk.set(component_alias, component_builder.finish());
  ctx.append_chunk(std::move(chunk));
}
}  // namespace gds
}  // namespace neug