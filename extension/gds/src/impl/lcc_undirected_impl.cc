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

#include "impl/lcc_undirected_impl.h"

#include <algorithm>
#include <atomic>
#include <cstdint>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "utils/parallel_utils.h"

namespace neug {
namespace gds {

namespace {

constexpr size_t kGallopingRatio = 16;

template <typename OnMatch>
inline void intersect_sorted_adaptive(const vid_t* a, size_t na, const vid_t* b,
                                      size_t nb, OnMatch&& on_match) {
  if (na == 0 || nb == 0) {
    return;
  }

  if (na > nb) {
    intersect_sorted_adaptive(b, nb, a, na, std::forward<OnMatch>(on_match));
    return;
  }

  if (nb >= na * kGallopingRatio) {
    size_t low = 0;
    for (size_t i = 0; i < na; ++i) {
      vid_t x = a[i];
      if (low >= nb) {
        break;
      }

      if (b[low] < x) {
        size_t step = 1;
        size_t hi = low + 1;
        while (hi < nb && b[hi] < x) {
          low = hi;
          step <<= 1;
          hi = low + step;
        }
        if (hi > nb) {
          hi = nb;
        }
        auto it = std::lower_bound(b + low + 1, b + hi, x);
        low = static_cast<size_t>(it - b);
      }

      if (low < nb && b[low] == x) {
        on_match(x, i, low);
      }
    }
  } else {
    size_t i = 0, j = 0;
    while (i < na && j < nb) {
      if (a[i] == b[j]) {
        on_match(a[i], i, j);
        ++i;
        ++j;
      } else if (a[i] < b[j]) {
        ++i;
      } else {
        ++j;
      }
    }
  }
}

}  // namespace

LCCUndirected::LCCUndirected(const StorageReadInterface& graph,
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

  degrees_.reset(new int32_t[vertex_set.size()]);
  triangles_.reset(new int64_t[vertex_set.size()]);
  lcc_.reset(new double[vertex_set.size()]);
}

void LCCUndirected::compute() {
  auto oe_view = graph_.GetGenericOutgoingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);
  auto ie_view = graph_.GetGenericIncomingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);
  size_t vertex_count = graph_.GetVertexSet(vertex_label_).size();

  std::vector<size_t> degrees_per_thread(concurrency_, 0);

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
        degrees_per_thread[tid] += degree;
      },
      concurrency_);
  std::unique_ptr<size_t[]> offsets(new size_t[vertex_count]);
  size_t off = 0;
  for (vid_t i : vertices_) {
    offsets[i] = off;
    off += degrees_[i] <= degree_threshold_ ? degrees_[i] : 0;
  }
  std::unique_ptr<int32_t[]> deg(new int32_t[vertex_count]);
  size_t total_oriented_edges = 0;
  for (size_t i = 0; i < concurrency_; ++i) {
    total_oriented_edges += degrees_per_thread[i];
  }
  std::unique_ptr<vid_t[]> oriented(new vid_t[total_oriented_edges]);
  ParallelUtils::parallel_for(
      vertices_.data(), vertices_.size(),
      [&](vid_t v, int tid) {
        if (degrees_[v] > degree_threshold_) {
          return;
        }
        vid_t* out = oriented.get() + offsets[v];
        int32_t deg_v = 0;

        const auto& oe = oe_view.get_edges(v);
        for (auto it = oe.begin(); it != oe.end(); ++it) {
          vid_t u = *it;
          if (degrees_[u] > degree_threshold_) {
            continue;
          }
          if (degrees_[u] < degrees_[v] ||
              (degrees_[u] == degrees_[v] && u < v)) {
            out[deg_v++] = u;
          }
        }
        const auto& ie = ie_view.get_edges(v);
        for (auto it = ie.begin(); it != ie.end(); ++it) {
          vid_t u = *it;
          if (degrees_[u] > degree_threshold_) {
            continue;
          }
          if (degrees_[u] < degrees_[v] ||
              (degrees_[u] == degrees_[v] && u < v)) {
            out[deg_v++] = u;
          }
        }

        std::sort(out, out + deg_v);
        deg[v] = std::unique(out, out + deg_v) - out;
      },
      concurrency_);

  std::unique_ptr<std::atomic<int64_t>[]> atomic_tri(
      new std::atomic<int64_t>[vertex_count]);
  ParallelUtils::parallel_for(
      vertices_.data(), vertices_.size(),
      [&](vid_t v, int tid) {
        atomic_tri[v].store(0, std::memory_order_relaxed);
      },
      concurrency_);

  ParallelUtils::parallel_for(
      vertices_.data(), vertices_.size(),
      [&](vid_t v, int tid) {
        if (degrees_[v] <= 1 || degrees_[v] > degree_threshold_) {
          return;
        }
        const vid_t* v_list = oriented.get() + offsets[v];
        size_t deg_v = deg[v];
        for (uint32_t k = 0; k < deg_v; ++k) {
          vid_t u = v_list[k];
          const vid_t* u_list = oriented.get() + offsets[u];
          size_t deg_u = deg[u];
          int64_t pair_triangles = 0;

          intersect_sorted_adaptive(
              v_list, deg_v, u_list, deg_u, [&](vid_t w, size_t, size_t) {
                ++pair_triangles;
                atomic_tri[w].fetch_add(1, std::memory_order_relaxed);
              });
          if (pair_triangles > 0) {
            atomic_tri[v].fetch_add(pair_triangles, std::memory_order_relaxed);
            atomic_tri[u].fetch_add(pair_triangles, std::memory_order_relaxed);
          }
        }
      },
      concurrency_);
  for (vid_t v : vertices_) {
    triangles_[v] = atomic_tri[v].load(std::memory_order_relaxed);
    if (degrees_[v] <= 1 || degrees_[v] > degree_threshold_) {
      lcc_[v] = 0.0;
    } else {
      lcc_[v] = 2.0 * static_cast<double>(triangles_[v]) /
                (static_cast<double>(degrees_[v]) *
                 static_cast<double>(degrees_[v] - 1));
    }
  }
}

void LCCUndirected::sink(execution::Context& ctx, int node_alias,
                         int lcc_alias) {
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
