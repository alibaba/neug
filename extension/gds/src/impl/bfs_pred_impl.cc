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

#include "impl/bfs_pred_impl.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/expression/predicates.h"
#include "utils/path_utils.h"

namespace neug {
namespace gds {

BFSPred::BFSPred(const StorageReadInterface& graph, label_t vertex_label,
                 label_t edge_label, vid_t source, bool directed,
                 int concurrency, execution::ExprBase* vertex_pred,
                 execution::ExprBase* edge_pred, bool return_path)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_label_(edge_label),
      source_(source),
      directed_(directed),
      concurrency_(concurrency),
      return_path_(return_path),
      vertex_pred_(vertex_pred),
      edge_pred_(edge_pred) {}

void BFSPred::compute() {
  const auto& vertex_set = graph_.GetVertexSet(vertex_label_);
  size_t n = vertex_set.size();
  distances_.reset(new uint32_t[n]);
  for (size_t i = 0; i < n; ++i) {
    distances_[i] = std::numeric_limits<uint32_t>::max();
  }

  std::unique_ptr<execution::GeneralPred> vpred;
  if (vertex_pred_ != nullptr) {
    vpred = std::make_unique<execution::GeneralPred>(
        vertex_pred_->bind(&graph_, {}));
  }
  std::unique_ptr<execution::GeneralPred> epred;
  if (edge_pred_ != nullptr) {
    epred =
        std::make_unique<execution::GeneralPred>(edge_pred_->bind(&graph_, {}));
  }

  std::vector<char> in_subgraph(n, 0);
  for (vid_t v : vertex_set) {
    if (!vpred || (*vpred)(vertex_label_, v)) {
      in_subgraph[v] = 1;
      vertices_.push_back(v);
    }
  }

  execution::LabelTriplet triplet{vertex_label_, vertex_label_, edge_label_};
  auto oe_view = graph_.GetGenericOutgoingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);
  auto ie_view = graph_.GetGenericIncomingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);

  if (!in_subgraph[source_]) {
    return;
  }

  distances_[source_] = 0;
  std::vector<vid_t> frontier{source_};
  uint32_t level = 1;
  while (!frontier.empty()) {
    std::vector<vid_t> next;
    for (vid_t u : frontier) {
      auto relax = [&](vid_t w) {
        if (!in_subgraph[w] ||
            distances_[w] != std::numeric_limits<uint32_t>::max()) {
          return;
        }
        distances_[w] = level;
        next.push_back(w);
      };

      auto oe_edges = oe_view.get_edges(u);
      for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
        vid_t w = *it;
        if (epred && !(*epred)(triplet, u, w, it.get_data_ptr())) {
          continue;
        }
        relax(w);
      }

      if (!directed_) {
        auto ie_edges = ie_view.get_edges(u);
        for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
          vid_t w = *it;
          if (epred && !(*epred)(triplet, w, u, it.get_data_ptr())) {
            continue;
          }
          relax(w);
        }
      }
    }
    frontier.swap(next);
    ++level;
  }
}

void BFSPred::sink(execution::Context& ctx, int node_alias, int distance_alias,
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

    std::unique_ptr<execution::GeneralPred> epred;
    if (edge_pred_ != nullptr) {
      epred = std::make_unique<execution::GeneralPred>(
          edge_pred_->bind(&graph_, {}));
    }
    execution::LabelTriplet triplet{vertex_label_, vertex_label_, edge_label_};

    auto find_pred = [&](vid_t v) -> vid_t {
      auto ie_edges = ie_view.get_edges(v);
      for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
        vid_t u = *it;
        if (distances_[u] == distances_[v] - 1) {
          if (!epred || (*epred)(triplet, u, v, it.get_data_ptr())) {
            return u;
          }
        }
      }
      if (!directed_) {
        auto oe_edges = oe_view.get_edges(v);
        for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
          vid_t u = *it;
          if (distances_[u] == distances_[v] - 1) {
            if (!epred || (*epred)(triplet, v, u, it.get_data_ptr())) {
              return u;
            }
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
