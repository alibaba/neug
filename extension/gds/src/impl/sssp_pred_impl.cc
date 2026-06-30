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

#include "impl/sssp_pred_impl.h"

#include <algorithm>
#include <cmath>
#include <memory>
#include <queue>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/expression/predicates.h"
#include "utils/path_utils.h"

namespace neug {
namespace gds {

SSSPPred::SSSPPred(const StorageReadInterface& graph, label_t vertex_label,
                   label_t edge_label, vid_t source, bool directed,
                   const std::string& edge_weight_prop, int concurrency,
                   execution::ExprBase* vertex_pred,
                   execution::ExprBase* edge_pred, bool return_path)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_label_(edge_label),
      source_(source),
      directed_(directed),
      edge_weight_prop_(edge_weight_prop),
      concurrency_(concurrency),
      return_path_(return_path),
      vertex_pred_(vertex_pred),
      edge_pred_(edge_pred) {}

void SSSPPred::compute() {
  const auto& vertex_set = graph_.GetVertexSet(vertex_label_);
  size_t n = vertex_set.size();
  distances_.reset(new double[n]);
  for (size_t i = 0; i < n; ++i) {
    distances_[i] = -1.0;
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

  bool has_weight = !edge_weight_prop_.empty();
  std::unique_ptr<EdgeDataAccessor> weight_accessor;
  if (has_weight) {
    weight_accessor =
        std::make_unique<EdgeDataAccessor>(graph_.GetEdgeDataAccessor(
            vertex_label_, vertex_label_, edge_label_, edge_weight_prop_));
  }

  execution::LabelTriplet triplet{vertex_label_, vertex_label_, edge_label_};
  auto oe_view = graph_.GetGenericOutgoingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);
  auto ie_view = graph_.GetGenericIncomingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);

  if (!in_subgraph[source_]) {
    return;
  }

  using QueueEntry = std::pair<double, vid_t>;
  std::priority_queue<QueueEntry, std::vector<QueueEntry>,
                      std::greater<QueueEntry>>
      pq;
  distances_[source_] = 0.0;
  pq.push({0.0, source_});

  while (!pq.empty()) {
    auto [dist, u] = pq.top();
    pq.pop();
    if (dist > distances_[u]) {
      continue;
    }

    auto relax = [&, dist = dist](auto& it, vid_t w, vid_t edge_src,
                                  vid_t edge_dst) {
      if (!in_subgraph[w]) {
        return;
      }
      if (epred && !(*epred)(triplet, edge_src, edge_dst, it.get_data_ptr())) {
        return;
      }
      double weight =
          has_weight ? weight_accessor->get_typed_data<double>(it) : 1.0;
      double cand = dist + weight;
      if (distances_[w] < 0 || cand < distances_[w]) {
        distances_[w] = cand;
        pq.push({cand, w});
      }
    };

    auto oe_edges = oe_view.get_edges(u);
    for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
      relax(it, *it, u, *it);
    }
    if (!directed_) {
      auto ie_edges = ie_view.get_edges(u);
      for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
        relax(it, *it, *it, u);
      }
    }
  }
}

void SSSPPred::sink(execution::Context& ctx, int node_alias, int distance_alias,
                    int path_alias) {
  execution::MSVertexColumnBuilder node_builder(vertex_label_);
  execution::ValueColumnBuilder<double> distance_builder;
  distance_builder.reserve(vertices_.size());

  std::shared_ptr<execution::IContextColumn> path_column;
  if (return_path_) {
    auto oe_view = graph_.GetGenericOutgoingGraphView(
        vertex_label_, vertex_label_, edge_label_);
    auto ie_view = graph_.GetGenericIncomingGraphView(
        vertex_label_, vertex_label_, edge_label_);

    bool has_weight = !edge_weight_prop_.empty();
    std::unique_ptr<EdgeDataAccessor> weight_accessor;
    if (has_weight) {
      weight_accessor =
          std::make_unique<EdgeDataAccessor>(graph_.GetEdgeDataAccessor(
              vertex_label_, vertex_label_, edge_label_, edge_weight_prop_));
    }

    std::unique_ptr<execution::GeneralPred> epred;
    if (edge_pred_ != nullptr) {
      epred = std::make_unique<execution::GeneralPred>(
          edge_pred_->bind(&graph_, {}));
    }
    execution::LabelTriplet triplet{vertex_label_, vertex_label_, edge_label_};

    auto find_pred = [&](vid_t v) -> vid_t {
      double dv = distances_[v];
      auto ie_edges = ie_view.get_edges(v);
      for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
        vid_t u = *it;
        double du = distances_[u];
        if (du < 0)
          continue;
        if (epred && !(*epred)(triplet, u, v, it.get_data_ptr()))
          continue;
        double weight =
            has_weight ? weight_accessor->get_typed_data<double>(it) : 1.0;
        if (std::abs(du + weight - dv) < 1e-9) {
          return u;
        }
      }
      if (!directed_) {
        auto oe_edges = oe_view.get_edges(v);
        for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
          vid_t u = *it;
          double du = distances_[u];
          if (du < 0)
            continue;
          if (epred && !(*epred)(triplet, v, u, it.get_data_ptr()))
            continue;
          double weight =
              has_weight ? weight_accessor->get_typed_data<double>(it) : 1.0;
          if (std::abs(du + weight - dv) < 1e-9) {
            return u;
          }
        }
      }
      return source_;
    };

    execution::PathColumnBuilder path_builder;
    for (vid_t v : vertices_) {
      if (distances_[v] < 0) {
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
    distance_builder.push_back_opt(distances_[v]);
  }
  node_builder.append(vertex_label_, std::move(vertices_));

  execution::ContextChunk chunk;
  chunk.set(node_alias, node_builder.finish());
  chunk.set(distance_alias, distance_builder.finish());

  if (path_column) {
    chunk.set(path_alias, path_column);
  }

  ctx.append_chunk(std::move(chunk));
}

}  // namespace gds
}  // namespace neug
