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

#include "impl/kcore_pred_impl.h"

#include <memory>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/expression/predicates.h"

namespace neug {
namespace gds {

KCorePred::KCorePred(const StorageReadInterface& graph, label_t vertex_label,
                     label_t edge_label, int32_t k, int32_t concurrency,
                     execution::ExprBase* vertex_pred,
                     execution::ExprBase* edge_pred)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_label_(edge_label),
      k_(k),
      concurrency_(concurrency),
      vertex_pred_(vertex_pred),
      edge_pred_(edge_pred) {}

// Simple sequential k-core peeling: repeatedly remove vertices whose remaining
// degree is below k, decrementing their neighbors' degrees.
void KCorePred::compute() {
  const auto& vertex_set = graph_.GetVertexSet(vertex_label_);
  size_t n = vertex_set.size();
  degree_.assign(n, 0);
  removed_.assign(n, 0);

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
    } else {
      removed_[v] = 1;
    }
  }

  execution::LabelTriplet triplet{vertex_label_, vertex_label_, edge_label_};
  auto oe_view = graph_.GetGenericOutgoingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);
  auto ie_view = graph_.GetGenericIncomingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);

  for (vid_t v : vertices_) {
    int32_t deg = 0;
    auto oe_edges = oe_view.get_edges(v);
    for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
      vid_t w = *it;
      if (!in_subgraph[w]) {
        continue;
      }
      if (epred && !(*epred)(triplet, v, w, it.get_data_ptr())) {
        continue;
      }
      ++deg;
    }
    auto ie_edges = ie_view.get_edges(v);
    for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
      vid_t u = *it;
      if (!in_subgraph[u]) {
        continue;
      }
      if (epred && !(*epred)(triplet, u, v, it.get_data_ptr())) {
        continue;
      }
      ++deg;
    }
    degree_[v] = deg;
  }

  std::vector<vid_t> frontier;
  for (vid_t v : vertices_) {
    if (degree_[v] < k_) {
      removed_[v] = 1;
      frontier.push_back(v);
    }
  }

  while (!frontier.empty()) {
    std::vector<vid_t> next;
    for (vid_t v : frontier) {
      auto decrement = [&](vid_t u) {
        if (!in_subgraph[u] || removed_[u]) {
          return;
        }
        if (--degree_[u] < k_) {
          removed_[u] = 1;
          next.push_back(u);
        }
      };
      auto oe_edges = oe_view.get_edges(v);
      for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
        vid_t w = *it;
        if (epred && !(*epred)(triplet, v, w, it.get_data_ptr())) {
          continue;
        }
        decrement(w);
      }
      auto ie_edges = ie_view.get_edges(v);
      for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
        vid_t u = *it;
        if (epred && !(*epred)(triplet, u, v, it.get_data_ptr())) {
          continue;
        }
        decrement(u);
      }
    }
    frontier.swap(next);
  }
}

void KCorePred::sink(execution::Context& ctx, int node_alias, int core_alias) {
  execution::MSVertexColumnBuilder node_builder(vertex_label_);
  execution::ValueColumnBuilder<int64_t> core_builder;
  core_builder.reserve(vertices_.size());

  for (vid_t v : vertices_) {
    core_builder.push_back_opt(removed_[v] ? -1
                                           : static_cast<int64_t>(degree_[v]));
  }
  node_builder.append(vertex_label_, std::move(vertices_));

  execution::DataChunk chunk;
  chunk.set(node_alias, node_builder.finish());
  chunk.set(core_alias, core_builder.finish());
  ctx.append_chunk(std::move(chunk));
}

}  // namespace gds
}  // namespace neug
