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

#include "impl/wcc_pred_impl.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/expression/predicates.h"

namespace neug {
namespace gds {

namespace {
constexpr int64_t kExcluded = std::numeric_limits<int64_t>::max();
}  // namespace

WCCPred::WCCPred(const StorageReadInterface& graph, label_t vertex_label,
                 label_t edge_label, int concurrency,
                 execution::ExprBase* vertex_pred,
                 execution::ExprBase* edge_pred)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_label_(edge_label),
      concurrency_(concurrency),
      vertex_pred_(vertex_pred),
      edge_pred_(edge_pred) {}

// Simple sequential flood fill over the undirected (in + out) edges of the
// subgraph. Each component is labelled with its smallest vertex id, matching
// the plain WCC convention.
void WCCPred::compute() {
  const auto& vertex_set = graph_.GetVertexSet(vertex_label_);
  size_t n = vertex_set.size();
  comps_.reset(new int64_t[n]);

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

  auto id_column_holder = graph_.GetVertexPropColumn(vertex_label_, "id");
  auto id_column =
      dynamic_cast<const TypedRefColumn<int64_t>*>(id_column_holder.get());

  for (vid_t v : vertex_set) {
    if (vpred && !(*vpred)(vertex_label_, v)) {
      comps_[v] = kExcluded;
    } else {
      comps_[v] = id_column ? id_column->get_view(v) : static_cast<int64_t>(v);
      vertices_.push_back(v);
    }
  }

  execution::LabelTriplet triplet{vertex_label_, vertex_label_, edge_label_};
  auto oe_view = graph_.GetGenericOutgoingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);
  auto ie_view = graph_.GetGenericIncomingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);

  std::vector<char> visited(n, 0);
  std::vector<vid_t> stack;
  std::vector<vid_t> component;
  for (vid_t s : vertices_) {
    if (visited[s]) {
      continue;
    }
    component.clear();
    stack.clear();
    stack.push_back(s);
    visited[s] = 1;
    int64_t min_label = comps_[s];

    while (!stack.empty()) {
      vid_t u = stack.back();
      stack.pop_back();
      component.push_back(u);
      min_label = std::min(min_label, comps_[u]);

      auto visit = [&](vid_t nbr) {
        if (comps_[nbr] == kExcluded || visited[nbr]) {
          return;
        }
        visited[nbr] = 1;
        stack.push_back(nbr);
      };

      auto oe_edges = oe_view.get_edges(u);
      for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
        vid_t nbr = *it;
        if (epred && !(*epred)(triplet, u, nbr, it.get_data_ptr())) {
          continue;
        }
        visit(nbr);
      }
      auto ie_edges = ie_view.get_edges(u);
      for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
        vid_t nbr = *it;
        if (epred && !(*epred)(triplet, nbr, u, it.get_data_ptr())) {
          continue;
        }
        visit(nbr);
      }
    }

    for (vid_t x : component) {
      comps_[x] = min_label;
    }
  }
}

void WCCPred::sink(execution::Context& ctx, int node_alias,
                   int component_alias) {
  execution::MSVertexColumnBuilder node_builder(vertex_label_);
  execution::ValueColumnBuilder<int64_t> component_builder;
  component_builder.reserve(vertices_.size());

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
