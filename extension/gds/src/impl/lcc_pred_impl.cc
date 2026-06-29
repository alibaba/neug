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

#include "impl/lcc_pred_impl.h"

#include <memory>
#include <set>
#include <unordered_set>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/expression/predicates.h"

namespace neug {
namespace gds {

LCCPred::LCCPred(const StorageReadInterface& graph, label_t vertex_label,
                 label_t edge_label, bool directed, int concurrency,
                 execution::ExprBase* vertex_pred,
                 execution::ExprBase* edge_pred)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_label_(edge_label),
      directed_(directed),
      concurrency_(concurrency),
      vertex_pred_(vertex_pred),
      edge_pred_(edge_pred) {}

// Evaluate the LCC definition directly: for each vertex collect its distinct
// neighborhood, then count the (directed or undirected) edges among those
// neighbors.
void LCCPred::compute() {
  const auto& vertex_set = graph_.GetVertexSet(vertex_label_);
  size_t n = vertex_set.size();
  lcc_.reset(new double[n]);
  for (size_t i = 0; i < n; ++i) {
    lcc_[i] = 0.0;
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

  for (vid_t v : vertices_) {
    std::unordered_set<vid_t> nbrs;
    // raw_degree counts every incident (predicate-satisfying) edge with
    // multiplicity, matching the plain undirected LCC denominator; nbrs is the
    // set of distinct neighbors used by the directed denominator and by the
    // edge-among-neighbors counting.
    int64_t raw_degree = 0;
    auto oe_edges = oe_view.get_edges(v);
    for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
      vid_t w = *it;
      if (!in_subgraph[w]) {
        continue;
      }
      if (epred && !(*epred)(triplet, v, w, it.get_data_ptr())) {
        continue;
      }
      ++raw_degree;
      if (w != v) {
        nbrs.insert(w);
      }
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
      ++raw_degree;
      if (u != v) {
        nbrs.insert(u);
      }
    }

    size_t d = nbrs.size();

    if (directed_) {
      if (d <= 1) {
        lcc_[v] = 0.0;
        continue;
      }
      // Count directed edges a->b between distinct neighbors.
      int64_t links = 0;
      for (vid_t a : nbrs) {
        auto a_oe = oe_view.get_edges(a);
        for (auto it = a_oe.begin(); it != a_oe.end(); ++it) {
          vid_t b = *it;
          if (b == a || nbrs.find(b) == nbrs.end()) {
            continue;
          }
          if (epred && !(*epred)(triplet, a, b, it.get_data_ptr())) {
            continue;
          }
          ++links;
        }
      }
      lcc_[v] = static_cast<double>(links) /
                (static_cast<double>(d) * static_cast<double>(d - 1));
    } else {
      // Undirected: denominator is the raw incident-edge degree (matching the
      // plain undirected LCC), numerator is the distinct neighbor pairs that
      // are connected by an edge.
      if (raw_degree <= 1) {
        lcc_[v] = 0.0;
        continue;
      }
      std::set<std::pair<vid_t, vid_t>> pairs;
      for (vid_t a : nbrs) {
        auto a_oe = oe_view.get_edges(a);
        for (auto it = a_oe.begin(); it != a_oe.end(); ++it) {
          vid_t b = *it;
          if (b == a || nbrs.find(b) == nbrs.end()) {
            continue;
          }
          if (epred && !(*epred)(triplet, a, b, it.get_data_ptr())) {
            continue;
          }
          pairs.insert({std::min(a, b), std::max(a, b)});
        }
      }
      lcc_[v] = 2.0 * static_cast<double>(pairs.size()) /
                (static_cast<double>(raw_degree) *
                 static_cast<double>(raw_degree - 1));
    }
  }
}

void LCCPred::sink(execution::Context& ctx, int node_alias, int lcc_alias) {
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
