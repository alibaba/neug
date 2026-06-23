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

#include "impl/cdlp_pred_impl.h"

#include <limits>
#include <memory>
#include <unordered_map>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/expression/predicates.h"

namespace neug {
namespace gds {

namespace {
constexpr int64_t kExcluded = std::numeric_limits<int64_t>::max();
}  // namespace

CDLPPred::CDLPPred(const StorageReadInterface& graph, label_t vertex_label,
                   const execution::LabelTriplet& edge_triplet,
                   int max_iterations, int concurrency,
                   execution::ExprBase* vertex_pred,
                   execution::ExprBase* edge_pred)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_triplet_(edge_triplet),
      max_iterations_(max_iterations),
      concurrency_(concurrency),
      vertex_pred_(vertex_pred),
      edge_pred_(edge_pred) {}

// Simple sequential synchronous label propagation. Each round every vertex
// adopts the most frequent label among its (predicate-satisfying) neighbors,
// breaking ties towards the smallest label, matching the plain CDLP.
void CDLPPred::compute() {
  const auto& vertex_set = graph_.GetVertexSet(vertex_label_);
  size_t n = vertex_set.size();
  community_.reset(new int64_t[n]);

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
      community_[v] = kExcluded;
    } else {
      community_[v] =
          id_column ? id_column->get_view(v) : static_cast<int64_t>(v);
      vertices_.push_back(v);
    }
  }

  auto ie_view = graph_.GetGenericIncomingGraphView(
      edge_triplet_.dst_label, edge_triplet_.src_label,
      edge_triplet_.edge_label);
  auto oe_view = graph_.GetGenericOutgoingGraphView(
      edge_triplet_.src_label, edge_triplet_.dst_label,
      edge_triplet_.edge_label);

  std::vector<int64_t> next(n);
  for (vid_t v : vertices_) {
    next[v] = community_[v];
  }

  for (int iteration = 0; iteration < max_iterations_; ++iteration) {
    bool changed = false;
    for (vid_t v : vertices_) {
      std::unordered_map<int64_t, int> counts;

      auto ie_edges = ie_view.get_edges(v);
      for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
        vid_t nbr = *it;
        if (community_[nbr] == kExcluded) {
          continue;
        }
        if (epred && !(*epred)(edge_triplet_, nbr, v, it.get_data_ptr())) {
          continue;
        }
        counts[community_[nbr]]++;
      }
      auto oe_edges = oe_view.get_edges(v);
      for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
        vid_t nbr = *it;
        if (community_[nbr] == kExcluded) {
          continue;
        }
        if (epred && !(*epred)(edge_triplet_, v, nbr, it.get_data_ptr())) {
          continue;
        }
        counts[community_[nbr]]++;
      }

      if (counts.empty()) {
        next[v] = community_[v];
        continue;
      }
      int64_t best = -1;
      int best_count = -1;
      for (const auto& kv : counts) {
        if (kv.second > best_count ||
            (kv.second == best_count && kv.first < best)) {
          best = kv.first;
          best_count = kv.second;
        }
      }
      next[v] = best;
      if (best != community_[v]) {
        changed = true;
      }
    }

    for (vid_t v : vertices_) {
      community_[v] = next[v];
    }
    if (!changed) {
      break;
    }
  }
}

void CDLPPred::sink(execution::Context& ctx, int32_t node_alias,
                    int32_t label_alias) {
  execution::MSVertexColumnBuilder node_builder(vertex_label_);
  execution::ValueColumnBuilder<int64_t> label_builder;
  label_builder.reserve(vertices_.size());

  for (vid_t v : vertices_) {
    label_builder.push_back_opt(community_[v]);
  }
  node_builder.append(vertex_label_, std::move(vertices_));

  execution::DataChunk chunk;
  chunk.set(node_alias, node_builder.finish());
  chunk.set(label_alias, label_builder.finish());
  ctx.append_chunk(std::move(chunk));
}

}  // namespace gds
}  // namespace neug
