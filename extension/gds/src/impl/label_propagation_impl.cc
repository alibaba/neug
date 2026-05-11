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

#include "impl/label_propagation_impl.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <thread>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "parallel_utils.h"

namespace neug {
namespace gds {

LabelPropagation::LabelPropagation(const StorageReadInterface& graph,
                                   label_t vertex_label,
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
      edge_pred_(edge_pred) {
  if (concurrency_ <= 0) {
    concurrency_ = static_cast<int>(std::thread::hardware_concurrency());
    if (concurrency_ <= 0) {
      concurrency_ = 1;
    }
  }
}

template <typename PRED_T>
void LabelPropagation::init_communities(const PRED_T& vertex_pred) {
  auto vertex_set = graph_.GetVertexSet(vertex_label_);
  auto begin = std::chrono::high_resolution_clock::now();

  community_.assign(vertex_set.size(), std::numeric_limits<int64_t>::max());
  next_community_.assign(vertex_set.size(),
                         std::numeric_limits<int64_t>::max());
  vertices_.clear();
  vertices_.reserve(vertex_set.size());

  auto id_column = dynamic_cast<const TypedRefColumn<int64_t>*>(
      graph_.GetVertexPropColumn(vertex_label_, "id").get());
  if (id_column != nullptr) {
    for (vid_t v : vertex_set) {
      if (vertex_pred(vertex_label_, v)) {
        int64_t id = id_column->get_view(v);
        community_[v] = id;
        next_community_[v] = id;
        vertices_.push_back(v);
      }
    }
  } else {
    for (vid_t v : vertex_set) {
      if (vertex_pred(vertex_label_, v)) {
        community_[v] = v;
        next_community_[v] = v;
        vertices_.push_back(v);
      }
    }
  }

  LOG(INFO) << "Initialized communities for " << vertices_.size()
            << " vertices in "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - begin)
                   .count()
            << " ms.";
}

void LabelPropagation::collect_neighbor_communities(
    vid_t dst_vid, std::vector<int64_t>& communities) const {
  const auto& ie_view = graph_.GetGenericIncomingGraphView(
      edge_triplet_.dst_label, edge_triplet_.src_label,
      edge_triplet_.edge_label);
  const auto& oe_view = graph_.GetGenericOutgoingGraphView(
      edge_triplet_.src_label, edge_triplet_.dst_label,
      edge_triplet_.edge_label);

  communities.clear();

  auto ie_edges = ie_view.get_edges(dst_vid);
  for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
    const vid_t src_vid = *it;
    if (community_[src_vid] != std::numeric_limits<int64_t>::max()) {
      communities.push_back(community_[src_vid]);
    }
  }

  auto oe_edges = oe_view.get_edges(dst_vid);
  for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
    const vid_t src_vid = *it;
    if (community_[src_vid] != std::numeric_limits<int64_t>::max()) {
      communities.push_back(community_[src_vid]);
    }
  }
}

int64_t LabelPropagation::get_majority_community(
    std::vector<int64_t>& communities) const {
  int32_t max_count = 0;
  std::sort(communities.begin(), communities.end());

  int64_t best = communities[0];
  int32_t count = 1;
  for (size_t i = 1; i < communities.size(); ++i) {
    if (communities[i] == communities[i - 1]) {
      ++count;
    } else {
      if (count > max_count) {
        max_count = count;
        best = communities[i - 1];
      }
      count = 1;
    }
  }
  if (count > max_count) {
    best = communities.back();
  }
  return best;
}

bool LabelPropagation::run_single_iteration(int iteration) {
  std::vector<bool> updated_by_thread(concurrency_, false);
  auto begin = std::chrono::high_resolution_clock::now();

  ParallelUtils::parallel_for(
      vertices_.data(), vertices_.size(),
      [&](vid_t idx, int tid) {
        vid_t dst_vid = vertices_[idx];
        std::vector<int64_t> neighbor_communities;
        collect_neighbor_communities(dst_vid, neighbor_communities);

        if (neighbor_communities.empty()) {
          next_community_[dst_vid] = community_[dst_vid];
          return;
        }

        int64_t best = get_majority_community(neighbor_communities);
        next_community_[dst_vid] = best;
        if (best != community_[dst_vid]) {
          updated_by_thread[tid] = true;
        }
      },
      concurrency_);

  bool updated = false;
  for (int i = 0; i < concurrency_; ++i) {
    updated = updated || updated_by_thread[i];
  }

  LOG(INFO) << "Iteration " << iteration + 1 << " completed in "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - begin)
                   .count()
            << " ms.";
  return updated;
}

void LabelPropagation::compute() {
  // Keep edge_pred_ for API consistency; edge predicates are not evaluated yet.
  (void) edge_pred_;

  if (vertex_pred_) {
    auto expr = vertex_pred_->bind(&graph_, {});
    execution::GeneralPred vertex_pred(std::move(expr));
    init_communities(vertex_pred);
  } else {
    execution::DummyPred vertex_pred;
    init_communities(vertex_pred);
  }

  for (int iteration = 0; iteration < max_iterations_; ++iteration) {
    bool updated = run_single_iteration(iteration);
    if (!updated) {
      break;
    }
    community_.swap(next_community_);
  }
}

void LabelPropagation::sink(execution::Context& ctx, int32_t node_alias,
                            int32_t label_alias) {
  execution::MSVertexColumnBuilder node_builder(vertex_label_);
  execution::ValueColumnBuilder<int64_t> label_builder;
  node_builder.reserve(vertices_.size());
  label_builder.reserve(vertices_.size());

  for (vid_t v : vertices_) {
    node_builder.push_back_opt(v);
    label_builder.push_back_opt(community_[v]);
  }

  ctx.set(node_alias, node_builder.finish());
  ctx.set(label_alias, label_builder.finish());
}

}  // namespace gds
}  // namespace neug
