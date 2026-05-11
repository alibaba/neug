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

#include <chrono>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "parallel_utils.h"

namespace neug {
namespace gds {

struct LabelPropagation {
  LabelPropagation(const StorageReadInterface& graph, label_t vertex_label,
                   const execution::LabelTriplet& edge_triplet,
                   int max_iterations, int concurrency)
      : graph(graph),
        vertex_label(vertex_label),
        edge_triplet(edge_triplet),
        max_iterations(max_iterations),
        concurrency_(concurrency) {}

  template <typename PRED_T>
  void init_communities(const PRED_T& vertex_pred) {
    auto vertex_set = graph.GetVertexSet(vertex_label);
    auto begin = std::chrono::high_resolution_clock::now();
    community.resize(vertex_set.size(), std::numeric_limits<int64_t>::max());
    next_community.resize(vertex_set.size(),
                          std::numeric_limits<int64_t>::max());
    vertices.reserve(vertex_set.size());
    auto id_column = dynamic_cast<const TypedRefColumn<int64_t>*>(
        graph.GetVertexPropColumn(vertex_label, "id").get());
    if (id_column != nullptr) {
      for (vid_t v : vertex_set) {
        if (vertex_pred(vertex_label, v)) {
          community[v] = id_column->get_view(v);
          next_community[v] = id_column->get_view(v);
          vertices.push_back(v);
        }
      }
    } else {
      for (vid_t v : vertex_set) {
        if (vertex_pred(vertex_label, v)) {
          community[v] = v;
          next_community[v] = v;
          vertices.push_back(v);
        }
      }
    }

    LOG(INFO) << "Initialized communities for " << vertices.size()
              << " vertices in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::high_resolution_clock::now() - begin)
                     .count()
              << " ms.";
  }

  vid_t get_max_community(std::vector<int64_t>& thread_communities) {
    int32_t max_count = 0;
    std::sort(thread_communities.begin(), thread_communities.end());
    vid_t max_community = thread_communities[0];
    int32_t count = 1;
    for (size_t j = 1; j < thread_communities.size(); ++j) {
      if (thread_communities[j] == thread_communities[j - 1]) {
        count++;
      } else {
        if (count > max_count) {
          max_count = count;
          max_community = thread_communities[j - 1];
        }
        count = 1;
      }
    }
    if (count > max_count) {
      max_count = count;
      max_community = thread_communities.back();
    }
    return max_community;
  }

  void propagate_labels() {
    const auto& ie_view = graph.GetGenericIncomingGraphView(
        edge_triplet.dst_label, edge_triplet.src_label,
        edge_triplet.edge_label);
    const auto& oe_view = graph.GetGenericOutgoingGraphView(
        edge_triplet.src_label, edge_triplet.dst_label,
        edge_triplet.edge_label);
    for (int iteration = 0; iteration < max_iterations; ++iteration) {
      std::vector<bool> update(concurrency_, false);
      auto begin = std::chrono::high_resolution_clock::now();
      ParallelUtils::parallel_for(
          vertices.data(), vertices.size(),
          [&](vid_t i, int tid) {
            vid_t dst_vid = vertices[i];
            auto edges = ie_view.get_edges(dst_vid);
            std::vector<int64_t> thread_communities;

            for (auto it = edges.begin(); it != edges.end(); ++it) {
              const vid_t src_vid = *it;
              if (community[src_vid] != std::numeric_limits<int64_t>::max()) {
                thread_communities.push_back(community[src_vid]);
              }
            }
            auto oe_edges = oe_view.get_edges(dst_vid);
            for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
              const vid_t src_vid = *it;
              if (community[src_vid] != std::numeric_limits<int64_t>::max()) {
                thread_communities.push_back(community[src_vid]);
              }
            }
            if (thread_communities.empty()) {
              return;
            }
            vid_t max_community = get_max_community(thread_communities);
            next_community[dst_vid] = max_community;
            if (max_community != community[dst_vid]) {
              update[tid] = true;
            }
          },
          concurrency_);
      bool updated = false;
      for (int i = 0; i < concurrency_; ++i) {
        updated = updated || update[i];
        update[i] = false;
      }
      if (!updated) {
        break;
      }
      community.swap(next_community);
      LOG(INFO) << "Iteration " << iteration + 1 << " completed in "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::high_resolution_clock::now() - begin)
                       .count()
                << " ms.";
    }
  }

  void sink(execution::Context& ctx, int32_t node_alias, int32_t label_alias) {
    execution::MSVertexColumnBuilder node_builder(vertex_label);
    execution::ValueColumnBuilder<int64_t> label_builder;
    node_builder.reserve(vertices.size());
    label_builder.reserve(vertices.size());
    for (vid_t v : vertices) {
      node_builder.push_back_opt(v);
      label_builder.push_back_opt(community[v]);
    }

    ctx.set(node_alias, node_builder.finish());
    ctx.set(label_alias, label_builder.finish());
  }

  const StorageReadInterface& graph;
  label_t vertex_label;
  execution::LabelTriplet edge_triplet;
  int max_iterations;
  std::vector<int64_t> community;
  std::vector<int64_t> next_community;
  std::vector<vid_t> vertices;
  int concurrency_;
};

execution::Context RunLabelPropagation(const LabelPropagationRunArgs& args,
                                       const StorageReadInterface& graph,
                                       execution::Context& ctx) {
  LabelPropagation label_propagation(graph, args.vertex_label,
                                     args.edge_triplet, args.max_iterations,
                                     args.concurrency);
  if (args.vertex_pred) {
    auto expr = args.vertex_pred->bind(&graph, {});
    execution::GeneralPred vertex_pred(std::move(expr));
    label_propagation.init_communities(vertex_pred);
  } else {
    execution::DummyPred vertex_pred;
    label_propagation.init_communities(vertex_pred);
  }

  label_propagation.propagate_labels();

  label_propagation.sink(ctx, args.node_alias, args.label_alias);
  return ctx;
}

}  // namespace gds
}  // namespace neug
