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

#include "impl/page_rank_undirected_impl.h"

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "utils/parallel_utils.h"

namespace neug {
namespace gds {

UndirectedPageRank::UndirectedPageRank(const StorageReadInterface& graph,
                                       label_t vertex_label, label_t edge_label,
                                       double damping_factor, int concurrency)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_label_(edge_label),
      damping_factor_(damping_factor),
      concurrency_(concurrency) {
  vertex_count_ = graph.GetVertexSet(vertex_label).size();
  pr_.reset(new double[vertex_count_]);
  out_degree_.reset(new uint32_t[vertex_count_]);
  valid_vertices_.reserve(vertex_count_);

  for (const auto& v : graph.GetVertexSet(vertex_label)) {
    valid_vertices_.push_back(v);
  }
  vertex_count_ = valid_vertices_.size();
  auto oe_view =
      graph.GetGenericOutgoingGraphView(vertex_label, vertex_label, edge_label);
  auto ie_view =
      graph.GetGenericIncomingGraphView(vertex_label, vertex_label, edge_label);
  double p = 1.0 / vertex_count_;
  std::vector<uint32_t> dangling_counts(concurrency_, 0);
  ParallelUtils::parallel_for(
      valid_vertices_.data(), valid_vertices_.size(),
      [&](vid_t v, int tid) {
        uint32_t degree = 0;
        auto oe_edges = oe_view.get_edges(v);
        for ([[maybe_unused]] auto it = oe_edges.begin(); it != oe_edges.end();
             ++it) {
          degree++;
        }
        auto ie_edges = ie_view.get_edges(v);
        for ([[maybe_unused]] auto it = ie_edges.begin(); it != ie_edges.end();
             ++it) {
          degree++;
        }
        out_degree_[v] = degree;
        if (degree == 0) {
          dangling_counts[tid]++;
          pr_[v] = p;
        } else {
          pr_[v] = p / degree;
        }
      },
      concurrency_);
  dangling_count_ = 0;
  for (auto count : dangling_counts) {
    dangling_count_ += count;
  }
  dangling_sum_ = p * dangling_count_;
}

void UndirectedPageRank::compute(int max_iterations) {
  std::unique_ptr<double[]> new_pr(
      new double[graph_.GetVertexSet(vertex_label_).size()]);
  auto ie_view = graph_.GetGenericIncomingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);
  auto oe_view = graph_.GetGenericOutgoingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);
  for (int iter = 0; iter < max_iterations; ++iter) {
    double base = (1.0 - damping_factor_) / vertex_count_ +
                  damping_factor_ * dangling_sum_ / vertex_count_;
    dangling_sum_ = base * dangling_count_;
    ParallelUtils::parallel_for(
        valid_vertices_.data(), valid_vertices_.size(),
        [&](vid_t v, int thread_id) {
          double rank_sum = 0.0;
          auto ie_edges = ie_view.get_edges(v);
          for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
            rank_sum += pr_[*it];
          }

          auto oe_edges = oe_view.get_edges(v);
          for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
            rank_sum += pr_[*it];
          }

          uint32_t degree = out_degree_[v];
          new_pr[v] =
              degree > 0 ? (base + damping_factor_ * rank_sum) / degree : base;
          // Ranks are kept divided by degree so neighbors can be summed
          // directly; on the final iteration undo that normalization to
          // recover the actual PageRank value.
          if (iter == max_iterations - 1) {
            double pr = new_pr[v];
            new_pr[v] = degree > 0 ? pr * degree : pr;
          }
        },
        concurrency_);

    std::swap(pr_, new_pr);
  }
}

void UndirectedPageRank::sink(execution::Context& ctx, int node_alias,
                              int pr_alias) {
  execution::MSVertexColumnBuilder builder(vertex_label_);

  execution::ValueColumnBuilder<double> pr_builder;
  pr_builder.reserve(valid_vertices_.size());
  for (vid_t v : valid_vertices_) {
    pr_builder.push_back_opt(pr_[v]);
  }

  builder.append(vertex_label_, std::move(valid_vertices_));
  execution::ContextChunk chunk;
  chunk.set(node_alias, builder.finish());
  chunk.set(pr_alias, pr_builder.finish());
  ctx.append_chunk(std::move(chunk));
}

}  // namespace gds
}  // namespace neug
