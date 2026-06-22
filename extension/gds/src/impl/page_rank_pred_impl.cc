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

#include "impl/page_rank_pred_impl.h"

#include <memory>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/expression/predicates.h"

namespace neug {
namespace gds {

PageRankPred::PageRankPred(const StorageReadInterface& graph,
                           label_t vertex_label, label_t edge_label,
                           double damping_factor, int max_iterations,
                           int concurrency, bool directed,
                           execution::ExprBase* vertex_pred,
                           execution::ExprBase* edge_pred)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_label_(edge_label),
      damping_factor_(damping_factor),
      max_iterations_(max_iterations),
      concurrency_(concurrency),
      directed_(directed),
      vertex_pred_(vertex_pred),
      edge_pred_(edge_pred) {}

// Simple sequential power iteration mirroring the plain PageRank, restricted
// to the predicate-defined subgraph. For directed graphs the rank flows along
// out-edges; for undirected graphs both directions contribute (degree counts
// all incident edges), matching the plain directed/undirected variants.
void PageRankPred::compute() {
  const auto& vertex_set = graph_.GetVertexSet(vertex_label_);
  size_t capacity = vertex_set.size();
  pr_.reset(new double[capacity]);

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

  std::vector<char> in_subgraph(capacity, 0);
  for (vid_t v : vertex_set) {
    if (!vpred || (*vpred)(vertex_label_, v)) {
      in_subgraph[v] = 1;
      vertices_.push_back(v);
    }
  }
  size_t n = vertices_.size();
  if (n == 0) {
    return;
  }

  execution::LabelTriplet triplet{vertex_label_, vertex_label_, edge_label_};
  auto oe_view = graph_.GetGenericOutgoingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);
  auto ie_view = graph_.GetGenericIncomingGraphView(vertex_label_,
                                                    vertex_label_, edge_label_);

  // contributors[v] are the vertices whose rank flows into v; degree[v] is the
  // out-degree (directed) or total incident degree (undirected) used to split
  // each vertex's rank among its targets.
  std::vector<std::vector<vid_t>> contributors(capacity);
  std::vector<int> degree(capacity, 0);

  for (vid_t v : vertices_) {
    auto ie_edges = ie_view.get_edges(v);
    for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
      vid_t u = *it;
      if (!in_subgraph[u]) {
        continue;
      }
      if (epred && !(*epred)(triplet, u, v, it.get_data_ptr())) {
        continue;
      }
      contributors[v].push_back(u);  // in-edge u -> v
    }

    int out_deg = 0;
    auto oe_edges = oe_view.get_edges(v);
    for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
      vid_t w = *it;
      if (!in_subgraph[w]) {
        continue;
      }
      if (epred && !(*epred)(triplet, v, w, it.get_data_ptr())) {
        continue;
      }
      ++out_deg;
      if (!directed_) {
        contributors[v].push_back(w);  // undirected: out-edge also contributes
      }
    }

    degree[v] = directed_ ? out_deg : static_cast<int>(contributors[v].size());
  }

  double dn = static_cast<double>(n);
  double p = 1.0 / dn;
  int dangling_count = 0;
  for (vid_t v : vertices_) {
    if (degree[v] == 0) {
      pr_[v] = p;
      ++dangling_count;
    } else {
      pr_[v] = p / degree[v];
    }
  }
  double dangling_sum = p * dangling_count;

  std::unique_ptr<double[]> new_pr(new double[capacity]);
  for (int iter = 0; iter < max_iterations_; ++iter) {
    double base = (1.0 - damping_factor_) / dn +
                  damping_factor_ * dangling_sum / dn;

    if (directed_) {
      double next_dangling = 0.0;
      for (vid_t v : vertices_) {
        double rank_sum = 0.0;
        for (vid_t u : contributors[v]) {
          rank_sum += pr_[u];
        }
        new_pr[v] = base + damping_factor_ * rank_sum;
        if (degree[v] == 0) {
          next_dangling += new_pr[v];
        } else {
          new_pr[v] /= degree[v];
        }
      }
      for (vid_t v : vertices_) {
        pr_[v] = new_pr[v];
      }
      dangling_sum = next_dangling;
    } else {
      dangling_sum = base * dangling_count;
      for (vid_t v : vertices_) {
        double rank_sum = 0.0;
        for (vid_t u : contributors[v]) {
          rank_sum += pr_[u];
        }
        new_pr[v] = degree[v] > 0
                        ? (base + damping_factor_ * rank_sum) / degree[v]
                        : base;
        if (iter == max_iterations_ - 1 && degree[v] > 0) {
          new_pr[v] *= degree[v];
        }
      }
      for (vid_t v : vertices_) {
        pr_[v] = new_pr[v];
      }
    }
  }

  if (directed_) {
    for (vid_t v : vertices_) {
      if (degree[v] > 0) {
        pr_[v] *= degree[v];
      }
    }
  }
}

void PageRankPred::sink(execution::Context& ctx, int node_alias, int pr_alias) {
  execution::MSVertexColumnBuilder node_builder(vertex_label_);
  execution::ValueColumnBuilder<double> pr_builder;
  pr_builder.reserve(vertices_.size());

  for (vid_t v : vertices_) {
    pr_builder.push_back_opt(pr_[v]);
  }
  node_builder.append(vertex_label_, std::move(vertices_));

  execution::DataChunk chunk;
  chunk.set(node_alias, node_builder.finish());
  chunk.set(pr_alias, pr_builder.finish());
  ctx.append_chunk(std::move(chunk));
}

}  // namespace gds
}  // namespace neug
