/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "impl/louvain_impl.h"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <numeric>
#include <random>
#include <unordered_map>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "utils/parallel_utils.h"

namespace neug {
namespace gds {
namespace community {

Louvain::Louvain(const StorageReadInterface& graph, label_t vertex_label,
                 label_t edge_label, double resolution, double threshold,
                 int concurrency)
    : graph_(graph),
      vertex_label_(vertex_label),
      edge_label_(edge_label),
      resolution_(resolution),
      threshold_(threshold),
      concurrency_(concurrency) {
  const auto& vertex_set = graph.GetVertexSet(vertex_label);
  valid_vertices_.reserve(vertex_set.size());
  vid_t max_vid = 0;
  for (const auto& v : vertex_set) {
    valid_vertices_.push_back(v);
    if (v > max_vid) max_vid = v;
  }
  vertex_count_ = valid_vertices_.size();

  // Allocate arrays indexed by vid_t
  // For contiguous vid_t values, size = max_vid + 1 ≈ vertex_count_
  array_size_ = static_cast<size_t>(max_vid) + 1;
  community_ = std::make_unique<uint32_t[]>(array_size_);
  degree_ = std::make_unique<double[]>(array_size_);
  stot_ = std::make_unique<double[]>(array_size_);

  // Per-thread scratch for parallel moving phase
  num_threads_ = concurrency_ > 0 ? concurrency_
                                  : static_cast<int>(std::thread::hardware_concurrency());
  if (num_threads_ < 1) num_threads_ = 1;
  size_t total_scratch = static_cast<size_t>(num_threads_) * array_size_;
  thread_comm_weight_ = std::make_unique<double[]>(total_scratch);
  thread_gen_ = std::make_unique<uint32_t[]>(total_scratch);
  std::fill_n(thread_comm_weight_.get(), total_scratch, 0.0);
  std::fill_n(thread_gen_.get(), total_scratch, 0);

  // Initialize: each vertex in its own community
  for (vid_t v : valid_vertices_) {
    community_[v] = v;
    stot_[v] = 0;
    degree_[v] = 0;
  }
}

void Louvain::compute() {
  auto oe_view = graph_.GetGenericOutgoingGraphView(
      vertex_label_, vertex_label_, edge_label_);
  auto ie_view = graph_.GetGenericIncomingGraphView(
      vertex_label_, vertex_label_, edge_label_);

  // Compute degrees: for undirected graph, degree = out_degree + in_degree
  ParallelUtils::parallel_for(
      valid_vertices_.data(), valid_vertices_.size(),
      [&](vid_t v, int /*tid*/) {
        double deg = 0;
        auto oes = oe_view.get_edges(v);
        for (auto it = oes.begin(); it != oes.end(); ++it) deg += 1.0;
        auto ies = ie_view.get_edges(v);
        for (auto it = ies.begin(); it != ies.end(); ++it) deg += 1.0;
        degree_[v] = deg;
      },
      concurrency_);

  // Compute total edge weight m (for undirected: each edge counted once)
  // We count out-edges only, which gives us m for undirected graphs
  {
    std::vector<double> local_m(num_threads_, 0.0);
    ParallelUtils::parallel_for(
        valid_vertices_.data(), valid_vertices_.size(),
        [&](vid_t v, int tid) {
          auto oes = oe_view.get_edges(v);
          double cnt = 0;
          for (auto it = oes.begin(); it != oes.end(); ++it) cnt += 1.0;
          local_m[tid] += cnt;
        },
        num_threads_);
    m_ = 0;
    for (int i = 0; i < num_threads_; ++i) m_ += local_m[i];
  }

  if (m_ == 0) {
    modularity_ = 0;
    return;
  }

  // Initialize stot in parallel
  ParallelUtils::parallel_for(
      valid_vertices_.data(), valid_vertices_.size(),
      [&](vid_t v, int /*tid*/) { stot_[v] = degree_[v]; }, num_threads_);

  // Iterate with convergence detection using threshold_
  double prev_mod = -1.0;
  for (int level = 0; level < 100; ++level) {
    bool improved = one_level();
    if (!improved) break;

    // Compute modularity to check convergence
    std::vector<double> local_mod(num_threads_, 0.0);
    ParallelUtils::parallel_for(
        valid_vertices_.data(), valid_vertices_.size(),
        [&](vid_t v, int tid) {
          auto oes = oe_view.get_edges(v);
          double lm = 0;
          for (auto it = oes.begin(); it != oes.end(); ++it) {
            vid_t u = *it;
            if (community_[v] == community_[u]) {
              lm += 1.0 / (2.0 * m_) -
                    degree_[v] * degree_[u] / (4.0 * m_ * m_);
            }
          }
          local_mod[tid] += lm;
        },
        num_threads_);
    modularity_ = 0;
    for (int i = 0; i < num_threads_; ++i) modularity_ += local_mod[i];

    if (prev_mod >= 0 && std::abs(modularity_ - prev_mod) < threshold_) {
      break;
    }
    prev_mod = modularity_;
  }

  // Compute final modularity (if not already up-to-date)
  if (prev_mod < 0) {
    std::vector<double> local_mod(concurrency_, 0.0);
    ParallelUtils::parallel_for(
        valid_vertices_.data(), valid_vertices_.size(),
        [&](vid_t v, int tid) {
          auto oes = oe_view.get_edges(v);
          double lm = 0;
          for (auto it = oes.begin(); it != oes.end(); ++it) {
            vid_t u = *it;
            if (community_[v] == community_[u]) {
              lm += 1.0 / (2.0 * m_) -
                    degree_[v] * degree_[u] / (4.0 * m_ * m_);
            }
          }
          local_mod[tid] += lm;
        },
        concurrency_);
    modularity_ = 0;
    for (int i = 0; i < concurrency_; ++i) modularity_ += local_mod[i];
  }
}

bool Louvain::one_level() {
  auto oe_view = graph_.GetGenericOutgoingGraphView(
      vertex_label_, vertex_label_, edge_label_);
  auto ie_view = graph_.GetGenericIncomingGraphView(
      vertex_label_, vertex_label_, edge_label_);

  std::vector<vid_t> order = valid_vertices_;
  std::mt19937 rng(42);
  std::shuffle(order.begin(), order.end(), rng);

  bool improved = false;
  const size_t n = order.size();
  const size_t chunk = 4096;
  const size_t num_batches = (n + chunk - 1) / chunk;
  const int nt = num_threads_;

  // best_com[i] stores the computed best community for order[i]
  std::vector<uint32_t> best_com(n);

  // Per-thread touched community lists (reused across batches)
  std::vector<std::vector<uint32_t>> touched(nt);
  for (int t = 0; t < nt; ++t) touched[t].reserve(256);

  for (int pass = 0; pass < 10; ++pass) {
    bool moved = false;

    for (size_t batch = 0; batch < num_batches; ++batch) {
      size_t batch_start = batch * chunk;
      size_t batch_end = std::min(batch_start + chunk, n);

      // Phase 1: Compute best move for each vertex in this batch (parallel)
      {
        std::atomic<size_t> cursor(batch_start);
        std::vector<std::thread> threads;
        threads.reserve(nt - 1);

        auto worker = [&](int tid) {
          uint32_t* my_gen = thread_gen_.get() + static_cast<size_t>(tid) * array_size_;
          double* my_cw = thread_comm_weight_.get() + static_cast<size_t>(tid) * array_size_;
          uint32_t gen_val = 0;
          auto& my_touched = touched[tid];

          while (true) {
            size_t start = cursor.fetch_add(64);
            if (start >= batch_end) break;
            size_t end = std::min(start + size_t(64), batch_end);

            for (size_t i = start; i < end; ++i) {
              vid_t u = order[i];
              uint32_t cur_com = community_[u];
              double deg_u = degree_[u];

              ++gen_val;
              my_touched.clear();

              auto process_nbr = [&](vid_t v) {
                if (v == u) return;
                uint32_t com = community_[v];
                if (my_gen[com] != gen_val) {
                  my_gen[com] = gen_val;
                  my_cw[com] = 0.0;
                  my_touched.push_back(com);
                }
                my_cw[com] += 1.0;
              };

              auto oes = oe_view.get_edges(u);
              for (auto it = oes.begin(); it != oes.end(); ++it)
                process_nbr(*it);
              auto ies = ie_view.get_edges(u);
              for (auto it = ies.begin(); it != ies.end(); ++it)
                process_nbr(*it);

              double w_self =
                  (my_gen[cur_com] == gen_val) ? my_cw[cur_com] : 0.0;

              // Remove u from current community for gain calculation
              double stot_cur_minus_u = stot_[cur_com] - deg_u;

              uint32_t best = cur_com;
              double best_gain = 0.0;

              for (uint32_t com : my_touched) {
                if (com == cur_com) continue;
                double w_com = my_cw[com];
                // Gain = benefit of joining com - cost of leaving cur_com
                double gain = (w_com - w_self) / m_ -
                              resolution_ * stot_[com] * deg_u / (2.0 * m_ * m_) +
                              resolution_ * stot_cur_minus_u * deg_u / (2.0 * m_ * m_);
                if (gain > best_gain) {
                  best_gain = gain;
                  best = com;
                }
              }

              best_com[i] = best;
            }
          }
        };

        for (int t = 1; t < nt; ++t)
          threads.emplace_back(worker, t);
        worker(0);
        for (auto& th : threads) th.join();
      }

      // Phase 2: Apply moves (sequential — safe stot_ updates)
      for (size_t i = batch_start; i < batch_end; ++i) {
        vid_t u = order[i];
        uint32_t cur_com = community_[u];
        uint32_t new_com = best_com[i];
        if (new_com != cur_com) {
          stot_[cur_com] -= degree_[u];
          stot_[new_com] += degree_[u];
          community_[u] = new_com;
          moved = true;
          improved = true;
        }
      }
    }

    if (!moved) break;
  }

  return improved;
}

void Louvain::sink(execution::Context& ctx, int node_alias,
                   int community_alias) {
  execution::MSVertexColumnBuilder builder(vertex_label_);
  builder.reserve(valid_vertices_.size());
  execution::ValueColumnBuilder<int64_t> community_builder;
  community_builder.reserve(valid_vertices_.size());

  // Remap communities to contiguous [0, num_coms)
  std::unordered_map<uint32_t, uint32_t> com_remap;
  uint32_t next_id = 0;

  for (vid_t v : valid_vertices_) {
    uint32_t c = community_[v];
    if (com_remap.find(c) == com_remap.end()) com_remap[c] = next_id++;
    builder.push_back_opt(v);
    community_builder.push_back_opt(static_cast<int64_t>(com_remap[c]));
  }

  execution::DataChunk chunk;
  chunk.set(node_alias, builder.finish());
  chunk.set(community_alias, community_builder.finish());
  ctx.append_chunk(std::move(chunk));
}

LouvainResult RunLouvain(const StorageReadInterface& graph,
                         label_t vertex_label, label_t edge_label,
                         bool directed, double resolution, double threshold,
                         int concurrency) {
  (void)directed;  // TODO: directed support

  Louvain louvain(graph, vertex_label, edge_label, resolution, threshold,
                  concurrency);
  louvain.compute();

  LouvainResult result;
  // Result is stored internally; sink() method is used for output
  result.modularity = 0;
  result.num_communities = 0;
  return result;
}

}  // namespace community
}  // namespace gds
}  // namespace neug
