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
#include <random>
#include <unordered_map>
#include <unordered_set>
#include "neug/common/columns/value_columns.h"
#include "neug/common/columns/vertex_columns.h"
#include "utils/aggregated_graph.h"
#include "utils/parallel_utils.h"
namespace neug {
namespace gds {
namespace community {
Louvain::Louvain(const StorageReadInterface& graph,
                 std::vector<label_t> vertex_labels,
                 std::vector<LabelTriplet> edge_triplets, double resolution,
                 double threshold, int concurrency,
                 const std::string& initial_community_property,
                 bool allow_relocation, const std::string& weight_property)
    : graph_(graph),
      index_(graph, std::move(vertex_labels), std::move(edge_triplets),
             weight_property),
      resolution_(resolution),
      threshold_(threshold),
      concurrency_(concurrency),
      initial_community_property_(initial_community_property),
      allow_relocation_(allow_relocation) {
  const size_t array_size = index_.array_size();
  const auto& valid_vertices = index_.valid_vertices();
  community_ = std::make_unique<uint32_t[]>(array_size);
  degree_ = std::make_unique<double[]>(array_size);
  stot_ = std::make_unique<double[]>(array_size);
  num_threads_ = concurrency_ > 0
                     ? concurrency_
                     : static_cast<int>(std::thread::hardware_concurrency());
  if (num_threads_ < 1)
    num_threads_ = 1;
  size_t total_scratch = static_cast<size_t>(num_threads_) * array_size;
  thread_comm_weight_ = std::make_unique<double[]>(total_scratch);
  thread_gen_ = std::make_unique<uint32_t[]>(total_scratch);
  std::fill_n(thread_comm_weight_.get(), total_scratch, 0.0);
  std::fill_n(thread_gen_.get(), total_scratch, 0);
  if (!initial_community_property_.empty()) {
    initial_community_ = std::make_unique<uint32_t[]>(array_size);
    std::fill_n(initial_community_.get(), array_size, UINT32_MAX);
    const auto& vlabels = index_.vertex_labels();
    for (size_t li = 0; li < vlabels.size(); ++li) {
      label_t label = vlabels[li];
      auto prop_col =
          graph_.GetVertexPropColumn(label, initial_community_property_);
      const auto& vs = graph_.GetVertexSet(label);
      size_t base = index_.label_base_offset(li);
      for (const auto& v : vs) {
        uint32_t gid = static_cast<uint32_t>(base + v);
        if (prop_col) {
          auto val = prop_col->get_any(v);
          if (!val.IsNull()) {
            int64_t raw = val.GetValue<int64_t>();
            if (raw >= 0 && static_cast<uint64_t>(raw) < array_size) {
              uint32_t cval = static_cast<uint32_t>(raw);
              community_[gid] = cval;
              initial_community_[gid] = cval;
            } else {
              community_[gid] = gid;
            }
          } else
            community_[gid] = gid;
        } else
          community_[gid] = gid;
        stot_[gid] = 0;
        degree_[gid] = 0;
      }
    }
  } else {
    for (uint32_t gid : valid_vertices) {
      community_[gid] = gid;
      stot_[gid] = 0;
      degree_[gid] = 0;
    }
  }
}
void Louvain::compute() {
  const auto& valid_vertices = index_.valid_vertices();
  const size_t array_size = index_.array_size();
  // Parallel degree computation
  ParallelUtils::parallel_for(
      valid_vertices.data(), valid_vertices.size(),
      [&](vid_t gid, int /*tid*/) {
        double deg = 0;
        index_.for_each_neighbor(gid,
                                 [&](uint32_t /*nbr*/, double w) { deg += w; });
        degree_[gid] = deg;
      },
      num_threads_);
  // Parallel m_ computation (sum of out-edge weights = total edge weight)
  std::vector<double> local_m(num_threads_, 0.0);
  ParallelUtils::parallel_for(
      valid_vertices.data(), valid_vertices.size(),
      [&](vid_t gid, int tid) {
        index_.for_each_out_edge(
            gid, [&](uint32_t /*nbr*/, double w) { local_m[tid] += w; });
      },
      num_threads_);
  m_ = 0;
  for (int i = 0; i < num_threads_; ++i)
    m_ += local_m[i];
  if (m_ == 0) {
    modularity_ = 0;
    return;
  }
  // Initialize stot_ (community degree totals)
  std::fill_n(stot_.get(), array_size, 0.0);
  for (uint32_t gid : valid_vertices)
    stot_[community_[gid]] += degree_[gid];
  double prev_mod = -1.0;
  for (int level = 0; level < 100; ++level) {
    bool improved = one_level();
    if (!improved)
      break;
    // Compute modularity (count each undirected edge once via out-edges)
    std::vector<double> local_mod(num_threads_, 0.0);
    ParallelUtils::parallel_for(
        valid_vertices.data(), valid_vertices.size(),
        [&](vid_t gid, int tid) {
          index_.for_each_out_edge(gid, [&](uint32_t ug, double w) {
            if (community_[gid] == community_[ug]) {
              local_mod[tid] += w / (2.0 * m_) - resolution_ * degree_[gid] *
                                                     degree_[ug] /
                                                     (4.0 * m_ * m_);
            }
          });
        },
        num_threads_);
    double new_mod = 0;
    for (int i = 0; i < num_threads_; ++i)
      new_mod += local_mod[i];
    modularity_ = new_mod;
    if (prev_mod >= 0 && std::abs(modularity_ - prev_mod) < threshold_)
      break;
    prev_mod = modularity_;
  }
  // === Graph Aggregation Phase ===
  // Gate: skip in freeze-assign mode (old vertices must not change)
  if (allow_relocation_ || !initial_community_) {
    // Build unified undirected CSR adjacency over valid_vertices
    size_t nv = valid_vertices.size();
    std::vector<size_t> csr_offsets(nv + 1, 0);
    for (size_t vi = 0; vi < nv; ++vi) {
      uint32_t gid = valid_vertices[vi];
      size_t cnt = 0;
      index_.for_each_neighbor(gid, [&](uint32_t, double) { ++cnt; });
      csr_offsets[vi + 1] = cnt;
    }
    for (size_t i = 1; i <= nv; ++i)
      csr_offsets[i] += csr_offsets[i - 1];
    std::vector<uint32_t> csr_adj(csr_offsets[nv]);
    std::vector<double> csr_w(csr_offsets[nv]);
    for (size_t vi = 0; vi < nv; ++vi) {
      uint32_t gid = valid_vertices[vi];
      size_t pos = csr_offsets[vi];
      index_.for_each_neighbor(gid, [&](uint32_t nbr, double w) {
        csr_adj[pos] = nbr;
        csr_w[pos] = w;
        ++pos;
      });
    }
    // Iterative aggregation loop
    for (int agg_level = 0; agg_level < 100; ++agg_level) {
      auto agg =
          build_aggregated_graph(valid_vertices, community_.get(),
                                 degree_.get(), csr_offsets, csr_adj, csr_w);
      if (agg.num_nodes <= 1)
        break;
      std::vector<uint32_t> agg_gen(agg.num_nodes, 0);
      std::vector<double> agg_cw(agg.num_nodes, 0.0);
      bool agg_improved =
          one_level_aggregated(agg, m_, resolution_, agg_gen, agg_cw);
      if (!agg_improved)
        break;
      propagate_aggregated_communities(valid_vertices, community_.get(), agg);
      // Rebuild stot_ after community changes
      std::fill_n(stot_.get(), array_size, 0.0);
      for (uint32_t gid : valid_vertices)
        stot_[community_[gid]] += degree_[gid];
      // Compute modularity on original graph
      std::vector<double> local_mod2(num_threads_, 0.0);
      ParallelUtils::parallel_for(
          valid_vertices.data(), valid_vertices.size(),
          [&](vid_t gid, int tid) {
            index_.for_each_out_edge(gid, [&](uint32_t ug, double w) {
              if (community_[gid] == community_[ug]) {
                local_mod2[tid] += w / (2.0 * m_) - resolution_ * degree_[gid] *
                                                        degree_[ug] /
                                                        (4.0 * m_ * m_);
              }
            });
          },
          num_threads_);
      double new_mod = 0;
      for (int i = 0; i < num_threads_; ++i)
        new_mod += local_mod2[i];
      modularity_ = new_mod;
      if (prev_mod >= 0 && std::abs(modularity_ - prev_mod) < threshold_)
        break;
      prev_mod = modularity_;
    }
  }
}
bool Louvain::one_level() {
  const auto& valid_vertices = index_.valid_vertices();
  std::vector<uint32_t> order = valid_vertices;
  std::mt19937 rng(42);
  std::shuffle(order.begin(), order.end(), rng);
  bool improved = false;
  const size_t n = order.size();
  // Local-moving phase: sequential Gauss-Seidel updates.
  uint32_t* mg = thread_gen_.get();
  double* mc = thread_comm_weight_.get();
  uint32_t gv = 0;
  std::vector<uint32_t> mt;
  mt.reserve(256);
  for (int pass = 0; pass < 10; ++pass) {
    bool moved = false;
    for (size_t i = 0; i < n; ++i) {
      uint32_t ug = order[i];
      if (initial_community_ && !allow_relocation_ &&
          initial_community_[ug] != UINT32_MAX)
        continue;
      uint32_t cc = community_[ug];
      double du = degree_[ug];
      ++gv;
      mt.clear();
      index_.for_each_neighbor(ug, [&](uint32_t vg, double w) {
        if (vg == ug)
          return;
        uint32_t cm = community_[vg];
        if (mg[cm] != gv) {
          mg[cm] = gv;
          mc[cm] = 0.0;
          mt.push_back(cm);
        }
        mc[cm] += w;
      });
      double ws = (mg[cc] == gv) ? mc[cc] : 0.0;
      double sm = stot_[cc] - du;
      uint32_t best = cc;
      double bg = 0.0;
      for (uint32_t cm : mt) {
        if (cm == cc)
          continue;
        double wc = mc[cm];
        double g = (wc - ws) / m_ -
                   resolution_ * stot_[cm] * du / (2.0 * m_ * m_) +
                   resolution_ * sm * du / (2.0 * m_ * m_);
        if (g > bg) {
          bg = g;
          best = cm;
        }
      }
      if (best != cc) {
        stot_[cc] -= du;
        stot_[best] += du;
        community_[ug] = best;
        moved = true;
        improved = true;
      }
    }
    if (!moved)
      break;
  }
  return improved;
}
void Louvain::sink(execution::Context& ctx, int node_alias, int community_alias,
                   int previous_community_alias) {
  std::unordered_map<uint32_t, uint32_t> cr;
  if (initial_community_) {
    // Stable ID: inherit old community IDs via majority vote
    std::unordered_map<uint32_t, std::unordered_map<uint32_t, uint32_t>>
        new_to_old_counts;
    uint32_t max_old_id = 0;
    bool has_valid_old = false;
    for (uint32_t gid : index_.valid_vertices()) {
      uint32_t new_com = community_[gid];
      uint32_t old_com = initial_community_[gid];
      if (old_com != UINT32_MAX) {
        new_to_old_counts[new_com][old_com]++;
        if (!has_valid_old || old_com > max_old_id) {
          max_old_id = old_com;
          has_valid_old = true;
        }
      } else {
        new_to_old_counts[new_com];
      }
    }
    std::vector<std::pair<uint32_t, uint32_t>> com_sizes;
    for (auto& [nc, old_counts] : new_to_old_counts) {
      uint32_t total = 0;
      for (auto& [_, cnt] : old_counts)
        total += cnt;
      com_sizes.push_back({nc, total});
    }
    std::sort(
        com_sizes.begin(), com_sizes.end(), [](const auto& a, const auto& b) {
          if (a.second != b.second)
            return a.second > b.second;
          return a.first < b.first;  // deterministic tie-break by new comm ID
        });
    std::unordered_set<uint32_t> used_ids;
    for (auto& [nc, _] : com_sizes) {
      auto& old_counts = new_to_old_counts[nc];
      uint32_t best_old = UINT32_MAX;
      uint32_t best_count = 0;
      for (auto& [oc, cnt] : old_counts) {
        // Prefer higher count; on tie, prefer smaller old community ID
        // for deterministic results across runs.
        if ((cnt > best_count ||
             (cnt == best_count && best_old != UINT32_MAX && oc < best_old)) &&
            used_ids.find(oc) == used_ids.end()) {
          best_count = cnt;
          best_old = oc;
        }
      }
      if (best_old != UINT32_MAX) {
        cr[nc] = best_old;
        used_ids.insert(best_old);
      }
    }
    uint32_t next_fresh = has_valid_old ? (max_old_id + 1) : 0;
    for (auto& [nc, _] : com_sizes) {
      if (cr.find(nc) == cr.end()) {
        while (used_ids.find(next_fresh) != used_ids.end())
          next_fresh++;
        cr[nc] = next_fresh;
        used_ids.insert(next_fresh);
        next_fresh++;
      }
    }
  } else {
    uint32_t ni = 0;
    for (uint32_t gid : index_.valid_vertices()) {
      uint32_t c = community_[gid];
      if (cr.find(c) == cr.end())
        cr[c] = ni++;
    }
  }
  bool need_prev = (previous_community_alias >= 0);
  const auto& vlabels = index_.vertex_labels();
  for (size_t li = 0; li < vlabels.size(); ++li) {
    label_t label = vlabels[li];
    size_t base = index_.label_base_offset(li);
    const auto& vs = graph_.GetVertexSet(label);
    MSVertexColumnBuilder b(label);
    ValueColumnBuilder<int64_t> cb;
    size_t cnt = vs.size();
    b.reserve(cnt);
    cb.reserve(cnt);
    std::shared_ptr<IContextColumn> prev_col;
    if (need_prev) {
      ValueColumnBuilder<int64_t> prev_builder(/*is_optional=*/true);
      prev_builder.reserve(cnt);
      for (const auto& v : vs) {
        uint32_t gid = static_cast<uint32_t>(base + v);
        if (initial_community_ && initial_community_[gid] != UINT32_MAX) {
          prev_builder.push_back_opt(
              static_cast<int64_t>(initial_community_[gid]));
        } else {
          prev_builder.push_back_null();
        }
      }
      prev_col = prev_builder.finish();
    }
    for (const auto& v : vs) {
      uint32_t gid = static_cast<uint32_t>(base + v);
      b.push_back_opt(v);
      cb.push_back_opt(static_cast<int64_t>(cr[community_[gid]]));
    }
    execution::ContextChunk chunk;
    chunk.set(node_alias, b.finish());
    chunk.set(community_alias, cb.finish());
    if (prev_col)
      chunk.set(previous_community_alias, prev_col);
    ctx.append_chunk(std::move(chunk));
  }
}
}  // namespace community
}  // namespace gds
}  // namespace neug
