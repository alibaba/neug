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

#include "impl/leiden_impl.h"
#include <algorithm>
#include <atomic>
#include <cmath>
#include <random>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include "neug/common/columns/value_columns.h"
#include "neug/common/columns/vertex_columns.h"
#include "utils/aggregated_graph.h"
#include "utils/parallel_utils.h"
namespace neug {
namespace gds {
namespace community {
Leiden::Leiden(const StorageReadInterface& graph,
               std::vector<label_t> vertex_labels,
               std::vector<LabelTriplet> edge_triplets, double resolution,
               double threshold, int concurrency,
               const std::string& initial_community_property,
               bool allow_relocation, const std::string& weight_property,
               std::vector<execution::ExprBase*> vertex_preds,
               std::vector<execution::ExprBase*> edge_preds)
    : graph_(graph),
      index_(graph, std::move(vertex_labels), std::move(edge_triplets),
             weight_property, std::move(vertex_preds), std::move(edge_preds)),
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
  sub_com_flat_ = std::make_unique<uint32_t[]>(array_size);
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
  for (size_t i = 0; i < array_size; ++i)
    sub_com_flat_[i] = kInvalidSubCom;
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
        // Vertices excluded by a vertex predicate don't participate; their
        // slots are never read by compute()/sink() (all loops iterate
        // valid_vertices only), so leave them untouched.
        if (!index_.is_valid(gid))
          continue;
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
          } else {
            community_[gid] = gid;
          }
        } else {
          community_[gid] = gid;
        }
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
void Leiden::compute() {
  const auto& valid_vertices = index_.valid_vertices();
  const size_t array_size = index_.array_size();
  // Parallel degree computation
  ParallelUtils::parallel_for(
      valid_vertices.data(), valid_vertices.size(),
      [&](vid_t gid, int /*tid*/) {
        double deg = 0;
        index_.for_each_neighbor(gid, [&](uint32_t, double w) { deg += w; });
        degree_[gid] = deg;
      },
      num_threads_);
  // Parallel m_ computation
  std::vector<double> local_m(num_threads_, 0.0);
  ParallelUtils::parallel_for(
      valid_vertices.data(), valid_vertices.size(),
      [&](vid_t gid, int tid) {
        index_.for_each_out_edge(
            gid, [&](uint32_t, double w) { local_m[tid] += w; });
      },
      num_threads_);
  m_ = 0;
  for (int i = 0; i < num_threads_; ++i)
    m_ += local_m[i];
  if (m_ == 0) {
    modularity_ = 0;
    return;
  }
  // Initialize stot_
  std::fill_n(stot_.get(), array_size, 0.0);
  for (uint32_t gid : valid_vertices)
    stot_[community_[gid]] += degree_[gid];
  double prev_mod = -1.0;
  for (int level = 0; level < 100; ++level) {
    bool improved = local_moving_phase();
    if (!improved)
      break;
    if (allow_relocation_ || !initial_community_)
      refine();
    // Rebuild stot_ after refine() which may split/rename communities.
    std::fill_n(stot_.get(), array_size, 0.0);
    for (uint32_t gid : valid_vertices)
      stot_[community_[gid]] += degree_[gid];
    // Compute modularity
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
  if (allow_relocation_ || !initial_community_) {
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
      std::fill_n(stot_.get(), array_size, 0.0);
      for (uint32_t gid : valid_vertices)
        stot_[community_[gid]] += degree_[gid];
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
bool Leiden::local_moving_phase() {
  const auto& valid_vertices = index_.valid_vertices();
  std::vector<uint32_t> order = valid_vertices;
  std::mt19937 rng(42);
  std::shuffle(order.begin(), order.end(), rng);
  bool improved = false;
  const size_t n = order.size();
  // Sequential Gauss-Seidel updates
  uint32_t* my_gen = thread_gen_.get();
  double* my_cw = thread_comm_weight_.get();
  uint32_t gen_val = 0;
  std::vector<uint32_t> touched;
  touched.reserve(256);
  for (int pass = 0; pass < 10; ++pass) {
    bool moved = false;
    for (size_t i = 0; i < n; ++i) {
      uint32_t u_gid = order[i];
      if (initial_community_ && !allow_relocation_ &&
          initial_community_[u_gid] != UINT32_MAX)
        continue;
      uint32_t cur_com = community_[u_gid];
      double deg_u = degree_[u_gid];
      ++gen_val;
      touched.clear();
      index_.for_each_neighbor(u_gid, [&](uint32_t v_gid, double w) {
        if (v_gid == u_gid)
          return;
        uint32_t com = community_[v_gid];
        if (my_gen[com] != gen_val) {
          my_gen[com] = gen_val;
          my_cw[com] = 0.0;
          touched.push_back(com);
        }
        my_cw[com] += w;
      });
      double w_self = (my_gen[cur_com] == gen_val) ? my_cw[cur_com] : 0.0;
      double stot_cur_minus_u = stot_[cur_com] - deg_u;
      uint32_t best = cur_com;
      double best_gain = 0.0;
      for (uint32_t com : touched) {
        if (com == cur_com)
          continue;
        double w_com = my_cw[com];
        double gain = (w_com - w_self) / m_ -
                      resolution_ * stot_[com] * deg_u / (2.0 * m_ * m_) +
                      resolution_ * stot_cur_minus_u * deg_u / (2.0 * m_ * m_);
        if (gain > best_gain) {
          best_gain = gain;
          best = com;
        }
      }
      if (best != cur_com) {
        stot_[cur_com] -= deg_u;
        stot_[best] += deg_u;
        community_[u_gid] = best;
        moved = true;
        improved = true;
      }
    }
    if (!moved)
      break;
  }
  return improved;
}
void Leiden::refine() {
  const auto& valid_vertices = index_.valid_vertices();
  const size_t array_size = index_.array_size();
  // Vertices excluded by a vertex predicate never appear in valid_vertices,
  // so they are absent from com_vertex_pairs and their sub_com_flat_ stays
  // kInvalidSubCom; the raw-view traversals below skip them via that check.
  std::vector<std::pair<uint32_t, uint32_t>> com_vertex_pairs;
  com_vertex_pairs.reserve(valid_vertices.size());
  for (uint32_t gid : valid_vertices)
    com_vertex_pairs.emplace_back(community_[gid], gid);
  std::sort(com_vertex_pairs.begin(), com_vertex_pairs.end());
  struct CommunityRange {
    size_t start;
    size_t end;
  };
  std::vector<CommunityRange> multi_comms;
  size_t i = 0, n = com_vertex_pairs.size();
  uint32_t next_com = 0;
  while (i < n) {
    uint32_t com_id = com_vertex_pairs[i].first;
    size_t j = i;
    while (j < n && com_vertex_pairs[j].first == com_id)
      ++j;
    if (j - i <= 1) {
      for (size_t k = i; k < j; ++k)
        community_[com_vertex_pairs[k].second] = next_com++;
    } else
      multi_comms.push_back({i, j});
    i = j;
  }
  std::atomic<uint32_t> atomic_next_com(next_com);
  if (multi_comms.empty())
    return;
  // Cap sub-community IDs to scratch array bounds (array_size per thread)
  const uint32_t max_sub_com = std::min<uint32_t>(50, array_size);
  std::atomic<size_t> cursor(0);
  std::vector<std::thread> threads;
  threads.reserve(num_threads_ - 1);
  if (index_.is_simple_graph()) {
    auto oe_view = index_.simple_out_view();
    auto ie_view = index_.simple_in_view();
    const bool has_weight = index_.has_weight();
    const auto& w_acc = index_.simple_weight_accessor();
    auto worker = [&](int tid) {
      uint32_t* r_gen =
          thread_gen_.get() + static_cast<size_t>(tid) * array_size;
      double* r_cw =
          thread_comm_weight_.get() + static_cast<size_t>(tid) * array_size;
      std::vector<uint32_t> touched_scs;
      touched_scs.reserve(64);
      std::fill_n(r_gen, std::min<size_t>(64, array_size), 0);
      uint32_t refine_gen = 0;
      while (true) {
        size_t idx = cursor.fetch_add(1);
        if (idx >= multi_comms.size())
          break;
        auto& range = multi_comms[idx];
        for (size_t k = range.start; k < range.end; ++k)
          sub_com_flat_[com_vertex_pairs[k].second] = 0;
        std::vector<uint32_t> nodes;
        nodes.reserve(range.end - range.start);
        for (size_t k = range.start; k < range.end; ++k)
          nodes.push_back(com_vertex_pairs[k].second);
        std::vector<uint32_t> order = nodes;
        std::mt19937 rng(42 + static_cast<uint32_t>(idx));
        std::shuffle(order.begin(), order.end(), rng);
        bool sub_improved = true;
        uint32_t next_sub = 1;
        while (sub_improved && next_sub < max_sub_com) {
          sub_improved = false;
          for (vid_t u : order) {
            uint32_t cur_sc = sub_com_flat_[u];
            ++refine_gen;
            touched_scs.clear();
            auto oes = oe_view.get_edges(u);
            for (auto it = oes.begin(); it != oes.end(); ++it) {
              vid_t v = *it;
              if (v == u || sub_com_flat_[v] == kInvalidSubCom)
                continue;
              if (!index_.edge_ok(0, u, v, it.get_data_ptr()))
                continue;
              uint32_t sc = sub_com_flat_[v];
              if (r_gen[sc] != refine_gen) {
                r_gen[sc] = refine_gen;
                r_cw[sc] = 0.0;
                touched_scs.push_back(sc);
              }
              r_cw[sc] += has_weight ? w_acc.get_typed_data<double>(it) : 1.0;
            }
            auto ies = ie_view.get_edges(u);
            for (auto it = ies.begin(); it != ies.end(); ++it) {
              vid_t v = *it;
              if (v == u || sub_com_flat_[v] == kInvalidSubCom)
                continue;
              if (!index_.edge_ok(0, v, u, it.get_data_ptr()))
                continue;
              uint32_t sc = sub_com_flat_[v];
              if (r_gen[sc] != refine_gen) {
                r_gen[sc] = refine_gen;
                r_cw[sc] = 0.0;
                touched_scs.push_back(sc);
              }
              r_cw[sc] += has_weight ? w_acc.get_typed_data<double>(it) : 1.0;
            }
            uint32_t best_sc = cur_sc;
            double best_w = (r_gen[cur_sc] == refine_gen) ? r_cw[cur_sc] : 0.0;
            for (uint32_t sc : touched_scs) {
              if (sc == cur_sc)
                continue;
              if (r_cw[sc] > best_w) {
                best_w = r_cw[sc];
                best_sc = sc;
              }
            }
            if (best_sc != cur_sc) {
              if (best_sc == next_sub)
                ++next_sub;
              sub_com_flat_[u] = best_sc;
              sub_improved = true;
            }
          }
        }
        // Assign new community IDs based on sub-communities
        std::unordered_map<uint32_t, uint32_t> sc_to_com;
        for (uint32_t u : nodes) {
          uint32_t sc = sub_com_flat_[u];
          if (sc_to_com.find(sc) == sc_to_com.end())
            sc_to_com[sc] = atomic_next_com.fetch_add(1);
          community_[u] = sc_to_com[sc];
        }
        for (uint32_t u : nodes)
          sub_com_flat_[u] = kInvalidSubCom;
      }
    };
    for (int tid = 1; tid < num_threads_; ++tid)
      threads.emplace_back(worker, tid);
    worker(0);
    for (auto& t : threads)
      t.join();
  } else {
    const auto& out_views = index_.out_views();
    const auto& in_views = index_.in_views();
    const auto& label_out_triplets = index_.label_out_triplets();
    const auto& label_in_triplets = index_.label_in_triplets();
    const auto& triplet_dst_base = index_.triplet_dst_base();
    const auto& triplet_src_base = index_.triplet_src_base();
    const auto& triplet_weight_accessors = index_.triplet_weight_accessors();
    const auto& triplet_has_weight = index_.triplet_has_weight();
    auto worker = [&](int tid) {
      uint32_t* r_gen =
          thread_gen_.get() + static_cast<size_t>(tid) * array_size;
      double* r_cw =
          thread_comm_weight_.get() + static_cast<size_t>(tid) * array_size;
      std::vector<uint32_t> touched_scs;
      touched_scs.reserve(64);
      std::fill_n(r_gen, std::min<size_t>(64, array_size), 0);
      uint32_t refine_gen = 0;
      while (true) {
        size_t idx = cursor.fetch_add(1);
        if (idx >= multi_comms.size())
          break;
        auto& range = multi_comms[idx];
        for (size_t k = range.start; k < range.end; ++k)
          sub_com_flat_[com_vertex_pairs[k].second] = 0;
        std::vector<uint32_t> nodes;
        nodes.reserve(range.end - range.start);
        for (size_t k = range.start; k < range.end; ++k)
          nodes.push_back(com_vertex_pairs[k].second);
        std::vector<uint32_t> order = nodes;
        std::mt19937 rng(42 + static_cast<uint32_t>(idx));
        std::shuffle(order.begin(), order.end(), rng);
        bool sub_improved = true;
        uint32_t next_sub = 1;
        while (sub_improved && next_sub < max_sub_com) {
          sub_improved = false;
          for (uint32_t u_gid : order) {
            uint32_t cur_sc = sub_com_flat_[u_gid];
            vid_t u_local = index_.local_vid(u_gid);
            size_t u_li = index_.label_idx(u_gid);
            ++refine_gen;
            touched_scs.clear();
            for (size_t ti : label_out_triplets[u_li]) {
              if (triplet_dst_base[ti] == SIZE_MAX)
                continue;
              size_t db = triplet_dst_base[ti];
              auto oes = out_views[ti].get_edges(u_local);
              for (auto it = oes.begin(); it != oes.end(); ++it) {
                uint32_t v_gid = static_cast<uint32_t>(db + (*it));
                if (v_gid == u_gid || sub_com_flat_[v_gid] == kInvalidSubCom)
                  continue;
                if (!index_.edge_ok(ti, u_local, *it, it.get_data_ptr()))
                  continue;
                uint32_t sc = sub_com_flat_[v_gid];
                if (r_gen[sc] != refine_gen) {
                  r_gen[sc] = refine_gen;
                  r_cw[sc] = 0.0;
                  touched_scs.push_back(sc);
                }
                r_cw[sc] +=
                    triplet_has_weight[ti]
                        ? triplet_weight_accessors[ti].get_typed_data<double>(
                              it)
                        : 1.0;
              }
            }
            for (size_t ti : label_in_triplets[u_li]) {
              if (triplet_src_base[ti] == SIZE_MAX)
                continue;
              size_t sb = triplet_src_base[ti];
              auto ies = in_views[ti].get_edges(u_local);
              for (auto it = ies.begin(); it != ies.end(); ++it) {
                uint32_t v_gid = static_cast<uint32_t>(sb + (*it));
                if (v_gid == u_gid || sub_com_flat_[v_gid] == kInvalidSubCom)
                  continue;
                if (!index_.edge_ok(ti, *it, u_local, it.get_data_ptr()))
                  continue;
                uint32_t sc = sub_com_flat_[v_gid];
                if (r_gen[sc] != refine_gen) {
                  r_gen[sc] = refine_gen;
                  r_cw[sc] = 0.0;
                  touched_scs.push_back(sc);
                }
                r_cw[sc] +=
                    triplet_has_weight[ti]
                        ? triplet_weight_accessors[ti].get_typed_data<double>(
                              it)
                        : 1.0;
              }
            }
            uint32_t best_sc = cur_sc;
            double best_w = (r_gen[cur_sc] == refine_gen) ? r_cw[cur_sc] : 0.0;
            for (uint32_t sc : touched_scs) {
              if (sc == cur_sc)
                continue;
              if (r_cw[sc] > best_w) {
                best_w = r_cw[sc];
                best_sc = sc;
              }
            }
            if (best_sc != cur_sc) {
              if (best_sc == next_sub)
                ++next_sub;
              sub_com_flat_[u_gid] = best_sc;
              sub_improved = true;
            }
          }
        }
        std::unordered_map<uint32_t, uint32_t> sc_to_com;
        for (uint32_t u : nodes) {
          uint32_t sc = sub_com_flat_[u];
          if (sc_to_com.find(sc) == sc_to_com.end())
            sc_to_com[sc] = atomic_next_com.fetch_add(1);
          community_[u] = sc_to_com[sc];
        }
        for (uint32_t u : nodes)
          sub_com_flat_[u] = kInvalidSubCom;
      }
    };
    for (int tid = 1; tid < num_threads_; ++tid)
      threads.emplace_back(worker, tid);
    worker(0);
    for (auto& t : threads)
      t.join();
  }
}
void Leiden::sink(execution::Context& ctx, int node_alias, int community_alias,
                  int previous_community_alias) {
  const auto& valid_vertices = index_.valid_vertices();
  std::unordered_map<uint32_t, uint32_t> com_remap;
  if (initial_community_) {
    std::unordered_map<uint32_t, std::unordered_map<uint32_t, uint32_t>>
        new_to_old_counts;
    uint32_t max_old_id = 0;
    bool has_valid_old = false;
    for (uint32_t gid : valid_vertices) {
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
    std::sort(com_sizes.begin(), com_sizes.end(),
              [](const auto& a, const auto& b) {
                if (a.second != b.second)
                  return a.second > b.second;
                return a.first < b.first;
              });
    std::unordered_set<uint32_t> used_ids;
    for (auto& [nc, _] : com_sizes) {
      auto& old_counts = new_to_old_counts[nc];
      uint32_t best_old = UINT32_MAX;
      uint32_t best_count = 0;
      for (auto& [oc, cnt] : old_counts) {
        if ((cnt > best_count ||
             (cnt == best_count && best_old != UINT32_MAX && oc < best_old)) &&
            used_ids.find(oc) == used_ids.end()) {
          best_count = cnt;
          best_old = oc;
        }
      }
      if (best_old != UINT32_MAX) {
        com_remap[nc] = best_old;
        used_ids.insert(best_old);
      }
    }
    uint32_t next_fresh = has_valid_old ? (max_old_id + 1) : 0;
    for (auto& [nc, _] : com_sizes) {
      if (com_remap.find(nc) == com_remap.end()) {
        while (used_ids.find(next_fresh) != used_ids.end())
          next_fresh++;
        com_remap[nc] = next_fresh;
        used_ids.insert(next_fresh);
        next_fresh++;
      }
    }
  } else {
    uint32_t next_id = 0;
    for (uint32_t gid : valid_vertices) {
      uint32_t c = community_[gid];
      if (com_remap.find(c) == com_remap.end())
        com_remap[c] = next_id++;
    }
  }
  bool need_prev = (previous_community_alias >= 0);
  const auto& vlabels = index_.vertex_labels();
  for (size_t li = 0; li < vlabels.size(); ++li) {
    label_t label = vlabels[li];
    size_t base = index_.label_base_offset(li);
    const auto& vertex_set = graph_.GetVertexSet(label);
    MSVertexColumnBuilder builder(label);
    ValueColumnBuilder<int64_t> community_builder;
    size_t count = vertex_set.size();
    builder.reserve(count);
    community_builder.reserve(count);
    std::shared_ptr<IContextColumn> prev_col;
    if (need_prev) {
      ValueColumnBuilder<int64_t> prev_builder(/*is_optional=*/true);
      prev_builder.reserve(count);
      for (const auto& v : vertex_set) {
        uint32_t gid = static_cast<uint32_t>(base + v);
        if (!index_.is_valid(gid))
          continue;
        if (initial_community_ && initial_community_[gid] != UINT32_MAX) {
          prev_builder.push_back_opt(
              static_cast<int64_t>(initial_community_[gid]));
        } else {
          prev_builder.push_back_null();
        }
      }
      prev_col = prev_builder.finish();
    }
    for (const auto& v : vertex_set) {
      uint32_t gid = static_cast<uint32_t>(base + v);
      if (!index_.is_valid(gid))
        continue;
      builder.push_back_opt(v);
      community_builder.push_back_opt(
          static_cast<int64_t>(com_remap[community_[gid]]));
    }
    execution::ContextChunk chunk;
    chunk.set(node_alias, builder.finish());
    chunk.set(community_alias, community_builder.finish());
    if (prev_col)
      chunk.set(previous_community_alias, prev_col);
    ctx.append_chunk(std::move(chunk));
  }
}
}  // namespace community
}  // namespace gds
}  // namespace neug
