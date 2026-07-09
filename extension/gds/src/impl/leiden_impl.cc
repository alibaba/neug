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
#include <numeric>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include "neug/common/columns/value_columns.h"
#include "neug/common/columns/vertex_columns.h"
#include "utils/parallel_utils.h"
namespace neug {
namespace gds {
namespace community {
Leiden::Leiden(const StorageReadInterface& graph,
               std::vector<label_t> vertex_labels,
               std::vector<LabelTriplet> edge_triplets, double resolution,
               double threshold, int concurrency,
               const std::string& initial_community_property)
    : graph_(graph),
      vertex_labels_(std::move(vertex_labels)),
      edge_triplets_(std::move(edge_triplets)),
      resolution_(resolution),
      threshold_(threshold),
      concurrency_(concurrency),
      initial_community_property_(initial_community_property) {
  for (size_t i = 0; i < vertex_labels_.size(); ++i)
    label_to_index_[vertex_labels_[i]] = i;
  label_base_offsets_.resize(vertex_labels_.size(), 0);
  label_local_sizes_.resize(vertex_labels_.size(), 0);
  size_t total_array_size = 0;
  for (size_t li = 0; li < vertex_labels_.size(); ++li) {
    label_base_offsets_[li] = total_array_size;
    const auto& vertex_set = graph_.GetVertexSet(vertex_labels_[li]);
    vid_t max_vid = 0;
    for (const auto& v : vertex_set) {
      if (v > max_vid)
        max_vid = v;
    }
    size_t local_size =
        (vertex_set.size() > 0) ? (static_cast<size_t>(max_vid) + 1) : 0;
    label_local_sizes_[li] = local_size;
    total_array_size += local_size;
  }
  array_size_ = total_array_size;
  global_to_label_.resize(array_size_, 0);
  global_to_vid_.resize(array_size_, 0);
  global_to_label_idx_.resize(array_size_, 0);
  for (size_t li = 0; li < vertex_labels_.size(); ++li) {
    const auto& vertex_set = graph_.GetVertexSet(vertex_labels_[li]);
    size_t base = label_base_offsets_[li];
    for (const auto& v : vertex_set) {
      uint32_t gid = static_cast<uint32_t>(base + v);
      valid_vertices_.push_back(gid);
      global_to_label_[gid] = vertex_labels_[li];
      global_to_vid_[gid] = v;
      global_to_label_idx_[gid] = li;
    }
  }
  vertex_count_ = valid_vertices_.size();
  community_ = std::make_unique<uint32_t[]>(array_size_);
  degree_ = std::make_unique<double[]>(array_size_);
  stot_ = std::make_unique<double[]>(array_size_);
  sub_com_flat_ = std::make_unique<uint32_t[]>(array_size_);
  num_threads_ = concurrency_ > 0
                     ? concurrency_
                     : static_cast<int>(std::thread::hardware_concurrency());
  if (num_threads_ < 1)
    num_threads_ = 1;
  size_t total_scratch = static_cast<size_t>(num_threads_) * array_size_;
  thread_comm_weight_ = std::make_unique<double[]>(total_scratch);
  thread_gen_ = std::make_unique<uint32_t[]>(total_scratch);
  std::fill_n(thread_comm_weight_.get(), total_scratch, 0.0);
  std::fill_n(thread_gen_.get(), total_scratch, 0);
  for (size_t i = 0; i < array_size_; ++i)
    sub_com_flat_[i] = kInvalidSubCom;
  if (!initial_community_property_.empty()) {
    initial_community_ = std::make_unique<uint32_t[]>(array_size_);
    std::fill_n(initial_community_.get(), array_size_, UINT32_MAX);
    for (size_t li = 0; li < vertex_labels_.size(); ++li) {
      label_t label = vertex_labels_[li];
      auto prop_col =
          graph_.GetVertexPropColumn(label, initial_community_property_);
      const auto& vertex_set = graph_.GetVertexSet(label);
      size_t base = label_base_offsets_[li];
      for (const auto& v : vertex_set) {
        uint32_t gid = static_cast<uint32_t>(base + v);
        if (prop_col) {
          auto val = prop_col->get_any(v);
          if (!val.IsNull()) {
            int64_t raw = val.GetValue<int64_t>();
            // Validate: community ID must be non-negative and within array
            // bounds
            if (raw >= 0 && static_cast<uint64_t>(raw) < array_size_) {
              uint32_t cval = static_cast<uint32_t>(raw);
              community_[gid] = cval;
              initial_community_[gid] = cval;
            } else {
              community_[gid] = gid;  // fallback to singleton
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
    for (uint32_t gid : valid_vertices_) {
      community_[gid] = gid;
      stot_[gid] = 0;
      degree_[gid] = 0;
    }
  }
  label_out_triplets_.resize(vertex_labels_.size());
  label_in_triplets_.resize(vertex_labels_.size());
  for (size_t ti = 0; ti < edge_triplets_.size(); ++ti) {
    const auto& triplet = edge_triplets_[ti];
    auto src_it = label_to_index_.find(triplet.src_label);
    auto dst_it = label_to_index_.find(triplet.dst_label);
    if (src_it != label_to_index_.end())
      label_out_triplets_[src_it->second].push_back(ti);
    if (dst_it != label_to_index_.end())
      label_in_triplets_[dst_it->second].push_back(ti);
  }
  // Pre-compute base offsets per triplet (avoids map lookup in hot loops)
  triplet_src_base_.resize(edge_triplets_.size(), SIZE_MAX);
  triplet_dst_base_.resize(edge_triplets_.size(), SIZE_MAX);
  for (size_t ti = 0; ti < edge_triplets_.size(); ++ti) {
    const auto& triplet = edge_triplets_[ti];
    auto src_it = label_to_index_.find(triplet.src_label);
    auto dst_it = label_to_index_.find(triplet.dst_label);
    if (src_it != label_to_index_.end())
      triplet_src_base_[ti] = label_base_offsets_[src_it->second];
    if (dst_it != label_to_index_.end())
      triplet_dst_base_[ti] = label_base_offsets_[dst_it->second];
  }
  // Detect simple graph for fast path (with defensive label consistency check)
  is_simple_graph_ =
      (vertex_labels_.size() == 1 && edge_triplets_.size() == 1 &&
       edge_triplets_[0].src_label == vertex_labels_[0] &&
       edge_triplets_[0].dst_label == vertex_labels_[0]);
  if (is_simple_graph_) {
    simple_vertex_label_ = vertex_labels_[0];
    simple_edge_label_ = edge_triplets_[0].edge_label;
  }
}
void Leiden::compute() {
  if (is_simple_graph_) {
    // Fast path: single label + single triplet - parallel preprocessing
    auto oe_view = graph_.GetGenericOutgoingGraphView(
        simple_vertex_label_, simple_vertex_label_, simple_edge_label_);
    auto ie_view = graph_.GetGenericIncomingGraphView(
        simple_vertex_label_, simple_vertex_label_, simple_edge_label_);
    ParallelUtils::parallel_for(
        valid_vertices_.data(), valid_vertices_.size(),
        [&](vid_t v, int /*tid*/) {
          double deg = 0;
          auto oes = oe_view.get_edges(v);
          for (auto it = oes.begin(); it != oes.end(); ++it)
            deg += 1.0;
          auto ies = ie_view.get_edges(v);
          for (auto it = ies.begin(); it != ies.end(); ++it)
            deg += 1.0;
          degree_[v] = deg;
        },
        num_threads_);
    std::vector<double> local_m(num_threads_, 0.0);
    ParallelUtils::parallel_for(
        valid_vertices_.data(), valid_vertices_.size(),
        [&](vid_t v, int tid) {
          auto oes = oe_view.get_edges(v);
          double cnt = 0;
          for (auto it = oes.begin(); it != oes.end(); ++it)
            cnt += 1.0;
          local_m[tid] += cnt;
        },
        num_threads_);
    m_ = 0;
    for (int i = 0; i < num_threads_; ++i)
      m_ += local_m[i];
    if (m_ == 0) {
      modularity_ = 0;
      return;
    }
    // Parallel stot_ init for non-warm-start; serial accumulate for warm-start
    if (!initial_community_) {
      ParallelUtils::parallel_for(
          valid_vertices_.data(), valid_vertices_.size(),
          [&](vid_t v, int /*tid*/) { stot_[v] = degree_[v]; }, num_threads_);
    } else {
      for (uint32_t gid : valid_vertices_)
        stot_[community_[gid]] += degree_[gid];
    }
    for (int level = 0; level < 100; ++level) {
      bool improved = local_moving_phase();
      if (!improved)
        break;
      refine();
      std::vector<double> local_mod(num_threads_, 0.0);
      ParallelUtils::parallel_for(
          valid_vertices_.data(), valid_vertices_.size(),
          [&](vid_t v, int tid) {
            auto oes = oe_view.get_edges(v);
            for (auto it = oes.begin(); it != oes.end(); ++it) {
              vid_t u = *it;
              if (community_[v] == community_[u])
                local_mod[tid] += 1.0 / (2.0 * m_) -
                                  degree_[v] * degree_[u] / (4.0 * m_ * m_);
            }
          },
          num_threads_);
      double new_mod = 0;
      for (int i = 0; i < num_threads_; ++i)
        new_mod += local_mod[i];
      if (std::abs(new_mod - modularity_) < threshold_)
        break;
      modularity_ = new_mod;
    }
  } else {
    // Generic path: multi-label / multi-triplet
    // Pre-fetch all views once (avoids repeated construction in hot loops)
    std::vector<CsrView> out_views(edge_triplets_.size()),
        in_views(edge_triplets_.size());
    for (size_t ti = 0; ti < edge_triplets_.size(); ++ti) {
      const auto& t = edge_triplets_[ti];
      out_views[ti] = graph_.GetGenericOutgoingGraphView(
          t.src_label, t.dst_label, t.edge_label);
      in_views[ti] = graph_.GetGenericIncomingGraphView(
          t.dst_label, t.src_label, t.edge_label);
    }
    for (uint32_t gid : valid_vertices_) {
      size_t li = global_to_label_idx_[gid];
      vid_t local_vid = global_to_vid_[gid];
      double deg = 0;
      for (size_t ti : label_out_triplets_[li]) {
        auto oes = out_views[ti].get_edges(local_vid);
        for (auto it = oes.begin(); it != oes.end(); ++it)
          deg += 1.0;
      }
      for (size_t ti : label_in_triplets_[li]) {
        auto ies = in_views[ti].get_edges(local_vid);
        for (auto it = ies.begin(); it != ies.end(); ++it)
          deg += 1.0;
      }
      degree_[gid] = deg;
    }
    m_ = 0;
    for (uint32_t gid : valid_vertices_) {
      size_t li = global_to_label_idx_[gid];
      vid_t lv = global_to_vid_[gid];
      for (size_t ti : label_out_triplets_[li]) {
        auto oes = out_views[ti].get_edges(lv);
        for (auto it = oes.begin(); it != oes.end(); ++it)
          m_ += 1.0;
      }
    }
    if (m_ == 0) {
      modularity_ = 0;
      return;
    }
    for (uint32_t gid : valid_vertices_)
      stot_[community_[gid]] += degree_[gid];
    for (int level = 0; level < 100; ++level) {
      bool improved = local_moving_phase();
      if (!improved)
        break;
      refine();
      double new_mod = 0;
      for (uint32_t gid : valid_vertices_) {
        size_t li = global_to_label_idx_[gid];
        vid_t lv = global_to_vid_[gid];
        for (size_t ti : label_out_triplets_[li]) {
          if (triplet_dst_base_[ti] == SIZE_MAX)
            continue;
          size_t dst_base = triplet_dst_base_[ti];
          auto oes = out_views[ti].get_edges(lv);
          for (auto it = oes.begin(); it != oes.end(); ++it) {
            uint32_t u_gid = static_cast<uint32_t>(dst_base + (*it));
            if (community_[gid] == community_[u_gid])
              new_mod += 1.0 / (2.0 * m_) -
                         degree_[gid] * degree_[u_gid] / (4.0 * m_ * m_);
          }
        }
      }
      if (std::abs(new_mod - modularity_) < threshold_)
        break;
      modularity_ = new_mod;
    }
  }
}
bool Leiden::local_moving_phase() {
  std::vector<uint32_t> order = valid_vertices_;
  std::mt19937 rng(42);
  std::shuffle(order.begin(), order.end(), rng);
  bool improved = false;
  const size_t n = order.size(), chunk = 4096;
  const size_t num_batches = (n + chunk - 1) / chunk;
  const int nt = num_threads_;
  std::vector<uint32_t> best_com(n);
  std::vector<std::vector<uint32_t>> touched(nt);
  for (int t = 0; t < nt; ++t)
    touched[t].reserve(256);
  if (is_simple_graph_) {
    auto oe_view = graph_.GetGenericOutgoingGraphView(
        simple_vertex_label_, simple_vertex_label_, simple_edge_label_);
    auto ie_view = graph_.GetGenericIncomingGraphView(
        simple_vertex_label_, simple_vertex_label_, simple_edge_label_);
    for (int pass = 0; pass < 10; ++pass) {
      bool moved = false;
      for (size_t batch = 0; batch < num_batches; ++batch) {
        size_t batch_start = batch * chunk,
               batch_end = std::min(batch_start + chunk, n);
        {
          std::atomic<size_t> cursor(batch_start);
          std::vector<std::thread> threads;
          threads.reserve(nt - 1);
          auto worker = [&](int tid) {
            uint32_t* my_gen =
                thread_gen_.get() + static_cast<size_t>(tid) * array_size_;
            double* my_cw = thread_comm_weight_.get() +
                            static_cast<size_t>(tid) * array_size_;
            uint32_t gen_val = 0;
            auto& my_touched = touched[tid];
            while (true) {
              size_t start = cursor.fetch_add(64);
              if (start >= batch_end)
                break;
              size_t end = std::min(start + size_t(64), batch_end);
              for (size_t i = start; i < end; ++i) {
                vid_t u = order[i];
                uint32_t cur_com = community_[u];
                double deg_u = degree_[u];
                ++gen_val;
                my_touched.clear();
                auto process_nbr = [&](vid_t v) {
                  if (v == u)
                    return;
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
                double stot_cur_minus_u = stot_[cur_com] - deg_u;
                uint32_t best = cur_com;
                double best_gain = 0.0;
                for (uint32_t com : my_touched) {
                  if (com == cur_com)
                    continue;
                  double w_com = my_cw[com];
                  double gain =
                      (w_com - w_self) / m_ -
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
          for (auto& th : threads)
            th.join();
        }
        for (size_t i = batch_start; i < batch_end; ++i) {
          vid_t u = order[i];
          uint32_t cur_com = community_[u], new_com = best_com[i];
          if (new_com != cur_com) {
            stot_[cur_com] -= degree_[u];
            stot_[new_com] += degree_[u];
            community_[u] = new_com;
            moved = true;
            improved = true;
          }
        }
      }
      if (!moved)
        break;
    }
  } else {
    // Pre-fetch all views once (shared by all threads, read-only)
    std::vector<CsrView> out_views(edge_triplets_.size()),
        in_views(edge_triplets_.size());
    for (size_t ti = 0; ti < edge_triplets_.size(); ++ti) {
      const auto& t = edge_triplets_[ti];
      out_views[ti] = graph_.GetGenericOutgoingGraphView(
          t.src_label, t.dst_label, t.edge_label);
      in_views[ti] = graph_.GetGenericIncomingGraphView(
          t.dst_label, t.src_label, t.edge_label);
    }
    for (int pass = 0; pass < 10; ++pass) {
      bool moved = false;
      for (size_t batch = 0; batch < num_batches; ++batch) {
        size_t batch_start = batch * chunk,
               batch_end = std::min(batch_start + chunk, n);
        {
          std::atomic<size_t> cursor(batch_start);
          std::vector<std::thread> threads;
          threads.reserve(nt - 1);
          auto worker = [&](int tid) {
            uint32_t* my_gen =
                thread_gen_.get() + static_cast<size_t>(tid) * array_size_;
            double* my_cw = thread_comm_weight_.get() +
                            static_cast<size_t>(tid) * array_size_;
            uint32_t gen_val = 0;
            auto& my_touched = touched[tid];
            while (true) {
              size_t start = cursor.fetch_add(64);
              if (start >= batch_end)
                break;
              size_t end = std::min(start + size_t(64), batch_end);
              for (size_t i = start; i < end; ++i) {
                uint32_t u_gid = order[i];
                uint32_t cur_com = community_[u_gid];
                double deg_u = degree_[u_gid];
                vid_t u_local = global_to_vid_[u_gid];
                size_t u_li = global_to_label_idx_[u_gid];
                ++gen_val;
                my_touched.clear();
                auto process_nbr = [&](uint32_t v_gid) {
                  if (v_gid == u_gid)
                    return;
                  uint32_t com = community_[v_gid];
                  if (my_gen[com] != gen_val) {
                    my_gen[com] = gen_val;
                    my_cw[com] = 0.0;
                    my_touched.push_back(com);
                  }
                  my_cw[com] += 1.0;
                };
                for (size_t ti : label_out_triplets_[u_li]) {
                  if (triplet_dst_base_[ti] == SIZE_MAX)
                    continue;
                  size_t dst_base = triplet_dst_base_[ti];
                  auto oes = out_views[ti].get_edges(u_local);
                  for (auto it = oes.begin(); it != oes.end(); ++it)
                    process_nbr(static_cast<uint32_t>(dst_base + (*it)));
                }
                for (size_t ti : label_in_triplets_[u_li]) {
                  if (triplet_src_base_[ti] == SIZE_MAX)
                    continue;
                  size_t src_base = triplet_src_base_[ti];
                  auto ies = in_views[ti].get_edges(u_local);
                  for (auto it = ies.begin(); it != ies.end(); ++it)
                    process_nbr(static_cast<uint32_t>(src_base + (*it)));
                }
                double w_self =
                    (my_gen[cur_com] == gen_val) ? my_cw[cur_com] : 0.0;
                double stot_cur_minus_u = stot_[cur_com] - deg_u;
                uint32_t best = cur_com;
                double best_gain = 0.0;
                for (uint32_t com : my_touched) {
                  if (com == cur_com)
                    continue;
                  double w_com = my_cw[com];
                  double gain =
                      (w_com - w_self) / m_ -
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
          for (auto& th : threads)
            th.join();
        }
        for (size_t i = batch_start; i < batch_end; ++i) {
          uint32_t u_gid = order[i], cur_com = community_[u_gid],
                   new_com = best_com[i];
          if (new_com != cur_com) {
            stot_[cur_com] -= degree_[u_gid];
            stot_[new_com] += degree_[u_gid];
            community_[u_gid] = new_com;
            moved = true;
            improved = true;
          }
        }
      }
      if (!moved)
        break;
    }
  }
  return improved;
}
void Leiden::refine() {
  std::vector<std::pair<uint32_t, uint32_t>> com_vertex_pairs;
  com_vertex_pairs.reserve(valid_vertices_.size());
  for (uint32_t gid : valid_vertices_)
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
  std::atomic<size_t> cursor(0);
  std::vector<std::thread> threads;
  threads.reserve(num_threads_ - 1);
  if (is_simple_graph_) {
    auto oe_view = graph_.GetGenericOutgoingGraphView(
        simple_vertex_label_, simple_vertex_label_, simple_edge_label_);
    auto worker = [&](int tid) {
      uint32_t* r_gen =
          thread_gen_.get() + static_cast<size_t>(tid) * array_size_;
      double* r_cw =
          thread_comm_weight_.get() + static_cast<size_t>(tid) * array_size_;
      std::vector<uint32_t> touched_scs;
      touched_scs.reserve(64);
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
        uint32_t next_sub = 1, refine_gen = 0;
        while (sub_improved && next_sub < 50) {
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
              uint32_t sc = sub_com_flat_[v];
              if (r_gen[sc] != refine_gen) {
                r_gen[sc] = refine_gen;
                r_cw[sc] = 0.0;
                touched_scs.push_back(sc);
              }
              r_cw[sc] += 1.0;
            }
            double w_self = (r_gen[cur_sc] == refine_gen) ? r_cw[cur_sc] : 0.0;
            uint32_t best_sc = cur_sc;
            double best_gain = 0.0;
            for (uint32_t sc : touched_scs) {
              double gain = r_cw[sc] - w_self;
              if (gain > best_gain) {
                best_gain = gain;
                best_sc = sc;
              }
            }
            if (-w_self > best_gain) {
              best_gain = -w_self;
              best_sc = next_sub;
            }
            if (best_sc != cur_sc) {
              sub_com_flat_[u] = best_sc;
              if (best_sc == next_sub)
                next_sub++;
              sub_improved = true;
            }
          }
        }
        // Use unordered_map instead of fixed-size stack array to prevent
        // overflow
        std::unordered_map<uint32_t, uint32_t> sc_to_new;
        for (uint32_t gid : nodes)
          sc_to_new[sub_com_flat_[gid]] = UINT32_MAX;
        for (uint32_t gid : nodes) {
          uint32_t sc = sub_com_flat_[gid];
          if (sc_to_new[sc] == UINT32_MAX)
            sc_to_new[sc] = atomic_next_com.fetch_add(1);
          community_[gid] = sc_to_new[sc];
        }
        for (uint32_t gid : nodes)
          sub_com_flat_[gid] = kInvalidSubCom;
      }
    };
    for (int t = 1; t < num_threads_; ++t)
      threads.emplace_back(worker, t);
    worker(0);
    for (auto& th : threads)
      th.join();
  } else {
    // Pre-fetch all views once (shared by all threads, read-only)
    std::vector<CsrView> out_views(edge_triplets_.size()),
        in_views(edge_triplets_.size());
    for (size_t ti = 0; ti < edge_triplets_.size(); ++ti) {
      const auto& t = edge_triplets_[ti];
      out_views[ti] = graph_.GetGenericOutgoingGraphView(
          t.src_label, t.dst_label, t.edge_label);
      in_views[ti] = graph_.GetGenericIncomingGraphView(
          t.dst_label, t.src_label, t.edge_label);
    }
    auto worker = [&](int tid) {
      uint32_t* r_gen =
          thread_gen_.get() + static_cast<size_t>(tid) * array_size_;
      double* r_cw =
          thread_comm_weight_.get() + static_cast<size_t>(tid) * array_size_;
      std::vector<uint32_t> touched_scs;
      touched_scs.reserve(64);
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
        uint32_t next_sub = 1, refine_gen = 0;
        while (sub_improved && next_sub < 50) {
          sub_improved = false;
          for (uint32_t u_gid : order) {
            uint32_t cur_sc = sub_com_flat_[u_gid];
            size_t u_li = global_to_label_idx_[u_gid];
            vid_t u_local = global_to_vid_[u_gid];
            ++refine_gen;
            touched_scs.clear();
            for (size_t ti : label_out_triplets_[u_li]) {
              if (triplet_dst_base_[ti] == SIZE_MAX)
                continue;
              size_t dst_base = triplet_dst_base_[ti];
              auto oes = out_views[ti].get_edges(u_local);
              for (auto it = oes.begin(); it != oes.end(); ++it) {
                uint32_t v_gid = static_cast<uint32_t>(dst_base + (*it));
                if (v_gid == u_gid || sub_com_flat_[v_gid] == kInvalidSubCom)
                  continue;
                uint32_t sc = sub_com_flat_[v_gid];
                if (r_gen[sc] != refine_gen) {
                  r_gen[sc] = refine_gen;
                  r_cw[sc] = 0.0;
                  touched_scs.push_back(sc);
                }
                r_cw[sc] += 1.0;
              }
            }
            double w_self = (r_gen[cur_sc] == refine_gen) ? r_cw[cur_sc] : 0.0;
            uint32_t best_sc = cur_sc;
            double best_gain = 0.0;
            for (uint32_t sc : touched_scs) {
              double gain = r_cw[sc] - w_self;
              if (gain > best_gain) {
                best_gain = gain;
                best_sc = sc;
              }
            }
            if (-w_self > best_gain) {
              best_gain = -w_self;
              best_sc = next_sub;
            }
            if (best_sc != cur_sc) {
              sub_com_flat_[u_gid] = best_sc;
              if (best_sc == next_sub)
                next_sub++;
              sub_improved = true;
            }
          }
        }
        // Use unordered_map instead of fixed-size stack array to prevent
        // overflow
        std::unordered_map<uint32_t, uint32_t> sc_to_new;
        for (uint32_t gid : nodes)
          sc_to_new[sub_com_flat_[gid]] = UINT32_MAX;
        for (uint32_t gid : nodes) {
          uint32_t sc = sub_com_flat_[gid];
          if (sc_to_new[sc] == UINT32_MAX)
            sc_to_new[sc] = atomic_next_com.fetch_add(1);
          community_[gid] = sc_to_new[sc];
        }
        for (uint32_t gid : nodes)
          sub_com_flat_[gid] = kInvalidSubCom;
      }
    };
    for (int t = 1; t < num_threads_; ++t)
      threads.emplace_back(worker, t);
    worker(0);
    for (auto& th : threads)
      th.join();
  }
}
void Leiden::sink(execution::Context& ctx, int node_alias,
                  int community_alias) {
  std::unordered_map<uint32_t, uint32_t> com_remap;
  if (initial_community_) {
    // Stable ID: inherit old community IDs via majority vote
    // Step 1: collect members per new community, count votes from old
    // communities
    std::unordered_map<uint32_t, std::unordered_map<uint32_t, uint32_t>>
        new_to_old_counts;
    uint32_t max_old_id = 0;
    bool has_valid_old = false;
    for (uint32_t gid : valid_vertices_) {
      uint32_t new_com = community_[gid];
      uint32_t old_com = initial_community_[gid];
      if (old_com != UINT32_MAX) {
        new_to_old_counts[new_com][old_com]++;
        if (!has_valid_old || old_com > max_old_id) {
          max_old_id = old_com;
          has_valid_old = true;
        }
      } else {
        new_to_old_counts[new_com];  // ensure entry exists
      }
    }
    // Step 2: for each new community, find best matching old ID (majority vote)
    // Sort by community size descending so larger communities get priority
    std::vector<std::pair<uint32_t, uint32_t>> com_sizes;  // (new_com, size)
    for (auto& [nc, old_counts] : new_to_old_counts) {
      uint32_t total = 0;
      for (auto& [_, cnt] : old_counts)
        total += cnt;
      com_sizes.push_back({nc, total});
    }
    std::sort(com_sizes.begin(), com_sizes.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });
    std::unordered_set<uint32_t> used_ids;
    for (auto& [nc, _] : com_sizes) {
      auto& old_counts = new_to_old_counts[nc];
      uint32_t best_old = UINT32_MAX;
      uint32_t best_count = 0;
      for (auto& [oc, cnt] : old_counts) {
        if (cnt > best_count && used_ids.find(oc) == used_ids.end()) {
          best_count = cnt;
          best_old = oc;
        }
      }
      if (best_old != UINT32_MAX) {
        com_remap[nc] = best_old;
        used_ids.insert(best_old);
      }
    }
    // Step 3: assign fresh IDs for unmatched new communities
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
    // No initial community: sequential 0-based IDs
    uint32_t next_id = 0;
    for (uint32_t gid : valid_vertices_) {
      uint32_t c = community_[gid];
      if (com_remap.find(c) == com_remap.end())
        com_remap[c] = next_id++;
    }
  }
  for (size_t li = 0; li < vertex_labels_.size(); ++li) {
    label_t label = vertex_labels_[li];
    size_t base = label_base_offsets_[li];
    const auto& vertex_set = graph_.GetVertexSet(label);
    MSVertexColumnBuilder builder(label);
    ValueColumnBuilder<int64_t> community_builder;
    size_t count = 0;
    for (const auto& v : vertex_set) {
      (void) v;
      count++;
    }
    builder.reserve(count);
    community_builder.reserve(count);
    for (const auto& v : vertex_set) {
      uint32_t gid = static_cast<uint32_t>(base + v);
      builder.push_back_opt(v);
      community_builder.push_back_opt(
          static_cast<int64_t>(com_remap[community_[gid]]));
    }
    execution::ContextChunk chunk;
    chunk.set(node_alias, builder.finish());
    chunk.set(community_alias, community_builder.finish());
    ctx.append_chunk(std::move(chunk));
  }
}
}  // namespace community
}  // namespace gds
}  // namespace neug
