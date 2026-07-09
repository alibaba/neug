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
#include "utils/parallel_utils.h"
namespace neug { namespace gds { namespace community {
Louvain::Louvain(
    const StorageReadInterface& graph, std::vector<label_t> vertex_labels,
    std::vector<LabelTriplet> edge_triplets, double resolution,
    double threshold, int concurrency, const std::string& initial_community_property)
    : graph_(graph), vertex_labels_(std::move(vertex_labels)),
      edge_triplets_(std::move(edge_triplets)), resolution_(resolution),
      threshold_(threshold), concurrency_(concurrency),
      initial_community_property_(initial_community_property) {
  for (size_t i = 0; i < vertex_labels_.size(); ++i) label_to_index_[vertex_labels_[i]] = i;
  label_base_offsets_.resize(vertex_labels_.size(), 0);
  label_local_sizes_.resize(vertex_labels_.size(), 0);
  size_t total_array_size = 0;
  for (size_t li = 0; li < vertex_labels_.size(); ++li) {
    label_base_offsets_[li] = total_array_size;
    const auto& vs = graph_.GetVertexSet(vertex_labels_[li]);
    vid_t max_vid = 0; for (const auto& v : vs) { if (v > max_vid) max_vid = v; }
    label_local_sizes_[li] = (vs.size() > 0) ? (static_cast<size_t>(max_vid) + 1) : 0;
    total_array_size += label_local_sizes_[li];
  }
  array_size_ = total_array_size;
  global_to_label_.resize(array_size_, 0); global_to_vid_.resize(array_size_, 0);
  global_to_label_idx_.resize(array_size_, 0);
  for (size_t li = 0; li < vertex_labels_.size(); ++li) {
    const auto& vs = graph_.GetVertexSet(vertex_labels_[li]); size_t base = label_base_offsets_[li];
    for (const auto& v : vs) { uint32_t gid = static_cast<uint32_t>(base+v); valid_vertices_.push_back(gid); global_to_label_[gid] = vertex_labels_[li]; global_to_vid_[gid] = v; global_to_label_idx_[gid] = li; }
  }
  vertex_count_ = valid_vertices_.size();
  community_ = std::make_unique<uint32_t[]>(array_size_);
  degree_ = std::make_unique<double[]>(array_size_);
  stot_ = std::make_unique<double[]>(array_size_);
  num_threads_ = concurrency_ > 0 ? concurrency_ : static_cast<int>(std::thread::hardware_concurrency());
  if (num_threads_ < 1) num_threads_ = 1;
  size_t total_scratch = static_cast<size_t>(num_threads_) * array_size_;
  thread_comm_weight_ = std::make_unique<double[]>(total_scratch);
  thread_gen_ = std::make_unique<uint32_t[]>(total_scratch);
  std::fill_n(thread_comm_weight_.get(), total_scratch, 0.0);
  std::fill_n(thread_gen_.get(), total_scratch, 0);
  if (!initial_community_property_.empty()) {
    initial_community_ = std::make_unique<uint32_t[]>(array_size_);
    std::fill_n(initial_community_.get(), array_size_, UINT32_MAX);
    for (size_t li = 0; li < vertex_labels_.size(); ++li) {
      label_t label = vertex_labels_[li];
      auto prop_col = graph_.GetVertexPropColumn(label, initial_community_property_);
      const auto& vs = graph_.GetVertexSet(label); size_t base = label_base_offsets_[li];
      for (const auto& v : vs) { uint32_t gid = static_cast<uint32_t>(base+v);
        if (prop_col) { auto val = prop_col->get_any(v);
          if (!val.IsNull()) {
            int64_t raw = val.GetValue<int64_t>();
            // Validate: community ID must be non-negative and within array bounds
            if (raw >= 0 && static_cast<uint64_t>(raw) < array_size_) {
              uint32_t cval = static_cast<uint32_t>(raw);
              community_[gid] = cval; initial_community_[gid] = cval;
            } else { community_[gid] = gid; }
          }
          else community_[gid] = gid;
        } else community_[gid] = gid;
        stot_[gid] = 0; degree_[gid] = 0; }
    }
  } else { for (uint32_t gid : valid_vertices_) { community_[gid] = gid; stot_[gid] = 0; degree_[gid] = 0; } }
  label_out_triplets_.resize(vertex_labels_.size()); label_in_triplets_.resize(vertex_labels_.size());
  for (size_t ti = 0; ti < edge_triplets_.size(); ++ti) {
    auto si = label_to_index_.find(edge_triplets_[ti].src_label); auto di = label_to_index_.find(edge_triplets_[ti].dst_label);
    if (si != label_to_index_.end()) label_out_triplets_[si->second].push_back(ti);
    if (di != label_to_index_.end()) label_in_triplets_[di->second].push_back(ti);
  }
  // Pre-compute base offsets per triplet (avoids map lookup in hot loops)
  triplet_src_base_.resize(edge_triplets_.size(), SIZE_MAX);
  triplet_dst_base_.resize(edge_triplets_.size(), SIZE_MAX);
  for (size_t ti = 0; ti < edge_triplets_.size(); ++ti) {
    const auto& t = edge_triplets_[ti];
    auto si = label_to_index_.find(t.src_label); auto di = label_to_index_.find(t.dst_label);
    if (si != label_to_index_.end()) triplet_src_base_[ti] = label_base_offsets_[si->second];
    if (di != label_to_index_.end()) triplet_dst_base_[ti] = label_base_offsets_[di->second];
  }
  // Detect simple graph for fast path (with defensive label consistency check)
  is_simple_graph_ = (vertex_labels_.size() == 1 && edge_triplets_.size() == 1
      && edge_triplets_[0].src_label == vertex_labels_[0]
      && edge_triplets_[0].dst_label == vertex_labels_[0]);
  if (is_simple_graph_) {
    simple_vertex_label_ = vertex_labels_[0];
    simple_edge_label_ = edge_triplets_[0].edge_label;
  }
}
void Louvain::compute() {
  if (is_simple_graph_) {
    // Fast path: parallel preprocessing
    auto oe_view = graph_.GetGenericOutgoingGraphView(simple_vertex_label_, simple_vertex_label_, simple_edge_label_);
    auto ie_view = graph_.GetGenericIncomingGraphView(simple_vertex_label_, simple_vertex_label_, simple_edge_label_);
    ParallelUtils::parallel_for(
        valid_vertices_.data(), valid_vertices_.size(),
        [&](vid_t v, int /*tid*/) {
          double deg = 0;
          auto oes = oe_view.get_edges(v); for (auto it = oes.begin(); it != oes.end(); ++it) deg += 1.0;
          auto ies = ie_view.get_edges(v); for (auto it = ies.begin(); it != ies.end(); ++it) deg += 1.0;
          degree_[v] = deg;
        }, num_threads_);
    std::vector<double> local_m(num_threads_, 0.0);
    ParallelUtils::parallel_for(
        valid_vertices_.data(), valid_vertices_.size(),
        [&](vid_t v, int tid) {
          auto oes = oe_view.get_edges(v); double cnt = 0;
          for (auto it = oes.begin(); it != oes.end(); ++it) cnt += 1.0;
          local_m[tid] += cnt;
        }, num_threads_);
    m_ = 0;
    for (int i = 0; i < num_threads_; ++i) m_ += local_m[i];
    if (m_ == 0) { modularity_ = 0; return; }
    // Parallel stot_ init for non-warm-start; serial accumulate for warm-start
    if (!initial_community_) {
      ParallelUtils::parallel_for(
          valid_vertices_.data(), valid_vertices_.size(),
          [&](vid_t v, int /*tid*/) { stot_[v] = degree_[v]; }, num_threads_);
    } else {
      for (uint32_t gid : valid_vertices_) stot_[community_[gid]] += degree_[gid];
    }
    double prev_mod = -1.0;
    for (int level = 0; level < 100; ++level) {
      bool improved = one_level(); if (!improved) break;
      std::vector<double> local_mod(num_threads_, 0.0);
      ParallelUtils::parallel_for(
          valid_vertices_.data(), valid_vertices_.size(),
          [&](vid_t v, int tid) {
            auto oes = oe_view.get_edges(v);
            for (auto it = oes.begin(); it != oes.end(); ++it) {
              vid_t u = *it;
              if (community_[v] == community_[u])
                local_mod[tid] += 1.0/(2.0*m_) - degree_[v]*degree_[u]/(4.0*m_*m_);
            }
          }, num_threads_);
      double new_mod = 0;
      for (int i = 0; i < num_threads_; ++i) new_mod += local_mod[i];
      modularity_ = new_mod;
      if (prev_mod >= 0 && std::abs(modularity_ - prev_mod) < threshold_) break;
      prev_mod = modularity_;
    }
  } else {
    // Generic path
    // Pre-fetch all views once (avoids repeated construction in hot loops)
    std::vector<CsrView> out_views(edge_triplets_.size()), in_views(edge_triplets_.size());
    for (size_t ti = 0; ti < edge_triplets_.size(); ++ti) {
      const auto& t = edge_triplets_[ti];
      out_views[ti] = graph_.GetGenericOutgoingGraphView(t.src_label, t.dst_label, t.edge_label);
      in_views[ti] = graph_.GetGenericIncomingGraphView(t.dst_label, t.src_label, t.edge_label);
    }
    for (uint32_t gid : valid_vertices_) { size_t li = global_to_label_idx_[gid]; vid_t lv = global_to_vid_[gid]; double deg = 0;
      for (size_t ti : label_out_triplets_[li]) { auto e = out_views[ti].get_edges(lv); for (auto it = e.begin(); it != e.end(); ++it) deg += 1.0; }
      for (size_t ti : label_in_triplets_[li]) { auto e = in_views[ti].get_edges(lv); for (auto it = e.begin(); it != e.end(); ++it) deg += 1.0; }
      degree_[gid] = deg; }
    m_ = 0;
    for (uint32_t gid : valid_vertices_) { size_t li = global_to_label_idx_[gid]; vid_t lv = global_to_vid_[gid];
      for (size_t ti : label_out_triplets_[li]) { auto e = out_views[ti].get_edges(lv); for (auto it = e.begin(); it != e.end(); ++it) m_ += 1.0; } }
    if (m_ == 0) { modularity_ = 0; return; }
    for (uint32_t gid : valid_vertices_) stot_[community_[gid]] += degree_[gid];
    double prev_mod = -1.0;
    for (int level = 0; level < 100; ++level) { bool improved = one_level(); if (!improved) break;
      double new_mod = 0;
      for (uint32_t gid : valid_vertices_) { size_t li = global_to_label_idx_[gid]; vid_t lv = global_to_vid_[gid];
        for (size_t ti : label_out_triplets_[li]) {
          if (triplet_dst_base_[ti] == SIZE_MAX) continue;
          size_t db = triplet_dst_base_[ti];
          auto oes = out_views[ti].get_edges(lv); for (auto it = oes.begin(); it != oes.end(); ++it) { uint32_t ug = static_cast<uint32_t>(db+(*it)); if (community_[gid] == community_[ug]) new_mod += 1.0/(2.0*m_) - degree_[gid]*degree_[ug]/(4.0*m_*m_); } } }
      modularity_ = new_mod; if (prev_mod >= 0 && std::abs(modularity_ - prev_mod) < threshold_) break; prev_mod = modularity_; }
  }
}
bool Louvain::one_level() {
  std::vector<uint32_t> order = valid_vertices_; std::mt19937 rng(42); std::shuffle(order.begin(), order.end(), rng);
  bool improved = false; const size_t n = order.size(), chunk = 4096, num_batches = (n+chunk-1)/chunk; const int nt = num_threads_;
  std::vector<uint32_t> best_com(n); std::vector<std::vector<uint32_t>> touched(nt); for (int t = 0; t < nt; ++t) touched[t].reserve(256);
  if (is_simple_graph_) {
    auto oe_view = graph_.GetGenericOutgoingGraphView(simple_vertex_label_, simple_vertex_label_, simple_edge_label_);
    auto ie_view = graph_.GetGenericIncomingGraphView(simple_vertex_label_, simple_vertex_label_, simple_edge_label_);
    for (int pass = 0; pass < 10; ++pass) { bool moved = false;
      for (size_t batch = 0; batch < num_batches; ++batch) { size_t bs = batch*chunk, be = std::min(bs+chunk, n);
        { std::atomic<size_t> cursor(bs); std::vector<std::thread> threads; threads.reserve(nt-1);
          auto worker = [&](int tid) { uint32_t* mg = thread_gen_.get()+static_cast<size_t>(tid)*array_size_; double* mc = thread_comm_weight_.get()+static_cast<size_t>(tid)*array_size_; uint32_t gv = 0; auto& mt = touched[tid];
            while (true) { size_t s = cursor.fetch_add(64); if (s >= be) break; size_t e = std::min(s+size_t(64), be);
              for (size_t i = s; i < e; ++i) { vid_t u = order[i]; uint32_t cc = community_[u]; double du = degree_[u]; ++gv; mt.clear();
                auto pn = [&](vid_t v) { if (v == u) return; uint32_t cm = community_[v]; if (mg[cm] != gv) { mg[cm] = gv; mc[cm] = 0.0; mt.push_back(cm); } mc[cm] += 1.0; };
                auto oes = oe_view.get_edges(u); for (auto it = oes.begin(); it != oes.end(); ++it) pn(*it);
                auto ies = ie_view.get_edges(u); for (auto it = ies.begin(); it != ies.end(); ++it) pn(*it);
                double ws = (mg[cc] == gv) ? mc[cc] : 0.0; double sm = stot_[cc] - du; uint32_t best = cc; double bg = 0.0;
                for (uint32_t cm : mt) { if (cm == cc) continue; double wc = mc[cm]; double g = (wc-ws)/m_ - resolution_*stot_[cm]*du/(2.0*m_*m_) + resolution_*sm*du/(2.0*m_*m_); if (g > bg) { bg = g; best = cm; } }
                best_com[i] = best; } } };
          for (int t = 1; t < nt; ++t) threads.emplace_back(worker, t); worker(0); for (auto& th : threads) th.join(); }
        for (size_t i = bs; i < be; ++i) { vid_t u = order[i]; uint32_t cc = community_[u], nc = best_com[i]; if (nc != cc) { stot_[cc] -= degree_[u]; stot_[nc] += degree_[u]; community_[u] = nc; moved = true; improved = true; } }
      } if (!moved) break; }
  } else {
    // Pre-fetch all views once (shared by all threads, read-only)
    std::vector<CsrView> out_views(edge_triplets_.size()), in_views(edge_triplets_.size());
    for (size_t ti = 0; ti < edge_triplets_.size(); ++ti) {
      const auto& t = edge_triplets_[ti];
      out_views[ti] = graph_.GetGenericOutgoingGraphView(t.src_label, t.dst_label, t.edge_label);
      in_views[ti] = graph_.GetGenericIncomingGraphView(t.dst_label, t.src_label, t.edge_label);
    }
    for (int pass = 0; pass < 10; ++pass) { bool moved = false;
      for (size_t batch = 0; batch < num_batches; ++batch) { size_t bs = batch*chunk, be = std::min(bs+chunk, n);
        { std::atomic<size_t> cursor(bs); std::vector<std::thread> threads; threads.reserve(nt-1);
          auto worker = [&](int tid) { uint32_t* mg = thread_gen_.get()+static_cast<size_t>(tid)*array_size_; double* mc = thread_comm_weight_.get()+static_cast<size_t>(tid)*array_size_; uint32_t gv = 0; auto& mt = touched[tid];
            while (true) { size_t s = cursor.fetch_add(64); if (s >= be) break; size_t e = std::min(s+size_t(64), be);
              for (size_t i = s; i < e; ++i) { uint32_t ug = order[i]; uint32_t cc = community_[ug]; double du = degree_[ug]; size_t ul = global_to_label_idx_[ug]; vid_t uv = global_to_vid_[ug]; ++gv; mt.clear();
                auto pn = [&](uint32_t vg) { if (vg == ug) return; uint32_t cm = community_[vg]; if (mg[cm] != gv) { mg[cm] = gv; mc[cm] = 0.0; mt.push_back(cm); } mc[cm] += 1.0; };
                for (size_t ti : label_out_triplets_[ul]) { if (triplet_dst_base_[ti] == SIZE_MAX) continue; size_t db = triplet_dst_base_[ti]; auto oes = out_views[ti].get_edges(uv); for (auto it = oes.begin(); it != oes.end(); ++it) pn(static_cast<uint32_t>(db+(*it))); }
                for (size_t ti : label_in_triplets_[ul]) { if (triplet_src_base_[ti] == SIZE_MAX) continue; size_t sb = triplet_src_base_[ti]; auto ies = in_views[ti].get_edges(uv); for (auto it = ies.begin(); it != ies.end(); ++it) pn(static_cast<uint32_t>(sb+(*it))); }
                double ws = (mg[cc] == gv) ? mc[cc] : 0.0; double sm = stot_[cc] - du; uint32_t best = cc; double bg = 0.0;
                for (uint32_t cm : mt) { if (cm == cc) continue; double wc = mc[cm]; double g = (wc-ws)/m_ - resolution_*stot_[cm]*du/(2.0*m_*m_) + resolution_*sm*du/(2.0*m_*m_); if (g > bg) { bg = g; best = cm; } }
                best_com[i] = best; } } };
          for (int t = 1; t < nt; ++t) threads.emplace_back(worker, t); worker(0); for (auto& th : threads) th.join(); }
        for (size_t i = bs; i < be; ++i) { uint32_t ug = order[i], cc = community_[ug], nc = best_com[i]; if (nc != cc) { stot_[cc] -= degree_[ug]; stot_[nc] += degree_[ug]; community_[ug] = nc; moved = true; improved = true; } }
      } if (!moved) break; }
  }
  return improved;
}
void Louvain::sink(execution::Context& ctx, int node_alias, int community_alias) {
  std::unordered_map<uint32_t,uint32_t> cr;
  if (initial_community_) {
    // Stable ID: inherit old community IDs via majority vote
    std::unordered_map<uint32_t, std::unordered_map<uint32_t, uint32_t>> new_to_old_counts;
    uint32_t max_old_id = 0; bool has_valid_old = false;
    for (uint32_t gid : valid_vertices_) {
      uint32_t new_com = community_[gid]; uint32_t old_com = initial_community_[gid];
      if (old_com != UINT32_MAX) { new_to_old_counts[new_com][old_com]++; if (!has_valid_old || old_com > max_old_id) { max_old_id = old_com; has_valid_old = true; } }
      else { new_to_old_counts[new_com]; }
    }
    std::vector<std::pair<uint32_t, uint32_t>> com_sizes;
    for (auto& [nc, old_counts] : new_to_old_counts) {
      uint32_t total = 0; for (auto& [_, cnt] : old_counts) total += cnt;
      com_sizes.push_back({nc, total});
    }
    std::sort(com_sizes.begin(), com_sizes.end(), [](const auto& a, const auto& b) { return a.second > b.second; });
    std::unordered_set<uint32_t> used_ids;
    for (auto& [nc, _] : com_sizes) {
      auto& old_counts = new_to_old_counts[nc];
      uint32_t best_old = UINT32_MAX; uint32_t best_count = 0;
      for (auto& [oc, cnt] : old_counts) { if (cnt > best_count && used_ids.find(oc) == used_ids.end()) { best_count = cnt; best_old = oc; } }
      if (best_old != UINT32_MAX) { cr[nc] = best_old; used_ids.insert(best_old); }
    }
    uint32_t next_fresh = has_valid_old ? (max_old_id + 1) : 0;
    for (auto& [nc, _] : com_sizes) {
      if (cr.find(nc) == cr.end()) { while (used_ids.find(next_fresh) != used_ids.end()) next_fresh++; cr[nc] = next_fresh; used_ids.insert(next_fresh); next_fresh++; }
    }
  } else {
    uint32_t ni = 0;
    for (uint32_t gid : valid_vertices_) { uint32_t c = community_[gid]; if (cr.find(c) == cr.end()) cr[c] = ni++; }
  }
  for (size_t li = 0; li < vertex_labels_.size(); ++li) { label_t label = vertex_labels_[li]; size_t base = label_base_offsets_[li]; const auto& vs = graph_.GetVertexSet(label);
    MSVertexColumnBuilder b(label); ValueColumnBuilder<int64_t> cb; size_t cnt = 0; for (const auto& v : vs) { (void)v; cnt++; } b.reserve(cnt); cb.reserve(cnt);
    for (const auto& v : vs) { uint32_t gid = static_cast<uint32_t>(base+v); b.push_back_opt(v); cb.push_back_opt(static_cast<int64_t>(cr[community_[gid]])); }
    execution::ContextChunk chunk; chunk.set(node_alias, b.finish()); chunk.set(community_alias, cb.finish()); ctx.append_chunk(std::move(chunk)); }
}
}}}  // namespace neug::gds::community
