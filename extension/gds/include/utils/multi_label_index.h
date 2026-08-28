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

#pragma once

#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "neug/common/types/graph_types.h"
#include "neug/execution/expression/expr.h"
#include "neug/execution/expression/predicates.h"
#include "neug/storages/csr/csr_view.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/property/types.h"

namespace neug {
namespace gds {

/**
 * @brief Unified multi-label subgraph index for GDS algorithms.
 *
 * Encapsulates the common boilerplate for algorithms that operate on a
 * subgraph defined by a set of vertex labels and edge triplets:
 * - Global ID assignment (dense per-label base offsets)
 * - Triplet routing (which triplets touch which label)
 * - Weight accessor management
 * - Simple-graph fast-path detection
 * - Cached CsrView objects (created once at construction)
 *
 * Usage:
 * @code
 *   MultiLabelIndex index(graph, {person_label}, {{person, person, knows}});
 *   for (uint32_t gid : index.valid_vertices()) {
 *     index.for_each_neighbor(gid, [&](uint32_t nbr, double w) {
 *       // ...
 *     });
 *   }
 * @endcode
 *
 * Optional predicates: `vertex_preds` (one per vertex label, entries may be
 * null) filter which vertices are indexed; `edge_preds` (one per edge
 * triplet, entries may be null) filter which edges are traversed. Vertices
 * excluded by a vertex predicate never appear in `valid_vertices()` and no
 * traversal visits them or their incident edges.
 */
class MultiLabelIndex {
 public:
  MultiLabelIndex(const StorageReadInterface& graph,
                  std::vector<label_t> vertex_labels,
                  std::vector<LabelTriplet> edge_triplets,
                  const std::string& weight_property = "",
                  std::vector<execution::ExprBase*> vertex_preds = {},
                  std::vector<execution::ExprBase*> edge_preds = {})
      : vertex_labels_(std::move(vertex_labels)),
        edge_triplets_(std::move(edge_triplets)),
        has_weight_(!weight_property.empty()) {
    // --- Bind predicates (if any) ---
    if (!vertex_preds.empty() && vertex_preds.size() != vertex_labels_.size()) {
      THROW_RUNTIME_ERROR("MultiLabelIndex: vertex predicate count (" +
                          std::to_string(vertex_preds.size()) +
                          ") must match vertex label count (" +
                          std::to_string(vertex_labels_.size()) + ").");
    }
    if (!edge_preds.empty() && edge_preds.size() != edge_triplets_.size()) {
      THROW_RUNTIME_ERROR("MultiLabelIndex: edge predicate count (" +
                          std::to_string(edge_preds.size()) +
                          ") must match edge triplet count (" +
                          std::to_string(edge_triplets_.size()) + ").");
    }
    bool has_vertex_pred = false;
    vertex_preds_.resize(vertex_labels_.size());
    for (size_t li = 0; li < vertex_labels_.size(); ++li) {
      execution::ExprBase* pred =
          (li < vertex_preds.size()) ? vertex_preds[li] : nullptr;
      if (pred != nullptr) {
        vertex_preds_[li] =
            std::make_unique<execution::GeneralPred>(pred->bind(&graph, {}));
        has_vertex_pred = true;
      }
    }
    edge_preds_.resize(edge_triplets_.size());
    for (size_t ti = 0; ti < edge_triplets_.size(); ++ti) {
      execution::ExprBase* pred =
          (ti < edge_preds.size()) ? edge_preds[ti] : nullptr;
      if (pred != nullptr) {
        edge_preds_[ti] =
            std::make_unique<execution::GeneralPred>(pred->bind(&graph, {}));
      }
    }

    // --- Label-to-index map ---
    for (size_t i = 0; i < vertex_labels_.size(); ++i)
      label_to_index_[vertex_labels_[i]] = i;

    // --- Compute per-label base offsets and sizes ---
    label_base_offsets_.resize(vertex_labels_.size(), 0);
    label_local_sizes_.resize(vertex_labels_.size(), 0);
    size_t total_array_size = 0;
    for (size_t li = 0; li < vertex_labels_.size(); ++li) {
      label_base_offsets_[li] = total_array_size;
      const auto& vs = graph.GetVertexSet(vertex_labels_[li]);
      vid_t max_vid = 0;
      for (const auto& v : vs) {
        if (v > max_vid)
          max_vid = v;
      }
      label_local_sizes_[li] =
          (vs.size() > 0) ? (static_cast<size_t>(max_vid) + 1) : 0;
      total_array_size += label_local_sizes_[li];
    }
    // Fail fast: global IDs are stored as uint32_t, so the total index
    // space must fit. (Can be exceeded with very sparse vertex IDs.)
    if (total_array_size > UINT32_MAX) {
      THROW_RUNTIME_ERROR(
          "MultiLabelIndex: total global-ID space (" +
          std::to_string(total_array_size) +
          ") exceeds uint32_t capacity; vertex IDs are too sparse/large. "
          "Consider projecting a smaller subgraph.");
    }
    array_size_ = total_array_size;

    // --- Build global ID mappings ---
    global_to_label_.resize(array_size_, 0);
    global_to_vid_.resize(array_size_, 0);
    global_to_label_idx_.resize(array_size_, 0);
    if (has_vertex_pred) {
      vertex_valid_.assign(array_size_, 0);
    }
    for (size_t li = 0; li < vertex_labels_.size(); ++li) {
      const auto& vs = graph.GetVertexSet(vertex_labels_[li]);
      size_t base = label_base_offsets_[li];
      for (const auto& v : vs) {
        if (vertex_preds_[li] && !(*vertex_preds_[li])(vertex_labels_[li], v))
          continue;
        uint32_t gid = static_cast<uint32_t>(base + v);
        valid_vertices_.push_back(gid);
        global_to_label_[gid] = vertex_labels_[li];
        global_to_vid_[gid] = v;
        global_to_label_idx_[gid] = li;
        if (has_vertex_pred)
          vertex_valid_[gid] = 1;
      }
    }
    vertex_count_ = valid_vertices_.size();

    // --- Triplet routing ---
    label_out_triplets_.resize(vertex_labels_.size());
    label_in_triplets_.resize(vertex_labels_.size());
    for (size_t ti = 0; ti < edge_triplets_.size(); ++ti) {
      auto si = label_to_index_.find(edge_triplets_[ti].src_label);
      auto di = label_to_index_.find(edge_triplets_[ti].dst_label);
      if (si != label_to_index_.end())
        label_out_triplets_[si->second].push_back(ti);
      if (di != label_to_index_.end())
        label_in_triplets_[di->second].push_back(ti);
    }
    triplet_src_base_.resize(edge_triplets_.size(), SIZE_MAX);
    triplet_dst_base_.resize(edge_triplets_.size(), SIZE_MAX);
    for (size_t ti = 0; ti < edge_triplets_.size(); ++ti) {
      const auto& t = edge_triplets_[ti];
      auto si = label_to_index_.find(t.src_label);
      auto di = label_to_index_.find(t.dst_label);
      if (si != label_to_index_.end())
        triplet_src_base_[ti] = label_base_offsets_[si->second];
      if (di != label_to_index_.end())
        triplet_dst_base_[ti] = label_base_offsets_[di->second];
    }

    // --- Simple-graph detection ---
    is_simple_graph_ =
        (vertex_labels_.size() == 1 && edge_triplets_.size() == 1 &&
         edge_triplets_[0].src_label == vertex_labels_[0] &&
         edge_triplets_[0].dst_label == vertex_labels_[0]);
    if (is_simple_graph_) {
      simple_vertex_label_ = vertex_labels_[0];
      simple_edge_label_ = edge_triplets_[0].edge_label;
    }

    // --- Cache CsrViews ---
    if (is_simple_graph_) {
      simple_out_view_ = graph.GetGenericOutgoingGraphView(
          simple_vertex_label_, simple_vertex_label_, simple_edge_label_);
      simple_in_view_ = graph.GetGenericIncomingGraphView(
          simple_vertex_label_, simple_vertex_label_, simple_edge_label_);
    } else {
      out_views_.resize(edge_triplets_.size());
      in_views_.resize(edge_triplets_.size());
      for (size_t ti = 0; ti < edge_triplets_.size(); ++ti) {
        const auto& t = edge_triplets_[ti];
        out_views_[ti] = graph.GetGenericOutgoingGraphView(
            t.src_label, t.dst_label, t.edge_label);
        in_views_[ti] = graph.GetGenericIncomingGraphView(
            t.dst_label, t.src_label, t.edge_label);
      }
    }

    // --- Weight accessors ---
    triplet_weight_accessors_.resize(edge_triplets_.size());
    triplet_has_weight_.resize(edge_triplets_.size(), false);
    if (has_weight_) {
      if (is_simple_graph_) {
        try {
          weight_accessor_ = graph.GetEdgeDataAccessor(
              simple_vertex_label_, simple_vertex_label_, simple_edge_label_,
              weight_property);
        } catch (const std::exception& e) {
          LOG(WARNING) << "Edge property '" << weight_property
                       << "' not found on edge label " << simple_edge_label_
                       << "; using default weight 1.0";
          has_weight_ = false;
        }
      } else {
        for (size_t ti = 0; ti < edge_triplets_.size(); ++ti) {
          const auto& t = edge_triplets_[ti];
          try {
            triplet_weight_accessors_[ti] = graph.GetEdgeDataAccessor(
                t.src_label, t.dst_label, t.edge_label, weight_property);
            triplet_has_weight_[ti] = true;
          } catch (const std::exception& e) {
            LOG(WARNING) << "Edge property '" << weight_property
                         << "' not found on edge triplet [" << t.src_label
                         << ", " << t.dst_label << ", " << t.edge_label
                         << "]; using default weight 1.0";
            triplet_has_weight_[ti] = false;
          }
        }
      }
    }
  }

  // ─── Core traversal ───────────────────────────────────────────────

  /// Iterate all neighbors (out + in) of gid, invoking fn(nbr_gid, weight).
  /// Edges failing the edge predicate and neighbors excluded by a vertex
  /// predicate are skipped.
  template <typename Fn>
  void for_each_neighbor(uint32_t gid, Fn&& fn) const {
    if (is_simple_graph_) {
      vid_t u = global_to_vid_[gid];
      auto oes = simple_out_view_.get_edges(u);
      for (auto it = oes.begin(); it != oes.end(); ++it) {
        uint32_t nbr = static_cast<uint32_t>(*it);
        if (!vertex_ok(nbr) || !edge_ok(0, u, *it, it.get_data_ptr()))
          continue;
        fn(nbr,
           has_weight_ ? weight_accessor_.get_typed_data<double>(it) : 1.0);
      }
      auto ies = simple_in_view_.get_edges(u);
      for (auto it = ies.begin(); it != ies.end(); ++it) {
        uint32_t nbr = static_cast<uint32_t>(*it);
        if (!vertex_ok(nbr) || !edge_ok(0, *it, u, it.get_data_ptr()))
          continue;
        fn(nbr,
           has_weight_ ? weight_accessor_.get_typed_data<double>(it) : 1.0);
      }
    } else {
      size_t li = global_to_label_idx_[gid];
      vid_t lv = global_to_vid_[gid];
      for (size_t ti : label_out_triplets_[li]) {
        if (triplet_dst_base_[ti] == SIZE_MAX)
          continue;
        size_t db = triplet_dst_base_[ti];
        auto oes = out_views_[ti].get_edges(lv);
        for (auto it = oes.begin(); it != oes.end(); ++it) {
          uint32_t nbr = static_cast<uint32_t>(db + (*it));
          if (!vertex_ok(nbr) || !edge_ok(ti, lv, *it, it.get_data_ptr()))
            continue;
          fn(nbr, triplet_has_weight_[ti]
                      ? triplet_weight_accessors_[ti].get_typed_data<double>(it)
                      : 1.0);
        }
      }
      for (size_t ti : label_in_triplets_[li]) {
        if (triplet_src_base_[ti] == SIZE_MAX)
          continue;
        size_t sb = triplet_src_base_[ti];
        auto ies = in_views_[ti].get_edges(lv);
        for (auto it = ies.begin(); it != ies.end(); ++it) {
          uint32_t nbr = static_cast<uint32_t>(sb + (*it));
          if (!vertex_ok(nbr) || !edge_ok(ti, *it, lv, it.get_data_ptr()))
            continue;
          fn(nbr, triplet_has_weight_[ti]
                      ? triplet_weight_accessors_[ti].get_typed_data<double>(it)
                      : 1.0);
        }
      }
    }
  }

  /// Iterate only outgoing edges of gid, invoking fn(nbr_gid, weight).
  /// Useful for modularity computation (count each undirected edge once).
  template <typename Fn>
  void for_each_out_edge(uint32_t gid, Fn&& fn) const {
    if (is_simple_graph_) {
      vid_t u = global_to_vid_[gid];
      auto oes = simple_out_view_.get_edges(u);
      for (auto it = oes.begin(); it != oes.end(); ++it) {
        uint32_t nbr = static_cast<uint32_t>(*it);
        if (!vertex_ok(nbr) || !edge_ok(0, u, *it, it.get_data_ptr()))
          continue;
        fn(nbr,
           has_weight_ ? weight_accessor_.get_typed_data<double>(it) : 1.0);
      }
    } else {
      size_t li = global_to_label_idx_[gid];
      vid_t lv = global_to_vid_[gid];
      for (size_t ti : label_out_triplets_[li]) {
        if (triplet_dst_base_[ti] == SIZE_MAX)
          continue;
        size_t db = triplet_dst_base_[ti];
        auto oes = out_views_[ti].get_edges(lv);
        for (auto it = oes.begin(); it != oes.end(); ++it) {
          uint32_t nbr = static_cast<uint32_t>(db + (*it));
          if (!vertex_ok(nbr) || !edge_ok(ti, lv, *it, it.get_data_ptr()))
            continue;
          fn(nbr, triplet_has_weight_[ti]
                      ? triplet_weight_accessors_[ti].get_typed_data<double>(it)
                      : 1.0);
        }
      }
    }
  }

  // ─── Mapping ──────────────────────────────────────────────────────

  inline vid_t local_vid(uint32_t gid) const { return global_to_vid_[gid]; }
  inline size_t label_idx(uint32_t gid) const {
    return global_to_label_idx_[gid];
  }
  inline label_t label_of(uint32_t gid) const { return global_to_label_[gid]; }

  // ─── Predicate helpers ────────────────────────────────────────────

  /// True if the vertex with global id `gid` passes the vertex predicate.
  inline bool is_valid(uint32_t gid) const {
    return vertex_valid_.empty() || vertex_valid_[gid];
  }
  /// True if the edge of triplet `ti` from `src` to `dst` passes the edge
  /// predicate. Exposed for algorithms that traverse raw CsrViews directly.
  inline bool edge_ok(size_t ti, vid_t src, vid_t dst,
                      const void* edge_data) const {
    return !edge_preds_[ti] ||
           (*edge_preds_[ti])(edge_triplets_[ti], src, dst, edge_data);
  }

  // ─── Properties ───────────────────────────────────────────────────

  const std::vector<uint32_t>& valid_vertices() const {
    return valid_vertices_;
  }
  size_t vertex_count() const { return vertex_count_; }
  size_t array_size() const { return array_size_; }
  bool is_simple_graph() const { return is_simple_graph_; }
  label_t simple_vertex_label() const { return simple_vertex_label_; }
  label_t simple_edge_label() const { return simple_edge_label_; }
  bool has_weight() const { return has_weight_; }
  const std::vector<label_t>& vertex_labels() const { return vertex_labels_; }
  const std::vector<LabelTriplet>& edge_triplets() const {
    return edge_triplets_;
  }

  // ─── Simple-graph direct access (for fast-path algorithms) ────────

  const CsrView& simple_out_view() const { return simple_out_view_; }
  const CsrView& simple_in_view() const { return simple_in_view_; }
  const EdgeDataAccessor& simple_weight_accessor() const {
    return weight_accessor_;
  }

  // ─── Generic-path direct access (for algorithms needing raw views) ─

  const std::vector<CsrView>& out_views() const { return out_views_; }
  const std::vector<CsrView>& in_views() const { return in_views_; }
  const std::vector<std::vector<size_t>>& label_out_triplets() const {
    return label_out_triplets_;
  }
  const std::vector<std::vector<size_t>>& label_in_triplets() const {
    return label_in_triplets_;
  }
  const std::vector<size_t>& triplet_src_base() const {
    return triplet_src_base_;
  }
  const std::vector<size_t>& triplet_dst_base() const {
    return triplet_dst_base_;
  }
  const std::vector<EdgeDataAccessor>& triplet_weight_accessors() const {
    return triplet_weight_accessors_;
  }
  const std::vector<bool>& triplet_has_weight() const {
    return triplet_has_weight_;
  }
  size_t label_base_offset(size_t li) const { return label_base_offsets_[li]; }

 private:
  /// True if the neighbor global id passes the vertex predicate.
  inline bool vertex_ok(uint32_t nbr_gid) const {
    return vertex_valid_.empty() || vertex_valid_[nbr_gid];
  }

  std::vector<label_t> vertex_labels_;
  std::vector<LabelTriplet> edge_triplets_;
  bool has_weight_ = false;

  // Label indexing
  std::unordered_map<label_t, size_t> label_to_index_;
  std::vector<size_t> label_base_offsets_;
  std::vector<size_t> label_local_sizes_;

  // Global ID mapping
  std::vector<label_t> global_to_label_;
  std::vector<vid_t> global_to_vid_;
  std::vector<size_t> global_to_label_idx_;
  std::vector<uint32_t> valid_vertices_;
  size_t vertex_count_ = 0;
  size_t array_size_ = 0;

  // Triplet routing
  std::vector<std::vector<size_t>> label_out_triplets_;
  std::vector<std::vector<size_t>> label_in_triplets_;
  std::vector<size_t> triplet_src_base_;
  std::vector<size_t> triplet_dst_base_;

  // Simple-graph fast path
  bool is_simple_graph_ = false;
  label_t simple_vertex_label_{};
  label_t simple_edge_label_{};

  // Cached CsrViews
  CsrView simple_out_view_;
  CsrView simple_in_view_;
  std::vector<CsrView> out_views_;
  std::vector<CsrView> in_views_;

  // Weight accessors
  EdgeDataAccessor weight_accessor_;
  std::vector<EdgeDataAccessor> triplet_weight_accessors_;
  std::vector<bool> triplet_has_weight_;

  // Predicates
  std::vector<std::unique_ptr<execution::GeneralPred>> vertex_preds_;
  std::vector<std::unique_ptr<execution::GeneralPred>> edge_preds_;
  // Empty when no vertex predicate is present (all vertices are valid).
  std::vector<uint8_t> vertex_valid_;
};

}  // namespace gds
}  // namespace neug
