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
#ifndef INCLUDE_NEUG_EXECUTION_UTILS_PREDICATES_H_
#define INCLUDE_NEUG_EXECUTION_UTILS_PREDICATES_H_

#include <map>
#include <string>

#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/utils/expr.h"
#include "neug/execution/utils/var.h"
#include "neug/generated/proto/plan/expr.pb.h"

namespace gs {

namespace runtime {

struct GeneralVertexPredicate {
  static constexpr bool is_dummy = false;
  template <typename GraphInterface>
  GeneralVertexPredicate(const GraphInterface& graph, const Context& ctx,
                         const std::map<std::string, std::string>& params,
                         const common::Expression& expr)
      : expr_(&graph, ctx, params, expr, VarType::kVertexVar) {}

  inline bool operator()(label_t label, vid_t v, size_t path_idx) const {
    auto val = expr_.eval_vertex(label, v, path_idx, arena_);
    return val.as_bool();
  }

  mutable Arena arena_;
  Expr expr_;
};

struct ExactVertexPredicate {
  static constexpr bool is_dummy = false;
  ExactVertexPredicate(label_t label, vid_t vid) : label_(label), vid_(vid) {}

  inline bool operator()(label_t label, vid_t vid, size_t path_idx) const {
    return (label == label_) && (vid == vid_);
  }

  label_t label_;
  vid_t vid_;
};

struct GeneralEdgePredicate {
  static constexpr bool is_dummy = false;

  template <typename GraphInterface>
  GeneralEdgePredicate(const GraphInterface& graph, const Context& ctx,
                       const std::map<std::string, std::string>& params,
                       const common::Expression& expr)
      : expr_(&graph, ctx, params, expr, VarType::kEdgeVar) {}

  inline bool operator()(label_t label, vid_t src, label_t nbr_label, vid_t nbr,
                         label_t e_label, Direction dir, const void* edata_ptr,
                         size_t idx) const {
    auto triplet = (dir == Direction::kOut)
                       ? LabelTriplet(label, nbr_label, e_label)
                       : LabelTriplet(nbr_label, label, e_label);
    auto val = expr_.eval_edge(triplet, dir == Direction::kOut ? src : nbr,
                               dir == Direction::kOut ? nbr : src, edata_ptr,
                               idx, arena_);
    return val.as_bool();
  }

  mutable Arena arena_;
  Expr expr_;
};

struct DummyVertexPredicate {
  static constexpr bool is_dummy = true;
  bool operator()(label_t label, vid_t v, size_t path_idx) const {
    return true;
  }
};

template <typename VERTEX_PRED_T>
struct EdgeNbrPredicate {
  static constexpr bool is_dummy = VERTEX_PRED_T::is_dummy;
  explicit EdgeNbrPredicate(const VERTEX_PRED_T& vertex_pred_)
      : vertex_pred(vertex_pred_) {}

  inline bool operator()(label_t label, vid_t src, label_t nbr_label, vid_t nbr,
                         label_t edge_label, Direction dir,
                         const void* data_ptr, size_t idx) const {
    return vertex_pred(nbr_label, nbr, idx);
  }

  const VERTEX_PRED_T& vertex_pred;
};

template <typename VERTEX_PRED_T, typename EDGE_PRED_T>
struct EdgeAndNbrPredicate {
  static constexpr bool is_dummy =
      VERTEX_PRED_T::is_dummy && EDGE_PRED_T::is_dummy;

  EdgeAndNbrPredicate(const VERTEX_PRED_T& vertex_pred_,
                      const EDGE_PRED_T& edge_pred_)
      : vertex_pred(vertex_pred_), edge_pred(edge_pred_) {}

  inline bool operator()(label_t label, vid_t src, label_t nbr_label, vid_t nbr,
                         label_t edge_label, Direction dir,
                         const void* data_ptr, size_t path_idx) const {
    return vertex_pred(nbr_label, nbr, path_idx) &&
           edge_pred(label, src, nbr_label, nbr, edge_label, dir, data_ptr,
                     path_idx);
  }

  const VERTEX_PRED_T& vertex_pred;
  const EDGE_PRED_T& edge_pred;
};

}  // namespace runtime

}  // namespace gs

#endif  // INCLUDE_NEUG_EXECUTION_UTILS_PREDICATES_H_
