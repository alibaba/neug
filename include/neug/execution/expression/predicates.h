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

#include "neug/execution/common/context.h"
#include "neug/execution/expression/expr.h"

namespace neug {

namespace execution {

class GeneralPred {
 public:
  static constexpr bool is_dummy = false;
  explicit GeneralPred(std::unique_ptr<BindedExprBase>&& pred)
      : pred_(std::move(pred)) {}

  inline bool operator()(label_t v_label, vid_t v_id) const {
    VertexExprBase& vertex_expr = pred_->Cast<VertexExprBase>();
    return vertex_expr.eval_vertex(v_label, v_id).IsTrue();
  }

  inline bool operator()(const LabelTriplet& triplet, vid_t src, vid_t dst,
                         const void* edge_data) const {
    EdgeExprBase& edge_expr = pred_->Cast<EdgeExprBase>();
    return edge_expr.eval_edge(triplet, src, dst, edge_data).IsTrue();
  }

  inline bool operator()(const Context& ctx, size_t idx) const {
    RecordExprBase& record_expr = pred_->Cast<RecordExprBase>();
    return record_expr.eval_record(ctx, idx).IsTrue();
  }

 private:
  std::unique_ptr<BindedExprBase> pred_;
};

class DummyPred {
 public:
  static constexpr bool is_dummy = true;

  inline bool operator()(label_t v_label, vid_t v_id) const { return true; }

  inline bool operator()(const LabelTriplet& triplet, vid_t src, vid_t dst,
                         const void* edge_data) const {
    return true;
  }

  inline bool operator()(const Context& ctx, size_t idx) const { return true; }
};

template <typename EDGE_PRED_T>
struct EdgePredicate {
  static constexpr bool is_dummy = EDGE_PRED_T::is_dummy;
  explicit EdgePredicate(const EDGE_PRED_T& edge_pred_)
      : edge_pred(edge_pred_) {}
  inline bool operator()(label_t label, vid_t src, label_t nbr_label, vid_t nbr,
                         label_t edge_label, Direction dir,
                         const void* data_ptr) const {
    auto triplet = (dir == Direction::kOut)
                       ? LabelTriplet(label, nbr_label, edge_label)
                       : LabelTriplet(nbr_label, label, edge_label);
    return edge_pred(triplet, dir == Direction::kOut ? src : nbr,
                     dir == Direction::kOut ? nbr : src, data_ptr);
  }
  const EDGE_PRED_T& edge_pred;
};

template <typename VERTEX_PRED_T>
struct EdgeNbrPredicate {
  static constexpr bool is_dummy = VERTEX_PRED_T::is_dummy;
  explicit EdgeNbrPredicate(const VERTEX_PRED_T& vertex_pred_)
      : vertex_pred(vertex_pred_) {}

  inline bool operator()(label_t label, vid_t src, label_t nbr_label, vid_t nbr,
                         label_t edge_label, Direction dir,
                         const void* data_ptr) const {
    return vertex_pred(nbr_label, nbr);
  }

  const VERTEX_PRED_T& vertex_pred;
};

struct EdgeAndNbrPredicate {
  EdgeAndNbrPredicate(std::unique_ptr<BindedExprBase>&& v_expr,
                      std::unique_ptr<BindedExprBase>&& e_expr)
      : v_expr_(std::move(v_expr)), e_expr_(std::move(e_expr)){};

  bool operator()(label_t v_label, vid_t v_id, label_t nbr_label, vid_t nbr_id,
                  label_t e_label, Direction dir, const void* edata_ptr) const {
    if (v_expr_) {
      auto v_val =
          v_expr_->Cast<VertexExprBase>().eval_vertex(nbr_label, nbr_id);
      if (!v_val.IsTrue()) {
        return false;
      }
    }
    if (e_expr_) {
      auto triplet = (dir == Direction::kOut)
                         ? LabelTriplet(v_label, nbr_label, e_label)
                         : LabelTriplet(nbr_label, v_label, e_label);
      return e_expr_->Cast<EdgeExprBase>()
          .eval_edge(triplet, dir == Direction::kOut ? v_id : nbr_id,
                     dir == Direction::kOut ? nbr_id : v_id, edata_ptr)
          .IsTrue();
    }
    return true;
  }

  std::unique_ptr<BindedExprBase> v_expr_;
  std::unique_ptr<BindedExprBase> e_expr_;
};

}  // namespace execution

}  // namespace neug
