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

#include "neug/execution/utils/expr_impl.h"

namespace neug {

namespace runtime {
class Context;
struct LabelTriplet;

class Expr {
 public:
  Expr(const StorageReadInterface* graph, const Context& ctx,
       const ParamsMap& params, const ::common::Expression& expr,
       VarType var_type) {
    expr_ = parse_expression(graph, ctx, params, expr, var_type);
    if (!expr_) {
      THROW_RUNTIME_ERROR(
          "Fail to create GeneralEdgePredicate: expr is null: " +
          expr.DebugString());
    }
  }

  Value eval_path(size_t idx) const;
  Value eval_vertex(label_t label, vid_t v) const;
  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const;

  const DataType& type() const;

  bool is_optional() const { return expr_->is_optional(); }

 private:
  std::unique_ptr<ExprBase> expr_;
};

struct PredWrapper {
 public:
  explicit PredWrapper(Expr&& pred) : pred(std::move(pred)) {}

  bool operator()(size_t idx) const { return pred.eval_path(idx).IsTrue(); }

  bool operator()(label_t label, vid_t v) const {
    return pred.eval_vertex(label, v).IsTrue();
  }

  bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const {
    return pred.eval_edge(label, src, dst, data_ptr).IsTrue();
  }

 private:
  Expr pred;
};

}  // namespace runtime

}  // namespace neug
