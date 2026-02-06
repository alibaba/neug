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

#include "neug/execution/expression/exprs/case_when.h"

namespace neug {
namespace execution {

class BindedCaseWhenExpr : public VertexExprBase,
                           public EdgeExprBase,
                           public RecordExprBase {
 public:
  BindedCaseWhenExpr(
      const DataType& type,
      std::vector<std::pair<std::unique_ptr<BindedExprBase>,
                            std::unique_ptr<BindedExprBase>>>&& when_then_exprs,
      std::unique_ptr<BindedExprBase>&& else_expr)
      : type_(type),
        when_then_exprs_(std::move(when_then_exprs)),
        else_expr_(std::move(else_expr)) {}
  ~BindedCaseWhenExpr() override = default;

  Value eval_record(const Context& ctx, size_t idx) const override {
    for (const auto& when_then : when_then_exprs_) {
      Value when_val =
          when_then.first->Cast<RecordExprBase>().eval_record(ctx, idx);
      if (when_val.IsTrue()) {
        return when_then.second->Cast<RecordExprBase>().eval_record(ctx, idx);
      }
    }
    return else_expr_->Cast<RecordExprBase>().eval_record(ctx, idx);
  }

  Value eval_vertex(label_t v_label, vid_t v_id) const override {
    for (const auto& when_then : when_then_exprs_) {
      Value when_val =
          when_then.first->Cast<VertexExprBase>().eval_vertex(v_label, v_id);
      if (when_val.IsTrue()) {
        return when_then.second->Cast<VertexExprBase>().eval_vertex(v_label,
                                                                    v_id);
      }
    }
    return else_expr_->Cast<VertexExprBase>().eval_vertex(v_label, v_id);
  }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    for (const auto& when_then : when_then_exprs_) {
      Value when_val = when_then.first->Cast<EdgeExprBase>().eval_edge(
          label, src, dst, data_ptr);
      if (when_val.IsTrue()) {
        return when_then.second->Cast<EdgeExprBase>().eval_edge(label, src, dst,
                                                                data_ptr);
      }
    }
    return else_expr_->Cast<EdgeExprBase>().eval_edge(label, src, dst,
                                                      data_ptr);
  }

  const DataType& type() const override { return type_; }

 private:
  DataType type_;
  std::vector<std::pair<std::unique_ptr<BindedExprBase>,
                        std::unique_ptr<BindedExprBase>>>
      when_then_exprs_;
  std::unique_ptr<BindedExprBase> else_expr_;
};

std::unique_ptr<BindedExprBase> CaseWhenExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  std::vector<std::pair<std::unique_ptr<BindedExprBase>,
                        std::unique_ptr<BindedExprBase>>>
      bound_when_then_exprs;
  for (const auto& when_then : when_then_exprs_) {
    bound_when_then_exprs.emplace_back(when_then.first->bind(storage, params),
                                       when_then.second->bind(storage, params));
  }
  auto bound_else_expr = else_expr_->bind(storage, params);
  return std::make_unique<BindedCaseWhenExpr>(
      type_, std::move(bound_when_then_exprs), std::move(bound_else_expr));
}
}  // namespace execution
}  // namespace neug