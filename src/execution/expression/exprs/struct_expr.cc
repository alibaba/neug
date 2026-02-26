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

#include "neug/execution/expression/exprs/struct_expr.h"

namespace neug {
namespace execution {
class BindedTupleExpr : public VertexExprBase,
                        public EdgeExprBase,
                        public RecordExprBase {
 public:
  BindedTupleExpr(std::vector<std::unique_ptr<BindedExprBase>>&& exprs,
                  const DataType& type)
      : exprs_(std::move(exprs)), type_(type) {}
  const DataType& type() const override { return type_; }

  Value eval_record(const Context& ctx, size_t idx) const override {
    std::vector<Value> values;
    for (const auto& expr : exprs_) {
      values.push_back(expr->Cast<RecordExprBase>().eval_record(ctx, idx));
    }
    return Value::STRUCT(type_, std::move(values));
  }

  Value eval_vertex(label_t v_label, vid_t v_id) const override {
    std::vector<Value> values;
    for (const auto& expr : exprs_) {
      values.push_back(expr->Cast<VertexExprBase>().eval_vertex(v_label, v_id));
    }
    return Value::STRUCT(type_, std::move(values));
  }
  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    std::vector<Value> values;
    for (const auto& expr : exprs_) {
      values.push_back(
          expr->Cast<EdgeExprBase>().eval_edge(label, src, dst, data_ptr));
    }
    return Value::STRUCT(type_, std::move(values));
  }

 private:
  std::vector<std::unique_ptr<BindedExprBase>> exprs_;
  DataType type_;
};

std::unique_ptr<BindedExprBase> TupleExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  std::vector<std::unique_ptr<BindedExprBase>> bound_exprs;
  for (const auto& expr : exprs_) {
    bound_exprs.push_back(expr->bind(storage, params));
  }
  return std::make_unique<BindedTupleExpr>(std::move(bound_exprs), type_);
}
}  // namespace execution
}  // namespace neug