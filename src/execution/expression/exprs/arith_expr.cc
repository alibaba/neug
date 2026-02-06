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

#include "neug/execution/expression/exprs/arith_expr.h"

namespace neug {
namespace execution {
class BindedArithExpr : public VertexExprBase,
                        public EdgeExprBase,
                        public RecordExprBase {
 public:
  BindedArithExpr(std::unique_ptr<BindedExprBase>&& lhs,
                  std::unique_ptr<BindedExprBase>&& rhs, const DataType& type,
                  const ::common::Arithmetic& arith)
      : lhs_(std::move(lhs)),
        rhs_(std::move(rhs)),
        type_(type),
        arith_(arith) {}

  const DataType& type() const override { return type_; }

  Value eval_record(const Context& ctx, size_t idx) const override {
    const auto& lhs_val = lhs_->Cast<RecordExprBase>().eval_record(ctx, idx);
    const auto& rhs_val = rhs_->Cast<RecordExprBase>().eval_record(ctx, idx);
    return eval_impl(arith_, lhs_val, rhs_val);
  }

  Value eval_vertex(label_t v_label, vid_t v_id) const override {
    const auto& lhs_val =
        lhs_->Cast<VertexExprBase>().eval_vertex(v_label, v_id);
    const auto& rhs_val =
        rhs_->Cast<VertexExprBase>().eval_vertex(v_label, v_id);
    return eval_impl(arith_, lhs_val, rhs_val);
  }

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    const auto& lhs_val =
        lhs_->Cast<EdgeExprBase>().eval_edge(label, src, dst, data_ptr);
    const auto& rhs_val =
        rhs_->Cast<EdgeExprBase>().eval_edge(label, src, dst, data_ptr);
    return eval_impl(arith_, lhs_val, rhs_val);
  }

 private:
  Value eval_impl(const ::common::Arithmetic& arith, const Value& lhs,
                  const Value& rhs) const {
    switch (arith) {
    case ::common::Arithmetic::ADD:
      return lhs + rhs;
    case ::common::Arithmetic::SUB:
      return lhs - rhs;
    case ::common::Arithmetic::MUL:
      return lhs * rhs;
    case ::common::Arithmetic::DIV:
      return lhs / rhs;
    case ::common::Arithmetic::MOD:
      return lhs % rhs;
    default:
      LOG(FATAL) << "Unsupported arithmetic operation: "
                 << static_cast<int>(arith);
      return Value(type_);
    }
  }

  std::unique_ptr<BindedExprBase> lhs_;
  std::unique_ptr<BindedExprBase> rhs_;
  DataType type_;
  ::common::Arithmetic arith_;
};

std::unique_ptr<BindedExprBase> ArithExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  auto bound_lhs = lhs_->bind(storage, params);
  auto bound_rhs = rhs_->bind(storage, params);
  return std::make_unique<BindedArithExpr>(std::move(bound_lhs),
                                           std::move(bound_rhs), type_, arith_);
}

}  // namespace execution
}  // namespace neug