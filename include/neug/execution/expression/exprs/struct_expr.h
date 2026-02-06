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
#include "neug/execution/expression/expr.h"

namespace neug {
namespace execution {
class TupleExpr : public ExprBase {
 public:
  TupleExpr(std::vector<std::unique_ptr<ExprBase>>&& exprs)
      : exprs_(std::move(exprs)) {
    std::vector<DataType> child_types;
    for (const auto& expr : exprs_) {
      child_types.push_back(expr->type());
    }
    type_ = DataType::Struct(child_types);
  }
  ~TupleExpr() override = default;
  const DataType& type() const override { return type_; }
  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;

 private:
  std::vector<std::unique_ptr<ExprBase>> exprs_;
  DataType type_;
};
}  // namespace execution
}  // namespace neug