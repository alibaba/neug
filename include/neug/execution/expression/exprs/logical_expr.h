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
#include "neug/generated/proto/plan/expr.pb.h"

namespace neug {
namespace execution {
class UnaryLogicalExpr : public ExprBase {
 public:
  UnaryLogicalExpr(std::unique_ptr<ExprBase>&& operand,
                   const ::common::Logical& logical)
      : operand_(std::move(operand)),
        type_(DataType::BOOLEAN),
        logical_(logical) {}
  ~UnaryLogicalExpr() override = default;
  const DataType& type() const override { return type_; }
  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;

 private:
  std::unique_ptr<ExprBase> operand_;
  DataType type_;
  ::common::Logical logical_;
};

class BinaryLogicalExpr : public ExprBase {
 public:
  BinaryLogicalExpr(std::unique_ptr<ExprBase>&& lhs,
                    std::unique_ptr<ExprBase>&& rhs,
                    const ::common::Logical& logical)
      : lhs_(std::move(lhs)),
        rhs_(std::move(rhs)),
        type_(DataType::BOOLEAN),
        logical_(logical) {}
  ~BinaryLogicalExpr() override = default;
  const DataType& type() const override { return type_; }
  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;

 private:
  std::unique_ptr<ExprBase> lhs_;
  std::unique_ptr<ExprBase> rhs_;
  DataType type_;
  ::common::Logical logical_;
};

class WithInExpr : public ExprBase {
 public:
  WithInExpr(std::unique_ptr<ExprBase>&& expr,
             std::unique_ptr<ExprBase>&& list_expr)
      : expr_(std::move(expr)),
        list_expr_(std::move(list_expr)),
        type_(DataType::BOOLEAN) {}
  ~WithInExpr() override = default;
  const DataType& type() const override { return type_; }
  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;

 private:
  std::unique_ptr<ExprBase> expr_;
  std::unique_ptr<ExprBase> list_expr_;
  DataType type_;
};
}  // namespace execution
}  // namespace neug