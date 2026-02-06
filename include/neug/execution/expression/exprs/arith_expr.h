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
class ArithExpr : public ExprBase {
 public:
  ArithExpr(std::unique_ptr<ExprBase>&& lhs, std::unique_ptr<ExprBase>&& rhs,
            const DataType& type, const ::common::Arithmetic& arith)
      : lhs_(std::move(lhs)),
        rhs_(std::move(rhs)),
        type_(type),
        arith_(arith) {}

  const DataType& type() const override { return type_; }

  std::unique_ptr<BindedExprBase> bind(const IStorageInterface* storage,
                                       const ParamsMap& params) const override;

 private:
  std::unique_ptr<ExprBase> lhs_;
  std::unique_ptr<ExprBase> rhs_;
  DataType type_;
  ::common::Arithmetic arith_;
};
}  // namespace execution
}  // namespace neug