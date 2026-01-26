/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include "expression.h"
#include "neug/compiler/function/aggregate_function.h"

namespace neug {
namespace binder {

class AggregateFunctionExpression final : public Expression {
  static constexpr common::ExpressionType expressionType_ =
      common::ExpressionType::AGGREGATE_FUNCTION;

 public:
  AggregateFunctionExpression(
      function::AggregateFunction function,
      std::unique_ptr<function::FunctionBindData> bindData,
      expression_vector children, std::string uniqueName)
      : Expression{expressionType_, bindData->resultType.copy(),
                   std::move(children), std::move(uniqueName)},
        function{std::move(function)},
        bindData{std::move(bindData)} {}

  const function::AggregateFunction& getFunction() const { return function; }
  function::FunctionBindData* getBindData() const { return bindData.get(); }
  bool isDistinct() const { return function.isDistinct; }

  std::string toStringInternal() const override;

  static std::string getUniqueName(const std::string& functionName,
                                   const expression_vector& children,
                                   bool isDistinct);

 private:
  function::AggregateFunction function;
  std::unique_ptr<function::FunctionBindData> bindData;
};

}  // namespace binder
}  // namespace neug
