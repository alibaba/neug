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
#include "neug/compiler/parser/expression/parsed_expression.h"

namespace neug {
namespace binder {

class LambdaExpression final : public Expression {
  static constexpr common::ExpressionType type_ =
      common::ExpressionType::LAMBDA;

 public:
  LambdaExpression(std::unique_ptr<parser::ParsedExpression> parsedLambdaExpr,
                   std::string uniqueName)
      : Expression{type_, common::LogicalType::ANY(), std::move(uniqueName)},
        parsedLambdaExpr{std::move(parsedLambdaExpr)} {}

  void cast(const common::LogicalType& type_) override {
    NEUG_ASSERT(dataType.getLogicalTypeID() == common::LogicalTypeID::ANY);
    dataType = type_.copy();
  }

  parser::ParsedExpression* getParsedLambdaExpr() const {
    return parsedLambdaExpr.get();
  }

  void setFunctionExpr(std::shared_ptr<Expression> expr) {
    functionExpr = std::move(expr);
  }
  std::shared_ptr<Expression> getFunctionExpr() const { return functionExpr; }

  std::string toStringInternal() const override {
    return parsedLambdaExpr->toString();
  }

 private:
  std::unique_ptr<parser::ParsedExpression> parsedLambdaExpr;
  std::shared_ptr<Expression> functionExpr;
};

}  // namespace binder
}  // namespace neug
