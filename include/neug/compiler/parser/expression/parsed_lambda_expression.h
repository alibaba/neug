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

#include "neug/compiler/common/enums/expression_type.h"
#include "parsed_expression.h"

namespace neug {
namespace parser {

class ParsedLambdaExpression : public ParsedExpression {
  static constexpr const common::ExpressionType type_ =
      common::ExpressionType::LAMBDA;

 public:
  ParsedLambdaExpression(std::vector<std::string> varNames,
                         std::unique_ptr<ParsedExpression> expr,
                         std::string rawName)
      : ParsedExpression{type_, rawName},
        varNames{std::move(varNames)},
        functionExpr{std::move(expr)} {}

  std::vector<std::string> getVarNames() const { return varNames; }

  ParsedExpression* getFunctionExpr() const { return functionExpr.get(); }

  std::unique_ptr<ParsedExpression> copy() const override {
    return std::make_unique<ParsedLambdaExpression>(
        varNames, functionExpr->copy(), rawName);
  }

 private:
  std::vector<std::string> varNames;
  std::unique_ptr<ParsedExpression> functionExpr;
};

}  // namespace parser
}  // namespace neug
