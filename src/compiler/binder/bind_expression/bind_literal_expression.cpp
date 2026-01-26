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

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/expression/literal_expression.h"
#include "neug/compiler/binder/expression_binder.h"
#include "neug/compiler/parser/expression/parsed_literal_expression.h"

using namespace neug::parser;
using namespace neug::common;

namespace neug {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindLiteralExpression(
    const ParsedExpression& parsedExpression) const {
  auto& literalExpression =
      parsedExpression.constCast<ParsedLiteralExpression>();
  auto value = literalExpression.getValue();
  if (value.isNull()) {
    return createNullLiteralExpression(value);
  }
  return createLiteralExpression(value);
}

std::shared_ptr<Expression> ExpressionBinder::createLiteralExpression(
    const Value& value) const {
  auto uniqueName = binder->getUniqueExpressionName(value.toString());
  return std::make_unique<LiteralExpression>(value, uniqueName);
}

std::shared_ptr<Expression> ExpressionBinder::createLiteralExpression(
    const std::string& strVal) const {
  return createLiteralExpression(Value(strVal));
}

std::shared_ptr<Expression> ExpressionBinder::createNullLiteralExpression()
    const {
  return make_shared<LiteralExpression>(
      Value::createNullValue(), binder->getUniqueExpressionName("NULL"));
}

std::shared_ptr<Expression> ExpressionBinder::createNullLiteralExpression(
    const Value& value) const {
  return make_shared<LiteralExpression>(
      value, binder->getUniqueExpressionName("NULL"));
}

}  // namespace binder
}  // namespace neug
