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
#include "neug/compiler/binder/expression/variable_expression.h"
#include "neug/compiler/binder/expression_binder.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/expression/parsed_variable_expression.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/exception/message.h"

using namespace neug::common;
using namespace neug::parser;

namespace neug {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindVariableExpression(
    const ParsedExpression& parsedExpression) const {
  auto& variableExpression =
      neug_dynamic_cast<const ParsedVariableExpression&>(parsedExpression);
  auto variableName = variableExpression.getVariableName();
  return bindVariableExpression(variableName);
}

std::shared_ptr<Expression> ExpressionBinder::bindVariableExpression(
    const std::string& varName) const {
  if (binder->scope.contains(varName)) {
    return binder->scope.getExpression(varName);
  }
  THROW_BINDER_EXCEPTION(ExceptionMessage::variableNotInScope(varName));
}

std::shared_ptr<Expression> ExpressionBinder::createVariableExpression(
    common::LogicalType logicalType, std::string_view name) const {
  return createVariableExpression(std::move(logicalType), std::string(name));
}

std::shared_ptr<Expression> ExpressionBinder::createVariableExpression(
    LogicalType logicalType, std::string name) const {
  return std::make_shared<VariableExpression>(
      std::move(logicalType), binder->getUniqueExpressionName(name),
      std::move(name));
}

}  // namespace binder
}  // namespace neug
