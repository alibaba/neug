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

#include "neug/compiler/binder/expression_mapper.h"

#include "neug/compiler/binder/expression/literal_expression.h"
#include "neug/compiler/binder/expression_visitor.h"  // IWYU pragma: keep (used in assert)
#include "neug/compiler/binder/function_evaluator.h"
#include "neug/compiler/binder/literal_evaluator.h"
#include "neug/compiler/common/string_format.h"
#include "neug/utils/exception/exception.h"

using namespace neug::binder;
using namespace neug::common;
using namespace neug::evaluator;
using namespace neug::planner;

namespace neug {
namespace processor {

static bool canEvaluateAsFunction(ExpressionType expressionType) {
  switch (expressionType) {
  case ExpressionType::OR:
  case ExpressionType::XOR:
  case ExpressionType::AND:
  case ExpressionType::NOT:
  case ExpressionType::EQUALS:
  case ExpressionType::NOT_EQUALS:
  case ExpressionType::GREATER_THAN:
  case ExpressionType::GREATER_THAN_EQUALS:
  case ExpressionType::LESS_THAN:
  case ExpressionType::LESS_THAN_EQUALS:
  case ExpressionType::IS_NULL:
  case ExpressionType::IS_NOT_NULL:
  case ExpressionType::FUNCTION:
    return true;
  default:
    return false;
  }
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getEvaluator(
    std::shared_ptr<Expression> expression) {
  if (schema == nullptr) {
    return getConstantEvaluator(std::move(expression));
  }
  auto expressionType = expression->expressionType;

  if (ExpressionType::LITERAL == expressionType) {
    return getLiteralEvaluator(std::move(expression));
  } else if (canEvaluateAsFunction(expressionType)) {
    return getFunctionEvaluator(std::move(expression));
  }

  else {
    // LCOV_EXCL_START
    THROW_NOT_IMPLEMENTED_EXCEPTION(
        stringFormat("Cannot evaluate expression with type {}.",
                     ExpressionTypeUtil::toString(expressionType)));
    // LCOV_EXCL_STOP
  }
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getConstantEvaluator(
    std::shared_ptr<Expression> expression) {
  NEUG_ASSERT(ConstantExpressionVisitor::isConstant(*expression));
  auto expressionType = expression->expressionType;
  if (ExpressionType::LITERAL == expressionType) {
    return getLiteralEvaluator(std::move(expression));
  } else if (canEvaluateAsFunction(expressionType)) {
    return getFunctionEvaluator(std::move(expression));
  } else {
    // LCOV_EXCL_START
    THROW_NOT_IMPLEMENTED_EXCEPTION(
        stringFormat("Cannot evaluate expression with type {}.",
                     ExpressionTypeUtil::toString(expressionType)));
    // LCOV_EXCL_STOP
  }
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getLiteralEvaluator(
    std::shared_ptr<Expression> expression) {
  auto& literalExpression = expression->constCast<LiteralExpression>();
  return std::make_unique<LiteralExpressionEvaluator>(
      std::move(expression), literalExpression.getValue());
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getFunctionEvaluator(
    std::shared_ptr<Expression> expression) {
  evaluator_vector_t childrenEvaluators;
  childrenEvaluators = getEvaluators(expression->getChildren());
  return std::make_unique<FunctionExpressionEvaluator>(
      std::move(expression), std::move(childrenEvaluators));
}

std::vector<std::unique_ptr<ExpressionEvaluator>>
ExpressionMapper::getEvaluators(const expression_vector& expressions) {
  std::vector<std::unique_ptr<ExpressionEvaluator>> evaluators;
  evaluators.reserve(expressions.size());
  for (auto& expression : expressions) {
    evaluators.push_back(getEvaluator(expression));
  }
  return evaluators;
}

}  // namespace processor
}  // namespace neug