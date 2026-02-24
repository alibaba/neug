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

#include "neug/compiler/binder/expression_binder.h"

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression/parameter_expression.h"
#include "neug/compiler/binder/expression_evaluator_utils.h"
#include "neug/compiler/binder/expression_visitor.h"
#include "neug/compiler/common/string_format.h"
#include "neug/compiler/function/cast/vector_cast_functions.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/expression/parsed_expression_visitor.h"
#include "neug/compiler/parser/expression/parsed_parameter_expression.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;
using namespace neug::function;
using namespace neug::parser;

namespace neug {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindExpression(
    const parser::ParsedExpression& parsedExpression) {
  if (config.bindOrderByAfterAggregate &&
      binder->scope.contains(parsedExpression.toString())) {
    return binder->scope.getExpression(parsedExpression.toString());
  }
  auto collector = ParsedParamExprCollector();
  collector.visit(&parsedExpression);
  if (collector.hasParamExprs()) {
    bool allParamExist = true;
    for (auto& parsedExpr : collector.getParamExprs()) {
      auto name =
          parsedExpr->constCast<ParsedParameterExpression>().getParameterName();
      if (!parameterMap.contains(name)) {
        auto value = std::make_shared<Value>(Value::createNullValue());
        parameterMap.insert({name, value});
        allParamExist = false;
      }
    }
  }
  std::shared_ptr<Expression> expression;
  auto expressionType = parsedExpression.getExpressionType();
  if (ExpressionTypeUtil::isBoolean(expressionType)) {
    expression = bindBooleanExpression(parsedExpression);
  } else if (ExpressionTypeUtil::isComparison(expressionType)) {
    expression = bindComparisonExpression(parsedExpression);
  } else if (ExpressionTypeUtil::isNullOperator(expressionType)) {
    expression = bindNullOperatorExpression(parsedExpression);
  } else if (ExpressionType::FUNCTION == expressionType) {
    expression = bindFunctionExpression(parsedExpression);
  } else if (ExpressionType::PROPERTY == expressionType) {
    expression = bindPropertyExpression(parsedExpression);
  } else if (ExpressionType::PARAMETER == expressionType) {
    expression = bindParameterExpression(parsedExpression);
  } else if (ExpressionType::LITERAL == expressionType) {
    expression = bindLiteralExpression(parsedExpression);
  } else if (ExpressionType::VARIABLE == expressionType) {
    expression = bindVariableExpression(parsedExpression);
  } else if (ExpressionType::SUBQUERY == expressionType) {
    expression = bindSubqueryExpression(parsedExpression);
  } else if (ExpressionType::CASE_ELSE == expressionType) {
    expression = bindCaseExpression(parsedExpression);
  } else if (ExpressionType::LAMBDA == expressionType) {
    expression = bindLambdaExpression(parsedExpression);
  } else {
    THROW_NOT_IMPLEMENTED_EXCEPTION(
        "bindExpression(" + ExpressionTypeUtil::toString(expressionType) +
        ").");
  }
  if (ConstantExpressionVisitor::needFold(*expression)) {
    return foldExpression(expression);
  }
  return expression;
}

std::shared_ptr<Expression> ExpressionBinder::foldExpression(
    const std::shared_ptr<Expression>& expression) const {
  auto value = evaluator::ExpressionEvaluatorUtils::evaluateConstantExpression(
      expression, context);
  auto result = createLiteralExpression(value);
  // Fold result should preserve the alias original expression. E.g.
  // RETURN 2, 1 + 1 AS x
  // Once folded, 1 + 1 will become 2 and have the same identifier as the first
  // RETURN element. We preserve alias (x) to avoid such conflict.
  if (expression->hasAlias()) {
    result->setAlias(expression->getAlias());
  } else {
    result->setAlias(expression->toString());
  }
  return result;
}

static std::string unsupportedImplicitCastException(
    const Expression& expression, const std::string& targetTypeStr) {
  return stringFormat(
      "Expression {} has data type {} but expected {}. Implicit cast is not "
      "supported.",
      expression.toString(), expression.dataType.toString(), targetTypeStr);
}

static bool checkUDTCast(const LogicalType& type, const LogicalType& target) {
  if (type.isInternalType() && target.isInternalType()) {
    return false;
  }
  return type.getLogicalTypeID() == target.getLogicalTypeID();
}

std::shared_ptr<Expression> ExpressionBinder::implicitCastIfNecessary(
    const std::shared_ptr<Expression>& expression,
    const LogicalType& targetType) {
  auto& type = expression->dataType;
  if (checkUDTCast(type, targetType)) {
    return expression;
  }
  if (type == targetType || targetType.containsAny()) {
    return expression;
  }
  if (ExpressionUtil::canCastStatically(*expression, targetType)) {
    expression->cast(targetType);
    return expression;
  }
  return implicitCast(expression, targetType);
}

std::shared_ptr<Expression> ExpressionBinder::implicitCast(
    const std::shared_ptr<Expression>& expression,
    const LogicalType& targetType) {
  if (CastFunction::hasImplicitCast(expression->dataType, targetType)) {
    return forceCast(expression, targetType);
  } else {
    THROW_CONVERSION_EXCEPTION(
        unsupportedImplicitCastException(*expression, targetType.toString()));
  }
}

std::shared_ptr<Expression> ExpressionBinder::forceCast(
    const std::shared_ptr<Expression>& expression,
    const LogicalType& targetType) {
  auto functionName = "CAST";
  // we suppose INTERNAL_ID can be converted from other types without cast
  if (targetType == common::LogicalType::INTERNAL_ID()) {
    return expression;
  }
  auto children = expression_vector{
      expression, createLiteralExpression(Value(targetType.toString()))};
  return bindScalarFunctionExpression(children, functionName);
}

std::string ExpressionBinder::getUniqueName(const std::string& name) const {
  return binder->getUniqueExpressionName(name);
}

}  // namespace binder
}  // namespace neug
