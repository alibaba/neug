#include "binder/expression_binder.h"

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/parameter_expression.h"
#include "binder/expression_visitor.h"
#include "common/exception/binder.h"
#include "common/exception/not_implemented.h"
#include "common/string_format.h"
#include "function/cast/vector_cast_functions.h"
#include "main/client_context.h"
#include "parser/expression/parsed_expression_visitor.h"
#include "parser/expression/parsed_parameter_expression.h"

using namespace gs::common;
using namespace gs::function;
using namespace gs::parser;

namespace gs {
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
    if (!allParamExist) {
      auto expr = std::make_shared<ParameterExpression>(
          binder->getUniqueExpressionName(""), Value::createNullValue());
      if (parsedExpression.hasAlias()) {
        expr->setAlias(parsedExpression.getAlias());
      }
      return expr;
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
    throw NotImplementedException("bindExpression(" +
                                  ExpressionTypeUtil::toString(expressionType) +
                                  ").");
  }
  if (ConstantExpressionVisitor::needFold(*expression)) {
    return foldExpression(expression);
  }
  return expression;
}

std::shared_ptr<Expression> ExpressionBinder::foldExpression(
    const std::shared_ptr<Expression>& expression) const {
  return nullptr;
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
    throw BinderException(
        unsupportedImplicitCastException(*expression, targetType.toString()));
  }
}

std::shared_ptr<Expression> ExpressionBinder::forceCast(
    const std::shared_ptr<Expression>& expression,
    const LogicalType& targetType) {
  auto functionName = "CAST";
  auto children = expression_vector{
      expression, createLiteralExpression(Value(targetType.toString()))};
  return bindScalarFunctionExpression(children, functionName);
}

std::string ExpressionBinder::getUniqueName(const std::string& name) const {
  return binder->getUniqueExpressionName(name);
}

}  // namespace binder
}  // namespace gs
