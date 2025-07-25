#include "neug/compiler/binder/expression_mapper.h"

#include "neug/compiler/binder/expression/literal_expression.h"
#include "neug/compiler/binder/expression_visitor.h"  // IWYU pragma: keep (used in assert)
#include "neug/compiler/binder/function_evaluator.h"
#include "neug/compiler/binder/literal_evaluator.h"
#include "neug/compiler/common/exception/not_implemented.h"
#include "neug/compiler/common/string_format.h"

using namespace gs::binder;
using namespace gs::common;
using namespace gs::evaluator;
using namespace gs::planner;

namespace gs {
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
    throw NotImplementedException(
        stringFormat("Cannot evaluate expression with type {}.",
                     ExpressionTypeUtil::toString(expressionType)));
    // LCOV_EXCL_STOP
  }
}

std::unique_ptr<ExpressionEvaluator> ExpressionMapper::getConstantEvaluator(
    std::shared_ptr<Expression> expression) {
  KU_ASSERT(ConstantExpressionVisitor::isConstant(*expression));
  auto expressionType = expression->expressionType;
  if (ExpressionType::LITERAL == expressionType) {
    return getLiteralEvaluator(std::move(expression));
  } else if (canEvaluateAsFunction(expressionType)) {
    return getFunctionEvaluator(std::move(expression));
  } else {
    // LCOV_EXCL_START
    throw NotImplementedException(
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
}  // namespace gs