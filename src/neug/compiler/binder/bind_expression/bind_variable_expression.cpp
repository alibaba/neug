#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/expression/variable_expression.h"
#include "neug/compiler/binder/expression_binder.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/expression/parsed_variable_expression.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/exception/message.h"

using namespace gs::common;
using namespace gs::parser;

namespace gs {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindVariableExpression(
    const ParsedExpression& parsedExpression) const {
  auto& variableExpression =
      ku_dynamic_cast<const ParsedVariableExpression&>(parsedExpression);
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
}  // namespace gs
