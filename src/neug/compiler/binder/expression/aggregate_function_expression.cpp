#include "neug/compiler/binder/expression/aggregate_function_expression.h"

#include "neug/compiler/binder/expression/expression_util.h"

using namespace gs::common;

namespace gs {
namespace binder {

std::string AggregateFunctionExpression::toStringInternal() const {
  return stringFormat("{}({}{})", function.name,
                      function.isDistinct ? "DISTINCT " : "",
                      ExpressionUtil::toString(children));
}

std::string AggregateFunctionExpression::getUniqueName(
    const std::string& functionName, const expression_vector& children,
    bool isDistinct) {
  return stringFormat("{}({}{})", functionName, isDistinct ? "DISTINCT " : "",
                      ExpressionUtil::getUniqueName(children));
}

}  // namespace binder
}  // namespace gs
