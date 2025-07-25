#include "neug/compiler/binder/expression/parameter_expression.h"
#include "neug/compiler/binder/expression_binder.h"
#include "neug/compiler/parser/expression/parsed_parameter_expression.h"

using namespace gs::common;
using namespace gs::parser;

namespace gs {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindParameterExpression(
    const ParsedExpression& parsedExpression) {
  auto& parsedParameterExpression =
      parsedExpression.constCast<ParsedParameterExpression>();
  auto parameterName = parsedParameterExpression.getParameterName();
  if (parameterMap.contains(parameterName)) {
    return make_shared<ParameterExpression>(parameterName,
                                            *parameterMap.at(parameterName));
  } else {
    auto value = std::make_shared<Value>(Value::createNullValue());
    parameterMap.insert({parameterName, value});
    return std::make_shared<ParameterExpression>(parameterName, *value);
  }
}

}  // namespace binder
}  // namespace gs
