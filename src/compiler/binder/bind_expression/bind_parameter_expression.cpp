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

#include "neug/compiler/binder/expression/parameter_expression.h"
#include "neug/compiler/binder/expression_binder.h"
#include "neug/compiler/parser/expression/parsed_parameter_expression.h"

using namespace neug::common;
using namespace neug::parser;

namespace neug {
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
}  // namespace neug
