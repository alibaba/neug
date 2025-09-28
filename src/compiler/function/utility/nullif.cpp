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

#include "neug/compiler/binder/expression/case_expression.h"
#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/binder/expression_binder.h"
#include "neug/compiler/function/rewrite_function.h"
#include "neug/compiler/function/utility/vector_utility_functions.h"

using namespace gs::binder;
using namespace gs::common;

namespace gs {
namespace function {

static std::shared_ptr<Expression> rewriteFunc(
    const RewriteFunctionBindInput& input) {
  NEUG_ASSERT(input.arguments.size() == 2);
  auto uniqueExpressionName = ScalarFunctionExpression::getUniqueName(
      NullIfFunction::name, input.arguments);
  const auto& resultType = input.arguments[0]->getDataType();
  auto caseExpression = std::make_shared<CaseExpression>(
      resultType.copy(), input.arguments[0], uniqueExpressionName);
  auto binder = input.expressionBinder;
  auto whenExpression =
      binder->bindComparisonExpression(ExpressionType::EQUALS, input.arguments);
  auto thenExpression = binder->createNullLiteralExpression();
  thenExpression =
      binder->implicitCastIfNecessary(thenExpression, resultType.copy());
  caseExpression->addCaseAlternative(whenExpression, thenExpression);
  return caseExpression;
}

function_set NullIfFunction::getFunctionSet() {
  function_set functionSet;
  for (auto typeID : LogicalTypeUtils::getAllValidLogicTypeIDs()) {
    functionSet.push_back(std::make_unique<RewriteFunction>(
        name, std::vector<LogicalTypeID>{typeID, typeID}, rewriteFunc));
  }
  return functionSet;
}

}  // namespace function
}  // namespace gs
