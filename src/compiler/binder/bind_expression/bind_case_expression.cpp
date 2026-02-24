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
#include "neug/compiler/binder/expression/case_expression.h"
#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression_binder.h"
#include "neug/compiler/parser/expression/parsed_case_expression.h"

using namespace neug::common;
using namespace neug::parser;

namespace neug {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindCaseExpression(
    const ParsedExpression& parsedExpression) {
  auto& parsedCaseExpression =
      parsedExpression.constCast<ParsedCaseExpression>();
  auto resultType = LogicalType::ANY();
  // Resolve result type by checking each then expression type.
  for (auto i = 0u; i < parsedCaseExpression.getNumCaseAlternative(); ++i) {
    auto alternative = parsedCaseExpression.getCaseAlternative(i);
    auto boundThen = bindExpression(*alternative->thenExpression);
    if (boundThen->getDataType().getLogicalTypeID() != LogicalTypeID::ANY) {
      resultType = boundThen->getDataType().copy();
    }
  }
  // Resolve result type by else expression if above resolving fails.
  if (resultType.getLogicalTypeID() == LogicalTypeID::ANY &&
      parsedCaseExpression.hasElseExpression()) {
    auto elseExpression =
        bindExpression(*parsedCaseExpression.getElseExpression());
    resultType = elseExpression->getDataType().copy();
  }
  auto name = binder->getUniqueExpressionName(parsedExpression.getRawName());
  // bind ELSE ...
  std::shared_ptr<Expression> elseExpression;
  if (parsedCaseExpression.hasElseExpression()) {
    elseExpression = bindExpression(*parsedCaseExpression.getElseExpression());
  } else {
    elseExpression = createNullLiteralExpression();
  }
  elseExpression = implicitCastIfNecessary(elseExpression, resultType);
  auto boundCaseExpression = make_shared<CaseExpression>(
      resultType.copy(), std::move(elseExpression), name);
  // bind WHEN ... THEN ...
  if (parsedCaseExpression.hasCaseExpression()) {
    auto boundCase = bindExpression(*parsedCaseExpression.getCaseExpression());
    for (auto i = 0u; i < parsedCaseExpression.getNumCaseAlternative(); ++i) {
      auto caseAlternative = parsedCaseExpression.getCaseAlternative(i);
      auto boundWhen = bindExpression(*caseAlternative->whenExpression);
      boundWhen = implicitCastIfNecessary(boundWhen, boundCase->dataType);
      // rewrite "CASE a.age WHEN 1" as "CASE WHEN a.age = 1"
      if (ExpressionUtil::isNullLiteral(*boundWhen)) {
        boundWhen = bindNullOperatorExpression(ExpressionType::IS_NULL,
                                               expression_vector{boundWhen});
      } else {
        boundWhen = bindComparisonExpression(
            ExpressionType::EQUALS, expression_vector{boundCase, boundWhen});
      }
      auto boundThen = bindExpression(*caseAlternative->thenExpression);
      boundThen = implicitCastIfNecessary(boundThen, resultType);
      boundCaseExpression->addCaseAlternative(boundWhen, boundThen);
    }
  } else {
    for (auto i = 0u; i < parsedCaseExpression.getNumCaseAlternative(); ++i) {
      auto caseAlternative = parsedCaseExpression.getCaseAlternative(i);
      auto boundWhen = bindExpression(*caseAlternative->whenExpression);
      boundWhen = implicitCastIfNecessary(boundWhen, LogicalType::BOOL());
      auto boundThen = bindExpression(*caseAlternative->thenExpression);
      boundThen = implicitCastIfNecessary(boundThen, resultType);
      boundCaseExpression->addCaseAlternative(boundWhen, boundThen);
    }
  }
  return boundCaseExpression;
}

}  // namespace binder
}  // namespace neug
