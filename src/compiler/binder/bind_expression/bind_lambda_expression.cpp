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
#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression/lambda_expression.h"
#include "neug/compiler/parser/expression/parsed_lambda_expression.h"

using namespace neug::common;
using namespace neug::parser;

namespace neug {
namespace binder {

void ExpressionBinder::bindLambdaExpression(const Expression& lambdaInput,
                                            Expression& lambdaExpr) const {
  ExpressionUtil::validateDataType(lambdaInput, LogicalTypeID::LIST);
  auto& listChildType = neug::common::ListType::getChildType(lambdaInput.getDataType());
  auto& boundLambdaExpr = lambdaExpr.cast<LambdaExpression>();
  auto& parsedLambdaExpr = boundLambdaExpr.getParsedLambdaExpr()
                               ->constCast<ParsedLambdaExpression>();
  auto prevScope = binder->saveScope();
  for (auto& varName : parsedLambdaExpr.getVarNames()) {
    binder->createVariable(varName, listChildType);
  }
  auto funcExpr = binder->getExpressionBinder()->bindExpression(
      *parsedLambdaExpr.getFunctionExpr());
  binder->restoreScope(std::move(prevScope));
  boundLambdaExpr.cast(funcExpr->getDataType().copy());
  boundLambdaExpr.setFunctionExpr(std::move(funcExpr));
}

std::shared_ptr<Expression> ExpressionBinder::bindLambdaExpression(
    const parser::ParsedExpression& parsedExpr) const {
  auto uniqueName = getUniqueName(parsedExpr.getRawName());
  return std::make_shared<LambdaExpression>(parsedExpr.copy(), uniqueName);
}

}  // namespace binder
}  // namespace neug
