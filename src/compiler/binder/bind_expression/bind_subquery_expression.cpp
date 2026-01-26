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
#include "neug/compiler/binder/expression/aggregate_function_expression.h"
#include "neug/compiler/binder/expression/subquery_expression.h"
#include "neug/compiler/binder/expression_binder.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/function/aggregate/count_star.h"
#include "neug/compiler/function/built_in_function_utils.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/expression/parsed_subquery_expression.h"

using namespace neug::parser;
using namespace neug::common;
using namespace neug::function;

namespace neug {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindSubqueryExpression(
    const ParsedExpression& parsedExpr) {
  auto& subqueryExpr =
      neug_dynamic_cast<const ParsedSubqueryExpression&>(parsedExpr);
  auto prevScope = binder->saveScope();
  auto boundGraphPattern =
      binder->bindGraphPattern(subqueryExpr.getPatternElements());
  if (subqueryExpr.hasWhereClause()) {
    boundGraphPattern.where =
        binder->bindWhereExpression(*subqueryExpr.getWhereClause());
  }
  binder->rewriteMatchPattern(boundGraphPattern);
  auto subqueryType = subqueryExpr.getSubqueryType();
  auto dataType = subqueryType == SubqueryType::COUNT ? LogicalType::INT64()
                                                      : LogicalType::BOOL();
  auto rawName = subqueryExpr.getRawName();
  auto uniqueName = binder->getUniqueExpressionName(rawName);
  auto boundSubqueryExpr = make_shared<SubqueryExpression>(
      subqueryType, std::move(dataType),
      std::move(boundGraphPattern.queryGraphCollection), uniqueName,
      std::move(rawName));
  boundSubqueryExpr->setWhereExpression(boundGraphPattern.where);
  // Bind projection
  auto entry = context->getCatalog()->getFunctionEntry(
      context->getTransaction(), CountStarFunction::name);
  auto function = BuiltInFunctionsUtils::matchAggregateFunction(
      CountStarFunction::name, std::vector<LogicalType>{}, false,
      entry->ptrCast<catalog::FunctionCatalogEntry>());
  auto bindData =
      std::make_unique<FunctionBindData>(LogicalType(function->returnTypeID));
  auto countStarExpr = std::make_shared<AggregateFunctionExpression>(
      function->copy(), std::move(bindData), expression_vector{},
      binder->getUniqueExpressionName(CountStarFunction::name));
  boundSubqueryExpr->setCountStarExpr(countStarExpr);
  std::shared_ptr<Expression> projectionExpr;
  switch (subqueryType) {
  case SubqueryType::COUNT: {
    // Rewrite COUNT subquery as COUNT(*)
    projectionExpr = countStarExpr;
  } break;
  case SubqueryType::EXISTS: {
    // Rewrite EXISTS subquery as COUNT(*) > 0
    auto literalExpr = createLiteralExpression(Value(static_cast<int64_t>(0)));
    projectionExpr =
        bindComparisonExpression(ExpressionType::GREATER_THAN,
                                 expression_vector{countStarExpr, literalExpr});
  } break;
  default:
    NEUG_UNREACHABLE;
  }
  // Use the same unique identifier for projection & subquery expression. We
  // will replace subquery expression with projection expression during
  // processing.
  projectionExpr->setUniqueName(uniqueName);
  boundSubqueryExpr->setProjectionExpr(projectionExpr);
  if (subqueryExpr.hasHint()) {
    boundSubqueryExpr->setHint(
        binder->bindJoinHint(*boundSubqueryExpr->getQueryGraphCollection(),
                             *subqueryExpr.getHint()));
  }
  binder->restoreScope(std::move(prevScope));
  return boundSubqueryExpr;
}

}  // namespace binder
}  // namespace neug
