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

#include <optional>
#include <vector>
#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression/variable_expression.h"
#include "neug/compiler/binder/query/normalized_query_part.h"
#include "neug/compiler/binder/query/return_with_clause/bound_return_clause.h"
#include "neug/compiler/binder/query/return_with_clause/bound_with_clause.h"
#include "neug/compiler/common/enums/expression_type.h"
#include "neug/compiler/parser/query/reading_clause/yield_variable.h"
#include "neug/compiler/parser/query/regular_query.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;
using namespace neug::parser;

namespace neug {
namespace binder {

void validateUnionColumnsOfTheSameType(
    const std::vector<NormalizedSingleQuery>& normalizedSingleQueries) {
  if (normalizedSingleQueries.size() <= 1) {
    return;
  }
  auto columns = normalizedSingleQueries[0].getStatementResult()->getColumns();
  for (auto i = 1u; i < normalizedSingleQueries.size(); i++) {
    auto otherColumns =
        normalizedSingleQueries[i].getStatementResult()->getColumns();
    if (columns.size() != otherColumns.size()) {
      THROW_BINDER_EXCEPTION(
          "The number of columns to union/union all must be the same.");
    }
    // Check whether the dataTypes in union expressions are exactly the same in
    // each single query.
    for (auto j = 0u; j < columns.size(); j++) {
      ExpressionUtil::validateDataType(*otherColumns[j],
                                       columns[j]->getDataType());
    }
  }
}

void validateIsAllUnionOrUnionAll(const BoundRegularQuery& regularQuery) {
  auto unionAllExpressionCounter = 0u;
  for (auto i = 0u; i < regularQuery.getNumSingleQueries() - 1; i++) {
    unionAllExpressionCounter += regularQuery.getIsUnionAll(i);
  }
  if ((0 < unionAllExpressionCounter) &&
      (unionAllExpressionCounter < regularQuery.getNumSingleQueries() - 1)) {
    THROW_BINDER_EXCEPTION("Union and union all can not be used together.");
  }
}

std::unique_ptr<BoundRegularQuery> Binder::bindQuery(
    const Statement& statement) {
  auto& regularQuery = statement.constCast<RegularQuery>();

  // bind pre query before the union
  std::vector<NormalizedQueryPart> preQueryPart;
  for (auto& part : regularQuery.getPreQueryPart()) {
    preQueryPart.emplace_back(std::move(bindQueryPart(part)));
  }

  // bind common expressions in pre query
  binder::expression_vector preQueryExprs;
  if (!preQueryPart.empty() && !regularQuery.getPreQueryExprs().empty()) {
    for (const auto& expr : regularQuery.getPreQueryExprs()) {
      preQueryExprs.emplace_back(expressionBinder.bindExpression(*expr));
    }
  }

  // record the common expressions in scope, each subquery in union need to bind
  // them later.
  auto commonExprMap = scope.getNameToExprMap();

  std::vector<NormalizedSingleQuery> normalizedSingleQueries;
  for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
    scope.clear();
    if (!preQueryPart.empty() && !preQueryExprs.empty()) {
      // each subquery in union need to bind the common expressions from scope.
      for (auto pair : commonExprMap) {
        auto expr = pair.second;
        if (expr->expressionType == ExpressionType::PATTERN) {
          addToScope(pair.first, expr);
        } else {
          // if the expression is not a pattern, convert it to a variable
          // expression
          auto exprVar = std::make_shared<VariableExpression>(
              expr->getDataType().copy(), expr->getUniqueName(),
              expr->getAlias());
          exprVar->setAlias(expr->getAlias());
          addToScope(pair.first, exprVar);
        }
      }
    }
    // Don't clear scope within bindSingleQuery() yet because it is also used
    // for subquery binding.
    auto singleQuery = bindSingleQuery(*regularQuery.getSingleQuery(i));
    normalizedSingleQueries.push_back(std::move(singleQuery));
  }
  validateUnionColumnsOfTheSameType(normalizedSingleQueries);
  NEUG_ASSERT(!normalizedSingleQueries.empty());
  auto boundRegularQuery = std::make_unique<BoundRegularQuery>(
      regularQuery.getIsUnionAll(),
      normalizedSingleQueries[0].getStatementResult()->copy());
  if (!preQueryPart.empty()) {
    boundRegularQuery->setPreQueryPart(std::move(preQueryPart));
  }
  if (!preQueryExprs.empty()) {
    boundRegularQuery->setPreQueryExprs(std::move(preQueryExprs));
  }
  for (auto& normalizedSingleQuery : normalizedSingleQueries) {
    boundRegularQuery->addSingleQuery(std::move(normalizedSingleQuery));
  }
  validateIsAllUnionOrUnionAll(*boundRegularQuery);

  if (regularQuery.getPostSingleQuery()) {
    scope.clear();
    if (boundRegularQuery->getNumSingleQueries() > 0) {
      // since all subqueries have the same schema, we can get output columns of
      // union from the first subquery directly.
      auto subQuery = boundRegularQuery->getSingleQueryUnsafe(0);
      auto statementResult = subQuery->getStatementResult();
      for (size_t pos = 0; pos < statementResult->getColumns().size(); pos++) {
        auto column = statementResult->getColumns()[pos];
        auto columnName = statementResult->getColumnNames()[pos];
        if (column->expressionType == ExpressionType::PATTERN) {
          addToScope(columnName, column);
        } else {
          auto exprVar = std::make_shared<VariableExpression>(
              column->getDataType().copy(), column->getUniqueName(),
              column->getAlias());
          exprVar->setAlias(column->getAlias());
          addToScope(columnName, exprVar);
        }
      }
    }
    boundRegularQuery->setPostSingleQuery(
        bindSingleQuery(*regularQuery.getPostSingleQuery()));
  }
  return boundRegularQuery;
}

NormalizedSingleQuery Binder::bindSingleQuery(const SingleQuery& singleQuery) {
  auto normalizedSingleQuery = NormalizedSingleQuery();
  for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
    normalizedSingleQuery.appendQueryPart(
        bindQueryPart(*singleQuery.getQueryPart(i)));
  }
  auto lastQueryPart = NormalizedQueryPart();
  for (auto i = 0u; i < singleQuery.getNumReadingClauses(); i++) {
    lastQueryPart.addReadingClause(
        bindReadingClause(*singleQuery.getReadingClause(i)));
  }
  for (auto i = 0u; i < singleQuery.getNumUpdatingClauses(); ++i) {
    lastQueryPart.addUpdatingClause(
        bindUpdatingClause(*singleQuery.getUpdatingClause(i)));
  }
  auto statementResult = BoundStatementResult();
  if (singleQuery.hasReturnClause()) {
    auto boundReturnClause = bindReturnClause(*singleQuery.getReturnClause());
    lastQueryPart.setProjectionBody(
        boundReturnClause.getProjectionBody()->copy());
    statementResult = boundReturnClause.getStatementResult()->copy();
  } else {
    statementResult = BoundStatementResult::createEmptyResult();
  }
  normalizedSingleQuery.appendQueryPart(std::move(lastQueryPart));
  normalizedSingleQuery.setStatementResult(std::move(statementResult));
  return normalizedSingleQuery;
}

NormalizedQueryPart Binder::bindQueryPart(const QueryPart& queryPart) {
  auto normalizedQueryPart = NormalizedQueryPart();
  for (auto i = 0u; i < queryPart.getNumReadingClauses(); i++) {
    normalizedQueryPart.addReadingClause(
        bindReadingClause(*queryPart.getReadingClause(i)));
  }
  for (auto i = 0u; i < queryPart.getNumUpdatingClauses(); ++i) {
    normalizedQueryPart.addUpdatingClause(
        bindUpdatingClause(*queryPart.getUpdatingClause(i)));
  }
  auto boundWithClause = bindWithClause(*queryPart.getWithClause());
  normalizedQueryPart.setProjectionBody(
      boundWithClause.getProjectionBody()->copy());
  if (boundWithClause.hasWhereExpression()) {
    normalizedQueryPart.setProjectionBodyPredicate(
        boundWithClause.getWhereExpression());
  }
  return normalizedQueryPart;
}

}  // namespace binder
}  // namespace neug
