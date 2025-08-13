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
#include "neug/compiler/binder/query/return_with_clause/bound_return_clause.h"
#include "neug/compiler/binder/query/return_with_clause/bound_with_clause.h"
#include "neug/compiler/parser/query/regular_query.h"
#include "neug/utils/exception/exception.h"

using namespace gs::common;
using namespace gs::parser;

namespace gs {
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
  std::vector<NormalizedSingleQuery> normalizedSingleQueries;
  for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
    // Don't clear scope within bindSingleQuery() yet because it is also used
    // for subquery binding.
    scope.clear();
    normalizedSingleQueries.push_back(
        bindSingleQuery(*regularQuery.getSingleQuery(i)));
  }
  validateUnionColumnsOfTheSameType(normalizedSingleQueries);
  KU_ASSERT(!normalizedSingleQueries.empty());
  auto boundRegularQuery = std::make_unique<BoundRegularQuery>(
      regularQuery.getIsUnionAll(),
      normalizedSingleQueries[0].getStatementResult()->copy());
  for (auto& normalizedSingleQuery : normalizedSingleQueries) {
    boundRegularQuery->addSingleQuery(std::move(normalizedSingleQuery));
  }
  validateIsAllUnionOrUnionAll(*boundRegularQuery);
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
}  // namespace gs
