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

#include "neug/compiler/binder/rewriter/normalized_query_part_match_rewriter.h"

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/query/reading_clause/bound_match_clause.h"

using namespace neug::common;

namespace neug {
namespace binder {

static bool canRewrite(const BoundMatchClause& matchClause) {
  return !matchClause.hasHint() &&
         matchClause.getMatchClauseType() != MatchClauseType::OPTIONAL_MATCH;
}

void NormalizedQueryPartMatchRewriter::visitQueryPartUnsafe(
    NormalizedQueryPart& queryPart) {
  if (queryPart.getNumReadingClause() == 0) {
    return;
  }
  for (auto i = 0u; i < queryPart.getNumReadingClause(); i++) {
    if (queryPart.getReadingClause(i)->getClauseType() != ClauseType::MATCH) {
      return;
    }
    auto& match = queryPart.getReadingClause(i)->constCast<BoundMatchClause>();
    if (!canRewrite(match)) {
      return;
    }
  }
  // Merge consecutive match clauses
  std::vector<std::unique_ptr<BoundReadingClause>> newReadingClauses;
  newReadingClauses.push_back(std::move(queryPart.readingClauses[0]));
  auto& leadingMatchClause = newReadingClauses[0]->cast<BoundMatchClause>();
  auto binder = Binder(clientContext);
  auto expressionBinder = binder.getExpressionBinder();
  for (auto idx = 1u; idx < queryPart.getNumReadingClause(); idx++) {
    auto& otherMatchClause =
        queryPart.readingClauses[idx]->constCast<BoundMatchClause>();
    leadingMatchClause.getQueryGraphCollectionUnsafe()->merge(
        *otherMatchClause.getQueryGraphCollection());
    auto predicate = expressionBinder->combineBooleanExpressions(
        ExpressionType::AND, leadingMatchClause.getPredicate(),
        otherMatchClause.getPredicate());
    leadingMatchClause.setPredicate(std::move(predicate));
  }
  // Move remaining reading clause
  queryPart.readingClauses = std::move(newReadingClauses);
}

}  // namespace binder
}  // namespace neug
