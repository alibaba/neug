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

#include "neug/compiler/binder/rewriter/match_clause_pattern_label_rewriter.h"

#include "neug/compiler/binder/query/bound_regular_query.h"

using namespace neug::common;

namespace neug {
namespace binder {

void MatchClausePatternLabelRewriter::visitMatchUnsafe(
    BoundReadingClause& readingClause) {
  auto& matchClause = readingClause.cast<BoundMatchClause>();
  bool skipNodes =
      matchClause.getMatchClauseType() == MatchClauseType::OPTIONAL_MATCH;
  auto collection = matchClause.getQueryGraphCollectionUnsafe();
  for (auto i = 0u; i < collection->getNumQueryGraphs(); ++i) {
    analyzer.pruneLabel(*collection->getQueryGraphUnsafe(i), skipNodes);
  }
}

void MatchClausePatternLabelRewriter::visitRegularQueryUnsafe(
    BoundStatement& statement) {
  auto& regularQuery = common::neug_dynamic_cast<BoundRegularQuery&>(statement);
  auto preQueryPart = regularQuery.getPreQueryPartUnsafe();
  for (auto part : preQueryPart) {
    visitQueryPartUnsafe(*part);
  }
  for (auto i = 0u; i < regularQuery.getNumSingleQueries(); ++i) {
    visitSingleQueryUnsafe(*regularQuery.getSingleQueryUnsafe(i));
  }
  auto postSingleQuery = regularQuery.getPostSingleQueryUnsafe();
  if (postSingleQuery) {
    visitSingleQueryUnsafe(*postSingleQuery);
  }
}

}  // namespace binder
}  // namespace neug
