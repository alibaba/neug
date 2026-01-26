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

#include "neug/compiler/binder/bound_statement_rewriter.h"

#include "neug/compiler/binder/rewriter/match_clause_pattern_label_rewriter.h"
#include "neug/compiler/binder/rewriter/normalized_query_part_match_rewriter.h"
#include "neug/compiler/binder/rewriter/with_clause_projection_rewriter.h"
#include "neug/compiler/binder/visitor/default_type_solver.h"

namespace neug {
namespace binder {

void BoundStatementRewriter::rewrite(BoundStatement& boundStatement,
                                     main::ClientContext& clientContext) {
  //   auto withClauseProjectionRewriter = WithClauseProjectionRewriter();
  //   withClauseProjectionRewriter.visitUnsafe(boundStatement);

  auto normalizedQueryPartMatchRewriter =
      NormalizedQueryPartMatchRewriter(&clientContext);
  normalizedQueryPartMatchRewriter.visitUnsafe(boundStatement);

  auto matchClausePatternLabelRewriter =
      MatchClausePatternLabelRewriter(clientContext);
  matchClausePatternLabelRewriter.visitUnsafe(boundStatement);

  auto defaultTypeSolver = DefaultTypeSolver();
  defaultTypeSolver.visit(boundStatement);
}

}  // namespace binder
}  // namespace neug
