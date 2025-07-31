#include "neug/compiler/binder/bound_statement_rewriter.h"

#include "neug/compiler/binder/rewriter/match_clause_pattern_label_rewriter.h"
#include "neug/compiler/binder/rewriter/normalized_query_part_match_rewriter.h"
#include "neug/compiler/binder/rewriter/with_clause_projection_rewriter.h"
#include "neug/compiler/binder/visitor/default_type_solver.h"

namespace gs {
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
}  // namespace gs
