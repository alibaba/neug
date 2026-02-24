#pragma once

#include "neug/compiler/binder/bound_statement_visitor.h"
#include "neug/compiler/binder/query/reading_clause/bound_join_hint.h"
#include "neug/compiler/binder/query/reading_clause/bound_match_clause.h"
#include "neug/compiler/binder/rewriter/adj_query_graph.h"
#include "neug/compiler/main/client_context.h"

namespace neug {
namespace binder {
/**
 * This rewriter is used to set join order heuristically for each subquery in
 * union. For example, in the following query: MATCH (a:person) CALL (a) {
 * subquery1 UNION ALL subquery2 }, it will generate join order for both
 * subquery1 and subquery2. Each subquery is bound to the common expression `a`
 * in this example, and the join order will start from the `a`
 */
class CommonPatReuseRewriter final : public BoundStatementVisitor {
 public:
  explicit CommonPatReuseRewriter(main::ClientContext* clientContext)
      : clientContext{clientContext} {}

  void visitRegularQueryUnsafe(BoundStatement& statement) override;

 private:
  main::ClientContext* clientContext;

  void visitBoundMatchClause(BoundMatchClause& matchClause,
                             const expression_vector& preQueryExprs);

  std::shared_ptr<BoundJoinHintNode> constructJoinHint(
      const BoundMatchClause& matchClause, std::shared_ptr<Expression> expr);

  std::shared_ptr<BoundJoinHintNode> constructJoinHintByDFS(
      const AdjQueryGraph& adjQueryGraph, std::shared_ptr<NodeExpression> root,
      std::unordered_set<std::string>& visited,
      std::shared_ptr<BoundJoinHintNode> preJoinHint);
};

}  // namespace binder
}  // namespace neug