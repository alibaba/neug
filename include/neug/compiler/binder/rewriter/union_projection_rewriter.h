#pragma once
#include "neug/compiler/binder/bound_statement_visitor.h"
#include "neug/compiler/binder/query/reading_clause/bound_match_clause.h"

namespace neug {
namespace binder {
/** This rewriter is used to project outer variables (defined by call) before
 * each subquery in union. For example, in the following query: MATCH (a:person)
 * CALL (a) { subquery1 UNION ALL subquery2 }, it will project the outer
 * variable `a` before subquery1 and subquery2.
 */
class UnionProjectionRewriter final : public BoundStatementVisitor {
 public:
  explicit UnionProjectionRewriter(main::ClientContext* clientContext)
      : clientContext{clientContext} {}

  void visitRegularQueryUnsafe(BoundStatement& statement) override;

 private:
  main::ClientContext* clientContext;

  bool visitBoundMatchClause(BoundMatchClause& matchClause,
                             const expression_vector& preQueryExprs);

  std::vector<std::shared_ptr<NodeExpression>> getJoinNodes(
      const expression_vector& preQueryExprs,
      const QueryGraphCollection* queryGraph);
  NormalizedQueryPart createProjectionPart(
      const std::vector<std::shared_ptr<Expression>>& projectionExprs);
};
}  // namespace binder
}  // namespace neug