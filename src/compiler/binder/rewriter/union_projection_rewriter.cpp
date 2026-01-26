#include "neug/compiler/binder/rewriter/union_projection_rewriter.h"
#include <vector>
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/binder/query/bound_regular_query.h"
#include "neug/compiler/binder/query/query_graph.h"
#include "neug/compiler/binder/query/reading_clause/bound_match_clause.h"
#include "neug/compiler/binder/query/reading_clause/bound_reading_clause.h"
#include "neug/compiler/common/enums/clause_type.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace binder {

void UnionProjectionRewriter::visitRegularQueryUnsafe(
    BoundStatement& statement) {
  auto& regularQuery = common::neug_dynamic_cast<BoundRegularQuery&>(statement);
  if (regularQuery.getPreQueryExprs().empty() ||
      regularQuery.getPreQueryPart().empty()) {
    return;
  }

  for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
    auto singleQuery = regularQuery.getSingleQueryUnsafe(i);
    if (singleQuery->getNumQueryParts() == 0) {
      continue;
    }
    auto queryPart = singleQuery->getQueryPartUnsafe(0);
    if (queryPart->getNumReadingClause() == 0) {
      continue;
    }
    auto readingClause = queryPart->getReadingClause(0);
    if (readingClause->getClauseType() != common::ClauseType::MATCH) {
      continue;
    }
    auto& matchClause = readingClause->cast<BoundMatchClause>();
    if (!visitBoundMatchClause(matchClause, regularQuery.getPreQueryExprs())) {
      return;
    }
  }

  regularQuery.setCommonPatReuse(true);

  // append projection before each subquery in union
  for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
    auto singleQuery = regularQuery.getSingleQueryUnsafe(i);
    if (singleQuery->getNumQueryParts() == 0) {
      continue;
    }
    auto queryPart = singleQuery->getQueryPartUnsafe(0);
    if (queryPart->getNumReadingClause() == 0) {
      continue;
    }
    auto readingClause = queryPart->getReadingClause(0);
    if (readingClause->getClauseType() != common::ClauseType::MATCH) {
      continue;
    }
    auto& matchClause = readingClause->cast<BoundMatchClause>();
    auto queryGraph = matchClause.getQueryGraphCollection();
    auto joinNodes = getJoinNodes(regularQuery.getPreQueryExprs(), queryGraph);
    if (joinNodes.size() != 1) {
      THROW_QUERY_EXECUTION_ERROR(
          "cannot apply union projection rewriter, the join nodes size is " +
          joinNodes.size());
    }
    singleQuery->insertQueryPart(
        0, createProjectionPart(regularQuery.getPreQueryExprs()));
  }
}

NormalizedQueryPart UnionProjectionRewriter::createProjectionPart(
    const std::vector<std::shared_ptr<Expression>>& projectionExprs) {
  expression_vector preProjectionExprs;
  for (auto expr : projectionExprs) {
    preProjectionExprs.emplace_back(expr);
  }
  BoundProjectionBody projectionBody(false);
  projectionBody.setProjectionExpressions(std::move(preProjectionExprs));
  NormalizedQueryPart queryPart;
  queryPart.setProjectionBody(std::move(projectionBody));
  return std::move(queryPart);
}

bool UnionProjectionRewriter::visitBoundMatchClause(
    BoundMatchClause& matchClause, const expression_vector& preQueryExprs) {
  auto queryGraph = matchClause.getQueryGraphCollection();
  auto joinNodes = getJoinNodes(preQueryExprs, queryGraph);
  return joinNodes.size() == 1;
}

std::vector<std::shared_ptr<NodeExpression>>
UnionProjectionRewriter::getJoinNodes(const expression_vector& preQueryExprs,
                                      const QueryGraphCollection* queryGraph) {
  std::vector<std::shared_ptr<NodeExpression>> joinNodes;
  for (auto queryNode : queryGraph->getQueryNodes()) {
    for (auto preQueryExpr : preQueryExprs) {
      if (preQueryExpr->toString() == queryNode->toString()) {
        joinNodes.push_back(queryNode);
        break;
      }
    }
  }
  return joinNodes;
}
}  // namespace binder
}  // namespace neug