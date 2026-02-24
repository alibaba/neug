#include "neug/compiler/binder/rewriter/common_pat_reuse_rewriter.h"
#include <memory>
#include <unordered_set>
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/binder/query/bound_regular_query.h"
#include "neug/compiler/binder/query/reading_clause/bound_match_clause.h"
#include "neug/compiler/binder/rewriter/adj_query_graph.h"
#include "neug/compiler/common/enums/clause_type.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace binder {
void CommonPatReuseRewriter::visitRegularQueryUnsafe(
    BoundStatement& statement) {
  auto& regularQuery = common::neug_dynamic_cast<BoundRegularQuery&>(statement);
  if (regularQuery.getPreQueryExprs().empty() ||
      regularQuery.getPreQueryPart().empty()) {
    return;
  }
  for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
    auto singleQuery = regularQuery.getSingleQueryUnsafe(i);
    for (auto j = 0u; j < singleQuery->getNumQueryParts(); j++) {
      auto queryPart = singleQuery->getQueryPartUnsafe(j);
      for (auto k = 0u; k < queryPart->getNumReadingClause(); k++) {
        auto readingClause = queryPart->getReadingClause(k);
        if (readingClause->getClauseType() == common::ClauseType::MATCH) {
          visitBoundMatchClause(readingClause->cast<BoundMatchClause>(),
                                regularQuery.getPreQueryExprs());
        }
      }
    }
  }
}

void CommonPatReuseRewriter::visitBoundMatchClause(
    BoundMatchClause& matchClause, const expression_vector& preQueryExprs) {
  auto joinHints = matchClause.getHint();
  if (preQueryExprs.empty() || joinHints) {
    return;
  }
  if (preQueryExprs.size() > 1) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "pre query before union with multiple bound expressions is "
        "unsupported");
  }
  auto expr = preQueryExprs.at(0);
  auto joinHint = constructJoinHint(matchClause, expr);
  if (joinHint) {
    matchClause.setHint(joinHint);
  }
}

std::shared_ptr<BoundJoinHintNode> CommonPatReuseRewriter::constructJoinHint(
    const BoundMatchClause& matchClause, std::shared_ptr<Expression> expr) {
  auto queryGraph = matchClause.getQueryGraphCollection();
  AdjQueryGraph adjQueryGraph(queryGraph);
  auto root = adjQueryGraph.getNode(expr);
  if (!root) {
    return nullptr;
  }
  auto rootHint = std::make_shared<BoundJoinHintNode>(root);
  std::unordered_set<std::string> visited;
  rootHint = constructJoinHintByDFS(adjQueryGraph, root, visited, rootHint);
  if (visited.size() != queryGraph->getQueryRels().size()) {
    return nullptr;
  }
  return rootHint;
}

std::shared_ptr<BoundJoinHintNode>
CommonPatReuseRewriter::constructJoinHintByDFS(
    const AdjQueryGraph& adjQueryGraph, std::shared_ptr<NodeExpression> root,
    std::unordered_set<std::string>& visited,
    std::shared_ptr<BoundJoinHintNode> preJoinHint) {
  // For each adjacent edge, do DFS if not visited
  for (auto adjEdge : adjQueryGraph.getAdjEdges(root)) {
    if (visited.contains(adjEdge->getUniqueName())) {
      continue;
    }
    visited.insert(adjEdge->getUniqueName());

    // Create join hint node for the adj edge
    auto edgeHint = std::make_shared<BoundJoinHintNode>(adjEdge);
    edgeHint->expandE = true;
    edgeHint->addChild(preJoinHint);

    // Create join hint node for the adj vertex
    auto adjNode = adjQueryGraph.getAdjNode(root, adjEdge);
    preJoinHint = std::make_shared<BoundJoinHintNode>(adjNode);
    preJoinHint->getV = true;
    preJoinHint->addChild(edgeHint);

    preJoinHint =
        constructJoinHintByDFS(adjQueryGraph, adjNode, visited, preJoinHint);
  }
  return preJoinHint;
}
}  // namespace binder
}  // namespace neug