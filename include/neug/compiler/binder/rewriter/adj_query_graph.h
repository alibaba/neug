#pragma once

#include <memory>
#include <vector>
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/binder/expression/rel_expression.h"
#include "neug/compiler/binder/query/query_graph.h"
namespace neug {
namespace binder {

class AdjQueryGraph {
 public:
  AdjQueryGraph(const QueryGraphCollection* queryGraph)
      : queryGraph{queryGraph} {}

  std::vector<std::shared_ptr<RelExpression>> getAdjEdges(
      std::shared_ptr<NodeExpression> node) const {
    std::vector<std::shared_ptr<RelExpression>> adjEdges;
    auto allRels = queryGraph->getQueryRels();
    for (auto rel : allRels) {
      if (rel->getSrcNode()->getUniqueName() == node->getUniqueName() ||
          rel->getDstNode()->getUniqueName() == node->getUniqueName()) {
        adjEdges.emplace_back(rel);
      }
    }
    return adjEdges;
  }

  std::shared_ptr<NodeExpression> getAdjNode(
      std::shared_ptr<NodeExpression> node,
      std::shared_ptr<RelExpression> rel) const {
    if (rel->getSrcNode()->getUniqueName() == node->getUniqueName()) {
      return rel->getDstNode();
    } else {
      return rel->getSrcNode();
    }
  }

  std::shared_ptr<NodeExpression> getNode(
      std::shared_ptr<Expression> name) const {
    for (auto node : queryGraph->getQueryNodes()) {
      if (node->toString() == name->toString()) {
        return node;
      }
    }
    return nullptr;
  }

 private:
  const QueryGraphCollection* queryGraph;
};

}  // namespace binder
}  // namespace neug