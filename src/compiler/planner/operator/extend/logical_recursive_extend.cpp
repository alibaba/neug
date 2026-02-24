#include "neug/compiler/planner/operator/extend/logical_recursive_extend.h"

namespace neug {
namespace planner {

std::string LogicalRecursiveExtend::getExpressionsForPrinting() const {
  if (relExpr) {
    auto recursiveInfo = relExpr->getRecursiveInfo();
    // return bindData.toString();
    return "rel=" + relExpr->toString() +
           ", bound=" + getBoundNode()->toString() +
           ", nbr=" + getNbrNode()->toString() + ", nodePred=" +
           (recursiveInfo->nodePredicate
                ? recursiveInfo->nodePredicate->toString()
                : "null") +
           ", relPred=" +
           (recursiveInfo->relPredicate
                ? recursiveInfo->relPredicate->toString()
                : "null");
  } else {
    return function->getFunctionName();
  }
}

void LogicalRecursiveExtend::computeFlatSchema() {
  createEmptySchema();
  schema->createGroup();
  for (auto& expr : resultColumns) {
    schema->insertToGroupAndScope(expr, 0);
  }
}

void LogicalRecursiveExtend::computeFactorizedSchema() {
  createEmptySchema();
  auto pos = schema->createGroup();
  for (auto& e : resultColumns) {
    schema->insertToGroupAndScope(e, pos);
  }
}

}  // namespace planner
}  // namespace neug
