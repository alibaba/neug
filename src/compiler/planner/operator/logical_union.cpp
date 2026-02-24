#include "neug/compiler/planner/operator/logical_union.h"

#include "neug/compiler/planner/operator/factorization/flatten_resolver.h"
#include "neug/compiler/planner/operator/factorization/sink_util.h"

namespace neug {
namespace planner {

f_group_pos_set LogicalUnion::getGroupsPosToFlatten(uint32_t childIdx) {
  f_group_pos_set groupsPos;
  auto childSchema = children[childIdx]->getSchema();
  for (auto i = 0u; i < expressionsToUnion.size(); ++i) {
    if (requireFlatExpression(i)) {
      auto expression = childSchema->getExpressionsInScope()[i];
      groupsPos.insert(childSchema->getGroupPos(*expression));
    }
  }
  return FlattenAll::getGroupsPosToFlatten(groupsPos, *childSchema);
}

void LogicalUnion::computeFactorizedSchema() {
  auto firstChildSchema = children[0]->getSchema();
  createEmptySchema();
  SinkOperatorUtil::recomputeSchema(
      *firstChildSchema, firstChildSchema->getExpressionsInScope(), *schema);
}

void LogicalUnion::computeFlatSchema() {
  createEmptySchema();
  schema->createGroup();
  for (auto& expression : children[0]->getSchema()->getExpressionsInScope()) {
    schema->insertToGroupAndScope(expression, 0);
  }
}

std::unique_ptr<LogicalOperator> LogicalUnion::copy() {
  std::vector<std::shared_ptr<LogicalOperator>> copiedChildren;
  copiedChildren.reserve(getNumChildren());
  for (auto i = 0u; i < getNumChildren(); ++i) {
    copiedChildren.push_back(getChild(i)->copy());
  }
  return make_unique<LogicalUnion>(expressionsToUnion,
                                   std::move(copiedChildren));
}

std::unique_ptr<LogicalOperator> LogicalUnion::copyWithSchema() {
  std::vector<std::shared_ptr<LogicalOperator>> copiedChildren;
  copiedChildren.reserve(getNumChildren());
  for (auto i = 0u; i < getNumChildren(); ++i) {
    auto childCopy = getChild(i)->copy();
    childCopy->computeFactorizedSchema();
    copiedChildren.push_back(std::move(childCopy));
  }
  auto unionCopy =
      make_unique<LogicalUnion>(expressionsToUnion, std::move(copiedChildren));
  unionCopy->computeFactorizedSchema();
  return unionCopy;
}

bool LogicalUnion::requireFlatExpression(uint32_t expressionIdx) {
  for (auto& child : children) {
    auto childSchema = child->getSchema();
    auto expression = childSchema->getExpressionsInScope()[expressionIdx];
    if (childSchema->getGroup(expression)->isFlat()) {
      return true;
    }
  }
  return false;
}

}  // namespace planner
}  // namespace neug
