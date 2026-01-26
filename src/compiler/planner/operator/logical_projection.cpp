#include "neug/compiler/planner/operator/logical_projection.h"
#include <string>
#include <unordered_set>

#include "neug/compiler/planner/operator/factorization/flatten_resolver.h"

namespace neug {
namespace planner {

void LogicalProjection::computeFactorizedSchema() {
  auto childSchema = children[0]->getSchema();
  schema = childSchema->copy();
  schema->clearExpressionsInScope();
  for (auto& expression : expressions) {
    auto groupPos = INVALID_F_GROUP_POS;
    if (childSchema->isExpressionInScope(
            *expression)) {  // expression to reference
      groupPos = childSchema->getGroupPos(*expression);
      schema->insertToScopeMayRepeat(expression, groupPos);
    } else {  // expression to evaluate
      auto analyzer = GroupDependencyAnalyzer(false, *childSchema);
      analyzer.visit(expression);
      auto dependentGroupPos = analyzer.getDependentGroups();
      SchemaUtils::validateAtMostOneUnFlatGroup(dependentGroupPos,
                                                *childSchema);
      if (dependentGroupPos.empty()) {  // constant
        groupPos = schema->createGroup();
        schema->setGroupAsSingleState(groupPos);
      } else {
        groupPos =
            SchemaUtils::getLeadingGroupPos(dependentGroupPos, *childSchema);
      }
      schema->insertToGroupAndScopeMayRepeat(expression, groupPos);
    }
  }
}

// Handles cases like `RETURN a.age, a.age AS k`, where the two expressions
// (`a.age`) have the same unique name. Assigns a non-duplicated unique name
// to each expression to fix this.
void LogicalProjection::resetExprUniqueNames() {
  std::unordered_set<std::string> uniqueNames;
  for (auto pos = 0; pos < expressions.size(); ++pos) {
    auto expr = expressions[pos];
    if (uniqueNames.contains(expr->getUniqueName())) {
      std::string newUnique = expr->getUniqueName() + "_" + std::to_string(pos);
      expr->setUniqueName(newUnique);
      continue;
    }
    uniqueNames.insert(expr->getUniqueName());
  }
}

void LogicalProjection::computeFlatSchema() {
  copyChildSchema(0);
  auto childSchema = children[0]->getSchema();
  schema->clearExpressionsInScope();
  resetExprUniqueNames();
  for (auto& expression : expressions) {
    if (childSchema->isExpressionInScope(*expression)) {
      schema->insertToScopeMayRepeat(expression, 0);
    } else {
      schema->insertToGroupAndScopeMayRepeat(expression, 0);
    }
  }
}

std::unordered_set<uint32_t> LogicalProjection::getDiscardedGroupsPos() const {
  auto groupsPosInScopeBeforeProjection =
      children[0]->getSchema()->getGroupsPosInScope();
  auto groupsPosInScopeAfterProjection = schema->getGroupsPosInScope();
  std::unordered_set<uint32_t> discardGroupsPos;
  for (auto i = 0u; i < schema->getNumGroups(); ++i) {
    if (groupsPosInScopeBeforeProjection.contains(i) &&
        !groupsPosInScopeAfterProjection.contains(i)) {
      discardGroupsPos.insert(i);
    }
  }
  return discardGroupsPos;
}

}  // namespace planner
}  // namespace neug
