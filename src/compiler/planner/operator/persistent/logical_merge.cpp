#include "neug/compiler/planner/operator/persistent/logical_merge.h"

#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/binder/expression/node_rel_expression.h"
#include "neug/compiler/common/cast.h"
#include "neug/compiler/common/enums/expression_type.h"
#include "neug/compiler/planner/operator/factorization/flatten_resolver.h"

using namespace neug::binder;
using namespace neug::common;

namespace neug {
namespace planner {

void LogicalMerge::computeFactorizedSchema() {
  copyChildSchema(0);
  for (auto& info : insertNodeInfos) {
    // Predicate iri is not matched but needs to be inserted.
    auto node = neug_dynamic_cast<NodeExpression*>(info.pattern.get());
    if (!schema->isExpressionInScope(*node->getInternalID())) {
      auto groupPos = schema->createGroup();
      schema->setGroupAsSingleState(groupPos);
      schema->insertToGroupAndScope(node->getInternalID(), groupPos);
    }
  }
}

void LogicalMerge::computeFlatSchema() {
  copyChildSchema(0);
  for (auto& info : insertNodeInfos) {
    auto node = neug_dynamic_cast<NodeExpression*>(info.pattern.get());
    schema->insertToGroupAndScopeMayRepeat(node->getInternalID(), 0);
  }
}

f_group_pos_set LogicalMerge::getGroupsPosToFlatten() {
  auto childSchema = children[0]->getSchema();
  return FlattenAll::getGroupsPosToFlatten(childSchema->getGroupsPosInScope(),
                                           *childSchema);
}

std::vector<gopt::GAliasName> LogicalMerge::getGAliasNames() const {
  std::vector<gopt::GAliasName> aliasNames;
  auto appendFromInsertInfos =
      [&aliasNames](const std::vector<LogicalInsertInfo>& infos) {
        for (const auto& info : infos) {
          auto pattern = info.pattern;
          if (pattern->expressionType == ExpressionType::PATTERN) {
            auto patternExpr = pattern->ptrCast<binder::NodeOrRelExpression>();
            std::string varName = patternExpr->getVariableName();
            aliasNames.emplace_back(
                patternExpr->getUniqueName(),
                varName.empty() ? std::nullopt : std::make_optional(varName));
          }
        }
      };
  appendFromInsertInfos(insertNodeInfos);
  appendFromInsertInfos(insertRelInfos);
  return aliasNames;
}

std::unique_ptr<LogicalOperator> LogicalMerge::copy() {
  auto merge =
      std::make_unique<LogicalMerge>(existenceMark, keys, children[0]->copy());
  merge->insertNodeInfos = copyVector(insertNodeInfos);
  merge->insertRelInfos = copyVector(insertRelInfos);
  merge->onCreateSetNodeInfos = copyVector(onCreateSetNodeInfos);
  merge->onCreateSetRelInfos = copyVector(onCreateSetRelInfos);
  merge->onMatchSetNodeInfos = copyVector(onMatchSetNodeInfos);
  merge->onMatchSetRelInfos = copyVector(onMatchSetRelInfos);
  return merge;
}

}  // namespace planner
}  // namespace neug
