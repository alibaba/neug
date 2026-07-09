#include "neug/compiler/planner/operator/persistent/logical_set.h"

#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression/rel_expression.h"
#include "neug/compiler/planner/operator/factorization/flatten_resolver.h"

using namespace neug::binder;
using namespace neug::common;

namespace neug {
namespace planner {

void LogicalSetProperty::computeFactorizedSchema() { copyChildSchema(0); }

void LogicalSetProperty::computeFlatSchema() { copyChildSchema(0); }

f_group_pos_set LogicalSetProperty::getGroupsPosToFlatten(uint32_t idx) const {
  f_group_pos_set result;
  auto childSchema = children[0]->getSchema();
  auto& info = infos[idx];
  switch (getEntryType()) {
  case SchemaEntryType::NODE: {
    auto node = info.pattern->constPtrCast<NodeExpression>();
    result.insert(childSchema->getGroupPos(*node->getInternalID()));
  } break;
  case SchemaEntryType::REL: {
    auto rel = info.pattern->constPtrCast<RelExpression>();
    result.insert(
        childSchema->getGroupPos(*rel->getSrcNode()->getInternalID()));
    result.insert(
        childSchema->getGroupPos(*rel->getDstNode()->getInternalID()));
  } break;
  default:
    NEUG_UNREACHABLE;
  }
  auto analyzer = GroupDependencyAnalyzer(false, *childSchema);
  analyzer.visit(info.columnData);
  for (auto& groupPos : analyzer.getDependentGroups()) {
    result.insert(groupPos);
  }
  return FlattenAll::getGroupsPosToFlatten(result, *childSchema);
}

std::string LogicalSetProperty::getExpressionsForPrinting() const {
  std::string result = ExpressionUtil::toString(
      std::make_pair(infos[0].column, infos[0].columnData));
  for (auto i = 1u; i < infos.size(); ++i) {
    result += ExpressionUtil::toString(
        std::make_pair(infos[i].column, infos[i].columnData));
  }
  return result;
}

SchemaEntryType LogicalSetProperty::getEntryType() const {
  NEUG_ASSERT(!infos.empty());
  return infos[0].entryType;
}

}  // namespace planner
}  // namespace neug
