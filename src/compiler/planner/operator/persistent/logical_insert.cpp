#include "neug/compiler/planner/operator/persistent/logical_insert.h"
#include <optional>

#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/binder/expression/node_rel_expression.h"
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/common/cast.h"
#include "neug/compiler/planner/operator/factorization/flatten_resolver.h"

using namespace neug::common;
using namespace neug::binder;

namespace neug {
namespace planner {

void LogicalInsert::computeFactorizedSchema() {
  copyChildSchema(0);
  for (auto& info : infos) {
    auto groupPos = schema->createGroup();
    schema->setGroupAsSingleState(groupPos);
    for (auto i = 0u; i < info.columnExprs.size(); ++i) {
      if (info.isReturnColumnExprs[i]) {
        schema->insertToGroupAndScope(info.columnExprs[i], groupPos);
      }
    }
    if (info.tableType == TableType::NODE) {
      auto node = neug_dynamic_cast<NodeExpression*>(info.pattern.get());
      schema->insertToGroupAndScopeMayRepeat(node->getInternalID(), groupPos);
    }
  }
}

void LogicalInsert::computeFlatSchema() {
  copyChildSchema(0);
  for (auto& info : infos) {
    for (auto i = 0u; i < info.columnExprs.size(); ++i) {
      if (info.isReturnColumnExprs[i]) {
        schema->insertToGroupAndScope(info.columnExprs[i], 0);
      }
    }
    if (info.tableType == TableType::NODE) {
      auto node = neug_dynamic_cast<NodeExpression*>(info.pattern.get());
      schema->insertToGroupAndScopeMayRepeat(node->getInternalID(), 0);
    }
  }
}

std::string LogicalInsert::getExpressionsForPrinting() const {
  std::string result;
  for (auto i = 0u; i < infos.size() - 1; ++i) {
    result += infos[i].pattern->toString() + ",";
  }
  result += infos[infos.size() - 1].pattern->toString();
  return result;
}

f_group_pos_set LogicalInsert::getGroupsPosToFlatten() {
  auto childSchema = children[0]->getSchema();
  return FlattenAll::getGroupsPosToFlatten(childSchema->getGroupsPosInScope(),
                                           *childSchema);
}

std::vector<gopt::GAliasName> LogicalInsert::getGAliasNames() const {
  std::vector<gopt::GAliasName> aliasNames;
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
  return aliasNames;
}

}  // namespace planner
}  // namespace neug
