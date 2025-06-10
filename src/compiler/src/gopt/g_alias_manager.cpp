#include "gopt/g_alias_manager.h"

#include <string>
#include <unordered_map>
#include "common/exception/exception.h"
#include "common/types/types.h"
#include "gopt/logical_get_v.h"
#include "planner/operator/extend/logical_extend.h"
#include "planner/operator/logical_operator.h"
#include "planner/operator/logical_plan.h"
#include "planner/operator/scan/logical_scan_node_table.h"

namespace gs {
namespace gopt {

GAliasManager::GAliasManager(const planner::LogicalPlan& plan) {
  auto lastOp = plan.getLastOperator();
  visitOperator(*lastOp);
}

void GAliasManager::extractAliasName(const planner::LogicalOperator& op,
                                     std::vector<std::string>& aliasNames) {
  switch (op.getOperatorType()) {
  case planner::LogicalOperatorType::SCAN_NODE_TABLE: {
    auto scanOp = op.constCast<planner::LogicalScanNodeTable>();
    aliasNames.emplace_back(scanOp.getAliasName());
    break;
  }
  case planner::LogicalOperatorType::EXTEND: {
    auto& extendOp = op.constCast<planner::LogicalExtend>();
    aliasNames.emplace_back(extendOp.getAliasName());
    break;
  }
  case planner::LogicalOperatorType::GET_V: {
    auto& getVOp = op.constCast<gopt::LogicalGetV>();
    aliasNames.emplace_back(getVOp.getAliasName());
    break;
  }
  case planner::LogicalOperatorType::PROJECTION: {
    auto schema = op.getSchema();
    if (schema != nullptr) {
      auto exprs = schema->getExpressionsInScope();
      for (const auto& expr : exprs) {
        if (expr->hasAlias()) {
          aliasNames.emplace_back(expr->getAlias());
        } else {
          aliasNames.emplace_back(expr->getUniqueName());
        }
      }
    }
    break;
  }
  case planner::LogicalOperatorType::COPY_FROM:
  case planner::LogicalOperatorType::TABLE_FUNCTION_CALL:
  case planner::LogicalOperatorType::ALTER:
  case planner::LogicalOperatorType::DROP:
  case planner::LogicalOperatorType::CREATE_TABLE:
  case planner::LogicalOperatorType::INDEX_LOOK_UP:
  case planner::LogicalOperatorType::PARTITIONER:
  case planner::LogicalOperatorType::FILTER:
    // do nothing
    break;
  default: {
    throw common::Exception(
        "Unsupported operator type for alias management: " +
        std::to_string(static_cast<int>(op.getOperatorType())));
  }
  }
}

void GAliasManager::visitOperator(const planner::LogicalOperator& op) {
  for (auto child : op.getChildren()) {
    visitOperator(*child);
  }
  std::vector<std::string> aliasNames;
  extractAliasName(op, aliasNames);
  for (const auto& name : aliasNames) {
    addAliasName(name);
  }
}

void GAliasManager::addAliasName(const std::string& name) {
  if (!name.empty() && !nameToId.contains(name)) {
    nameToId[name] = nextId;
    idToName[nextId] = name;
    nextId++;
  }
}

common::alias_id_t GAliasManager::getAliasId(const std::string& name) {
  if (!nameToId.contains(name)) {
    throw common::Exception("Alias name not found: " + name);
  }
  return nameToId[name];
}

std::string GAliasManager::getAliasName(common::alias_id_t id) {
  if (idToName.contains(id)) {
    return idToName[id];
  }
  throw common::Exception("Alias ID not found: " + std::to_string(id));
}

std::string GAliasManager::printForDebug() {
  std::string result = "nameToId:\n";
  for (const auto& [name, id] : nameToId) {
    result += "  " + name + " -> " + std::to_string(id) + "\n";
  }
  result += "idToName:\n";
  for (const auto& [id, name] : idToName) {
    result += "  " + std::to_string(id) + " -> " + name + "\n";
  }
  return result;
}

}  // namespace gopt
}  // namespace gs