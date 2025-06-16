/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "gopt/g_alias_manager.h"

#include <string>
#include <unordered_map>
#include "common/exception/exception.h"
#include "common/types/types.h"
#include "planner/operator/extend/logical_extend.h"
#include "planner/operator/extend/logical_recursive_extend.h"
#include "planner/operator/logical_aggregate.h"
#include "planner/operator/logical_get_v.h"
#include "planner/operator/logical_operator.h"
#include "planner/operator/logical_plan.h"
#include "planner/operator/scan/logical_scan_node_table.h"

namespace gs {
namespace gopt {

GAliasManager::GAliasManager(const planner::LogicalPlan& plan) {
  auto lastOp = plan.getLastOperator();
  visitOperator(*lastOp);
}

void GAliasManager::extractGAliasNames(
    const planner::LogicalOperator& op,
    std::vector<gopt::GAliasName>& aliasNames) {
  switch (op.getOperatorType()) {
  case planner::LogicalOperatorType::SCAN_NODE_TABLE: {
    auto scanOp = op.constCast<planner::LogicalScanNodeTable>();
    aliasNames.emplace_back(scanOp.getGAliasName());
    break;
  }
  case planner::LogicalOperatorType::EXTEND: {
    auto& extendOp = op.constCast<planner::LogicalExtend>();
    aliasNames.emplace_back(extendOp.getGAliasName());
    break;
  }
  case planner::LogicalOperatorType::RECURSIVE_EXTEND: {
    auto& extendOp = op.constCast<planner::LogicalRecursiveExtend>();
    aliasNames.emplace_back(extendOp.getGAliasName());
    break;
  }
  case planner::LogicalOperatorType::GET_V: {
    auto& getVOp = op.constCast<planner::LogicalGetV>();
    aliasNames.emplace_back(getVOp.getGAliasName());
    break;
  }
  case planner::LogicalOperatorType::PROJECTION:
  case planner::LogicalOperatorType::AGGREGATE:
  case planner::LogicalOperatorType::FILTER:
  case planner::LogicalOperatorType::ORDER_BY:
  case planner::LogicalOperatorType::LIMIT: {
    auto schema = op.getSchema();
    if (schema != nullptr) {
      auto exprs = schema->getExpressionsInScope();
      for (const auto& expr : exprs) {
        auto queryName = expr->hasAlias() ? std::make_optional(expr->getAlias())
                                          : std::nullopt;
        aliasNames.emplace_back(
            gopt::GAliasName{expr->getUniqueName(), queryName});
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
  case planner::LogicalOperatorType::MULTIPLICITY_REDUCER:
    // do nothing
    break;
  default: {
    throw common::Exception(
        "Unsupported operator type for alias management: " +
        std::to_string(static_cast<int>(op.getOperatorType())));
  }
  }
}

void GAliasManager::extractAliasIds(const planner::LogicalOperator& op,
                                    std::vector<common::alias_id_t>& aliasIds) {
  std::vector<gopt::GAliasName> aliasNames;
  extractGAliasNames(op, aliasNames);
  for (const auto& aliasName : aliasNames) {
    aliasIds.emplace_back(getAliasId(aliasName.uniqueName));
  }
}

void GAliasManager::visitOperator(const planner::LogicalOperator& op) {
  for (auto child : op.getChildren()) {
    visitOperator(*child);
  }

  std::vector<gopt::GAliasName> aliasNames;
  extractGAliasNames(op, aliasNames);
  for (const auto& name : aliasNames) {
    switch (op.getOperatorType()) {
    case planner::LogicalOperatorType::EXTEND:
    case planner::LogicalOperatorType::RECURSIVE_EXTEND:
    case planner::LogicalOperatorType::SCAN_NODE_TABLE:
    case planner::LogicalOperatorType::GET_V: {
      if (op.getOperatorType() == planner::LogicalOperatorType::EXTEND) {
        auto& extendOp = op.constCast<planner::LogicalExtend>();
        if (extendOp.getExtendOpt() == planner::ExtendOpt::VERTEX) {
          auto relUniqueName = extendOp.getRel()->getUniqueName();
          uniqueNameToId[relUniqueName] = DEFAULT_ALIAS_ID;
        }
      } else if (op.getOperatorType() ==
                 planner::LogicalOperatorType::RECURSIVE_EXTEND) {
        auto& extendOp = op.constCast<planner::LogicalRecursiveExtend>();
        // add alias names of expand and getV base, which are required by the
        // filtering on path node or rels.
        uniqueNameToId[extendOp.getExpandBaseName()] = DEFAULT_ALIAS_ID;
        uniqueNameToId[extendOp.getGetVBaseName()] = DEFAULT_ALIAS_ID;
      }
      auto uniqueName = name.uniqueName;
      auto queryName = name.queryName;
      if (!queryName.has_value()) {
        uniqueNameToId[uniqueName] = DEFAULT_ALIAS_ID;
        break;
      }
    }
    default:
      addGAliasName(name);
      break;
    }
  }
}

void GAliasManager::addGAliasName(const gopt::GAliasName& name) {
  auto& uniqueName = name.uniqueName;
  if (!uniqueNameToId.contains(uniqueName)) {
    uniqueNameToId[uniqueName] = nextId;
    idToGName[nextId] = name;
    ++nextId;
  } else {
    auto aliasId = uniqueNameToId[uniqueName];
    idToGName[aliasId] = name;
  }
}

common::alias_id_t GAliasManager::getAliasId(const std::string& uniqueName) {
  if (uniqueName == DEFAULT_ALIAS_NAME) {
    return DEFAULT_ALIAS_ID;
  }
  if (!uniqueNameToId.contains(uniqueName)) {
    throw common::Exception("Unique name not found: " + uniqueName);
  }
  return uniqueNameToId[uniqueName];
}

gopt::GAliasName GAliasManager::getGAliasName(common::alias_id_t id) {
  if (idToGName.contains(id)) {
    return idToGName[id];
  }
  throw common::Exception("Alias ID not found: " + std::to_string(id));
}

std::string GAliasManager::printForDebug() {
  std::string result;
  for (const auto& [uniqueName, aliasId] : uniqueNameToId) {
    result += "Unique Name: " + uniqueName +
              ", Alias ID: " + std::to_string(aliasId) + "\n";
  }
  for (const auto& [aliasId, gAliasName] : idToGName) {
    result += "Alias ID: " + std::to_string(aliasId) +
              ", Unique Name: " + gAliasName.uniqueName;
    if (gAliasName.queryName.has_value()) {
      result += ", Query Name: " + gAliasName.queryName.value() + "\n";
    } else {
      result += "\n";
    }
  }
  return result;
}

}  // namespace gopt
}  // namespace gs