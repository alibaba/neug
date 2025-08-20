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

#include "neug/compiler/gopt/g_alias_manager.h"

#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/planner/operator/extend/logical_extend.h"
#include "neug/compiler/planner/operator/extend/logical_recursive_extend.h"
#include "neug/compiler/planner/operator/logical_aggregate.h"
#include "neug/compiler/planner/operator/logical_get_v.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/logical_plan.h"
#include "neug/compiler/planner/operator/persistent/logical_insert.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"
#include "neug/utils/exception/exception.h"
#include "planner/operator/logical_projection.h"
#include "planner/operator/logical_union.h"

namespace gs {
namespace gopt {

GAliasManager::GAliasManager(const planner::LogicalPlan& plan) {
  auto lastOp = plan.getLastOperator();
  std::unordered_set<std::string> vTags;
  visitOperator(*lastOp, vTags);
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
    if (extendOp.getNumChildren() > 0) {
      extractGAliasNames(*extendOp.getChild(0), aliasNames);
    }
    break;
  }
  case planner::LogicalOperatorType::RECURSIVE_EXTEND: {
    auto& extendOp = op.constCast<planner::LogicalRecursiveExtend>();
    aliasNames.emplace_back(extendOp.getGAliasName());
    if (extendOp.getNumChildren() > 0) {
      extractGAliasNames(*extendOp.getChild(0), aliasNames);
    }
    break;
  }
  case planner::LogicalOperatorType::GET_V: {
    auto& getVOp = op.constCast<planner::LogicalGetV>();
    aliasNames.emplace_back(getVOp.getGAliasName());
    if (getVOp.getNumChildren() > 0) {
      extractGAliasNames(*getVOp.getChild(0), aliasNames);
    }
    break;
  }
  case planner::LogicalOperatorType::INSERT: {
    auto& insertOp = op.constCast<planner::LogicalInsert>();
    auto aliases = insertOp.getGAliasNames();
    aliasNames.insert(aliasNames.end(), aliases.begin(), aliases.end());
    break;
  }
  case planner::LogicalOperatorType::PROJECTION:
  case planner::LogicalOperatorType::AGGREGATE: {
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
  case planner::LogicalOperatorType::FILTER:
  case planner::LogicalOperatorType::ORDER_BY:
  case planner::LogicalOperatorType::LIMIT:
  case planner::LogicalOperatorType::SET_PROPERTY:
  case planner::LogicalOperatorType::DELETE: {
    extractGAliasNames(*op.getChild(0), aliasNames);
    break;
  }
  case planner::LogicalOperatorType::COPY_FROM:
  case planner::LogicalOperatorType::COPY_TO:
  case planner::LogicalOperatorType::TABLE_FUNCTION_CALL:
  case planner::LogicalOperatorType::ALTER:
  case planner::LogicalOperatorType::DROP:
  case planner::LogicalOperatorType::CREATE_TABLE:
  case planner::LogicalOperatorType::INDEX_LOOK_UP:
  case planner::LogicalOperatorType::PARTITIONER:
  case planner::LogicalOperatorType::MULTIPLICITY_REDUCER:
  case planner::LogicalOperatorType::DUMMY_SCAN:
  case planner::LogicalOperatorType::INTERSECT:
  case planner::LogicalOperatorType::CROSS_PRODUCT:
  case planner::LogicalOperatorType::FLATTEN:
  case planner::LogicalOperatorType::ACCUMULATE:
  case planner::LogicalOperatorType::HASH_JOIN:
  case planner::LogicalOperatorType::EXPRESSIONS_SCAN:
  case planner::LogicalOperatorType::UNION_ALL:
  case planner::LogicalOperatorType::ALIAS_MAP:
    // do nothing
    break;
  default: {
    THROW_EXCEPTION_WITH_FILE_LINE(
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

std::optional<std::string> getUniqueName(const planner::LogicalOperator& op) {
  if (op.getOperatorType() == planner::LogicalOperatorType::EXTEND) {
    auto& extendOp = op.constCast<planner::LogicalExtend>();
    return extendOp.getAliasName();
  } else if (op.getOperatorType() ==
             planner::LogicalOperatorType::RECURSIVE_EXTEND) {
    auto& pathOp = op.constCast<planner::LogicalRecursiveExtend>();
    return pathOp.getAliasName();
  } else if (op.getOperatorType() == planner::LogicalOperatorType::GET_V) {
    auto& getVOp = op.constCast<planner::LogicalGetV>();
    return getVOp.getAliasName();
  } else if (op.getOperatorType() ==
             planner::LogicalOperatorType::SCAN_NODE_TABLE) {
    auto& scanOp = op.constCast<planner::LogicalScanNodeTable>();
    return scanOp.getAliasName();
  }
  return std::nullopt;
}

void GAliasManager::visitOperator(const planner::LogicalOperator& op,
                                  std::unordered_set<std::string>& vTags) {
  if (op.getOperatorType() == planner::LogicalOperatorType::EXTEND) {
    auto& extendOp = op.constCast<planner::LogicalExtend>();
    auto childNameOpt = getUniqueName(*op.getChild(0));
    if (childNameOpt.has_value() &&
        childNameOpt.value() != extendOp.getStartAliasName()) {
      vTags.insert(extendOp.getStartAliasName());
    }
  } else if (op.getOperatorType() ==
             planner::LogicalOperatorType::RECURSIVE_EXTEND) {
    auto& pathOp = op.constCast<planner::LogicalRecursiveExtend>();
    auto childNameOpt = getUniqueName(*op.getChild(0));
    if (childNameOpt.has_value() &&
        childNameOpt.value() != pathOp.getStartAliasName()) {
      vTags.insert(pathOp.getStartAliasName());
    }
  } else if (op.getOperatorType() == planner::LogicalOperatorType::GET_V) {
    auto& getVOp = op.constCast<planner::LogicalGetV>();
    auto childNameOpt = getUniqueName(*op.getChild(0));
    if (childNameOpt.has_value() &&
        childNameOpt.value() != getVOp.getStartAliasName()) {
      vTags.insert(getVOp.getStartAliasName());
    }
  }

  for (auto child : op.getChildren()) {
    visitOperator(*child, vTags);
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
      // if the unique name is not used by any later operators and the query
      // given name is not set, we set it as the default alias id
      if (!vTags.contains(uniqueName) && !queryName.has_value()) {
        uniqueNameToId[uniqueName] = DEFAULT_ALIAS_ID;
        break;
      }
    }
    case planner::LogicalOperatorType::INSERT:
      // TODO: Apply field trimming rule.
      // Currently, we assign unique alias IDs for each `CREATE` vertex or edge
      // operation to ensure correctness in cases like:
      // `CREATE (:Person {id:1})-[:knows]->(:Person {id: 2})`.
      // In such cases, node aliases are not
      // explicitly provided, but the edge still requires source and destination
      // aliases for proper binding.
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
    THROW_EXCEPTION_WITH_FILE_LINE("Unique name not found: " + uniqueName);
  }
  return uniqueNameToId[uniqueName];
}

gopt::GAliasName GAliasManager::getGAliasName(common::alias_id_t id) {
  if (idToGName.contains(id)) {
    return idToGName[id];
  }
  THROW_EXCEPTION_WITH_FILE_LINE("Alias ID not found: " + std::to_string(id));
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