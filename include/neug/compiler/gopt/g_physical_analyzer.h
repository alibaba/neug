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

#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/common/enums/table_type.h"
#include "neug/compiler/function/export/export_function.h"
#include "neug/compiler/function/table/scan_file_function.h"
#include "neug/compiler/gopt/g_alias_manager.h"
#include "neug/compiler/planner/operator/logical_hash_join.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/logical_plan.h"
#include "neug/compiler/planner/operator/logical_table_function_call.h"
#include "neug/compiler/planner/operator/logical_transaction.h"
#include "neug/compiler/planner/operator/logical_union.h"
#include "neug/compiler/planner/operator/persistent/logical_copy_to.h"
#include "neug/compiler/planner/operator/persistent/logical_insert.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"

namespace neug {
namespace gopt {

struct ExecutionFlag {
  bool read = false;
  bool insert = false;
  bool update = false;
  bool schema = false;
  bool batch = false;
  bool create_temp_table = false;
  bool transaction = false;
  bool procedure_call = false;
};

class GPhysicalAnalyzer {
 public:
  GPhysicalAnalyzer(catalog::Catalog* catalog) : catalog(catalog) {}
  ExecutionFlag analyze(const planner::LogicalPlan& plan) {
    auto skipScanNames = std::vector<std::string>();
    analyzeOperator(*plan.getLastOperator(), skipScanNames);
    return flag;
  }

  static std::unordered_set<std::string> collectPrimaryKeys(
      catalog::Catalog* catalog, const planner::LogicalScanNodeTable& scan) {
    auto tableIds = scan.getTableIDs();
    auto result = std::unordered_set<std::string>();
    for (auto& tableId : tableIds) {
      auto tableEntry = catalog->getTableCatalogEntry(
          &neug::Constants::DEFAULT_TRANSACTION, tableId);
      auto nodeTableEntry =
          dynamic_cast<catalog::NodeTableCatalogEntry*>(tableEntry);
      if (!nodeTableEntry) {
        THROW_EXCEPTION_WITH_FILE_LINE(
            "Primary key scan is only supported for node "
            "tables, but got: " +
            tableEntry->getName());
      }
      result.insert(nodeTableEntry->getPrimaryKeyName());
    }
    return result;
  }

  static bool containsAliasName(const planner::LogicalOperator& op,
                                const binder::Expression& expr) {
    auto gAliasNames = std::vector<gopt::GAliasName>();
    GAliasManager::extractGAliasNames(op, gAliasNames);
    for (auto& gAliasName : gAliasNames) {
      if (gAliasName.uniqueName == expr.getUniqueName()) {
        return true;
      }
    }
    return false;
  }

  static planner::LogicalScanNodeTable* getScanFromPKJoin(
      catalog::Catalog* catalog, planner::LogicalOperator* child) {
    if (child->getOperatorType() == planner::LogicalOperatorType::HASH_JOIN) {
      auto join = child->ptrCast<planner::LogicalHashJoin>();
      auto left = join->getChild(0);
      auto conditions = join->getJoinConditions();
      if (join->getJoinType() == common::JoinType::INNER &&
          conditions.size() == 1) {
        auto [leftKey, rightKey] = conditions[0];
        if (leftKey->expressionType == common::ExpressionType::PROPERTY &&
            !containsAliasName(*left, *leftKey) &&
            (rightKey->expressionType == common::ExpressionType::VARIABLE ||
             containsAliasName(*join->getChild(1), *rightKey))) {
          auto leftProperty =
              leftKey->ptrCast<binder::PropertyExpression>()->getPropertyName();
          if (left->getOperatorType() ==
              planner::LogicalOperatorType::SCAN_NODE_TABLE) {
            auto scan = left->ptrCast<planner::LogicalScanNodeTable>();
            auto pks = collectPrimaryKeys(catalog, *scan);
            if (pks.size() == 1 && pks.contains(leftProperty)) {
              return scan;
            }
          }
        }
      }
    }
    return nullptr;
  }

 private:
  bool isDataSource(const planner::LogicalOperator& op) {
    if (op.getOperatorType() !=
        planner::LogicalOperatorType::TABLE_FUNCTION_CALL) {
      return false;
    }
    auto tableFuncOp = op.constPtrCast<planner::LogicalTableFunctionCall>();
    auto bindData = tableFuncOp->getBindData();
    return dynamic_cast<function::ScanFileBindData*>(bindData) != nullptr;
  }

  void collectAtomicScan(std::shared_ptr<planner::LogicalOperator> child,
                         std::vector<std::string>& result) {
    if (child->getOperatorType() ==
        planner::LogicalOperatorType::SCAN_NODE_TABLE) {
      auto scan = child->cast<planner::LogicalScanNodeTable>();
      auto extraInfo = scan.getExtraInfo();
      if (extraInfo) {
        auto pkInfo = dynamic_cast<planner::PrimaryKeyScanInfo*>(extraInfo);
        if (pkInfo) {
          result.push_back(scan.getAliasName());
        }
      }
    } else if (child->getOperatorType() ==
               planner::LogicalOperatorType::INSERT) {
      auto& insert = child->cast<planner::LogicalInsert>();
      auto& infos = insert.getInfos();
      if (!infos.empty() && infos[0].tableType == common::TableType::NODE) {
        auto gAliasNames = insert.getGAliasNames();
        for (auto& gAliasName : gAliasNames) {
          result.push_back(gAliasName.uniqueName);
        }
      }
    } else if (child->getOperatorType() ==
               planner::LogicalOperatorType::HASH_JOIN) {
      auto pkJoin = getScanFromPKJoin(catalog, child.get());
      if (pkJoin) {
        result.push_back(pkJoin->getAliasName());
      }
    }
    for (auto subChild : child->getChildren()) {
      collectAtomicScan(subChild, result);
    }
  }

  void analyzeOperator(const planner::LogicalOperator& op,
                       std::vector<std::string>& skipScanNames) {
    switch (op.getOperatorType()) {
    case planner::LogicalOperatorType::INSERT: {
      auto insertOp = op.constPtrCast<planner::LogicalInsert>();
      auto& infos = insertOp->getInfos();
      if (!infos.empty()) {
        // we assume that all info have the same table type
        auto tableType = infos[0].tableType;
        if (tableType == common::TableType::NODE) {
          flag.insert = true;
        } else if (tableType == common::TableType::REL) {
          if (insertOp->getChildren().empty()) {
            flag.insert = true;
          } else {
            auto boundNodes =
                std::vector<std::shared_ptr<binder::Expression>>();
            for (auto& info : infos) {
              auto relExpr = info.pattern->ptrCast<binder::RelExpression>();
              boundNodes.push_back(relExpr->getSrcNode());
              boundNodes.push_back(relExpr->getDstNode());
            }
            auto atomicScanNames = std::vector<std::string>();
            collectAtomicScan(insertOp->getChild(0), atomicScanNames);
            if (preQuery) {
              collectAtomicScan(preQuery, atomicScanNames);
            }
            if (std::any_of(
                    boundNodes.begin(), boundNodes.end(),
                    [&](const std::shared_ptr<binder::Expression>& node) {
                      return std::find(atomicScanNames.begin(),
                                       atomicScanNames.end(),
                                       node->getUniqueName()) ==
                             atomicScanNames.end();
                    })) {
              flag.update = true;
            } else {
              flag.insert = true;
              for (auto boundNode : boundNodes) {
                skipScanNames.push_back(boundNode->getUniqueName());
              }
            }
          }
        }
      }
      break;
    }
    case planner::LogicalOperatorType::SET_PROPERTY:
    case planner::LogicalOperatorType::DELETE: {
      flag.update = true;
      break;
    }
    case planner::LogicalOperatorType::COPY_FROM:
    case planner::LogicalOperatorType::COPY_TO: {
      flag.batch = true;
      break;
    }
    case planner::LogicalOperatorType::CREATE_TABLE:
    case planner::LogicalOperatorType::ALTER:
    case planner::LogicalOperatorType::DROP: {
      flag.schema = true;
      break;
    }
    case planner::LogicalOperatorType::TABLE_FUNCTION_CALL: {
      if (isDataSource(op)) {
        flag.batch = true;
      } else {
        flag.procedure_call = true;
      }
      break;
    }
    case planner::LogicalOperatorType::EXTENSION: {
      flag.procedure_call = true;
      break;
    }
    case planner::LogicalOperatorType::TRANSACTION: {
      flag.transaction = true;
      break;
    }
    // set read to true for graph operators
    case planner::LogicalOperatorType::SCAN_NODE_TABLE: {
      auto scan = op.constPtrCast<planner::LogicalScanNodeTable>();
      if (std::find(skipScanNames.begin(), skipScanNames.end(),
                    scan->getAliasName()) == skipScanNames.end()) {
        flag.read = true;
      }
      break;
    }
    case planner::LogicalOperatorType::RECURSIVE_EXTEND:
    case planner::LogicalOperatorType::GET_V:
    case planner::LogicalOperatorType::EXTEND: {
      flag.read = true;
      break;
    }
    default:
      // ignore other operators, because they do not interact with the graph
      // database
      break;
    }

    if (op.getOperatorType() == planner::LogicalOperatorType::UNION_ALL) {
      auto& unionOp = op.constCast<planner::LogicalUnion>();
      if (!unionOp.getChildren().empty()) {
        auto child0 = unionOp.getChild(0);
        if (unionOp.getPreQuery()) {
          preQuery = child0;
        }
        for (auto pos = 1; pos < unionOp.getNumChildren(); pos++) {
          auto child = unionOp.getChild(pos);
          analyzeOperator(*child, skipScanNames);
        }
        preQuery = nullptr;
        analyzeOperator(*child0, skipScanNames);
      }
    } else {
      for (auto child : op.getChildren()) {
        analyzeOperator(*child, skipScanNames);
      }
    }
  }

 private:
  ExecutionFlag flag;
  catalog::Catalog* catalog;
  std::shared_ptr<planner::LogicalOperator> preQuery;
};

}  // namespace gopt
}  // namespace neug
