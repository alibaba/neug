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
#include <vector>
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/common/enums/table_type.h"
#include "neug/compiler/function/export/export_function.h"
#include "neug/compiler/function/table/scan_file_function.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/logical_plan.h"
#include "neug/compiler/planner/operator/logical_table_function_call.h"
#include "neug/compiler/planner/operator/logical_transaction.h"
#include "neug/compiler/planner/operator/persistent/logical_copy_to.h"
#include "neug/compiler/planner/operator/persistent/logical_insert.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"
#include "neug/compiler/transaction/transaction_action.h"

namespace gs {
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
  GPhysicalAnalyzer() = default;
  ExecutionFlag analyze(const planner::LogicalPlan& plan) {
    auto skipScanNames = std::vector<std::string>();
    analyzeOperator(*plan.getLastOperator(), skipScanNames);
    return flag;
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
    case planner::LogicalOperatorType::PARTITIONER:
    case planner::LogicalOperatorType::INDEX_LOOK_UP:
    case planner::LogicalOperatorType::MULTIPLICITY_REDUCER:
    case planner::LogicalOperatorType::FLATTEN:
    case planner::LogicalOperatorType::ACCUMULATE:
    case planner::LogicalOperatorType::DUMMY_SCAN:
    case planner::LogicalOperatorType::HASH_JOIN:
    case planner::LogicalOperatorType::CROSS_PRODUCT:
    case planner::LogicalOperatorType::UNION_ALL: {
      break;
    }
    case planner::LogicalOperatorType::SCAN_NODE_TABLE: {
      auto scan = op.constPtrCast<planner::LogicalScanNodeTable>();
      if (std::find(skipScanNames.begin(), skipScanNames.end(),
                    scan->getAliasName()) != skipScanNames.end()) {
        break;
      }
    }
    default:
      // Other operators are set to read by default
      flag.read = true;
      break;
    }

    for (auto child : op.getChildren()) {
      analyzeOperator(*child, skipScanNames);
    }
  }

 private:
  ExecutionFlag flag;
};

}  // namespace gopt
}  // namespace gs
