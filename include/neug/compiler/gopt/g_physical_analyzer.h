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

#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/logical_plan.h"
#include "neug/compiler/planner/operator/logical_transaction.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace gopt {

enum class PhysicalMode { UNRECOGNIZED, READ_ONLY, READ_WRITE, DDL, ADMIN };

class GPhysicalAnalyzer {
 public:
  GPhysicalAnalyzer() : mode(PhysicalMode::UNRECOGNIZED) {}

  PhysicalMode analyze(const planner::LogicalPlan& plan) {
    mode = PhysicalMode::UNRECOGNIZED;  // Default mode is UNRECOGNIZED
    analyzeOperator(*plan.getLastOperator());
    return mode;
  }

 private:
  void analyzeOperator(const planner::LogicalOperator& op) {
    // Visit children first
    for (auto child : op.getChildren()) {
      analyzeOperator(*child);
    }

    if (isAdminOperator(op)) {
      if (mode != PhysicalMode::UNRECOGNIZED && mode != PhysicalMode::ADMIN) {
        THROW_EXCEPTION_WITH_FILE_LINE(
            "Cannot mix ADMIN with other operations in the same plan.");
      }
      mode = PhysicalMode::ADMIN;
      return;
    }

    // Check operator type
    switch (op.getOperatorType()) {
    case planner::LogicalOperatorType::CREATE_TABLE:
    case planner::LogicalOperatorType::ALTER:
    case planner::LogicalOperatorType::DROP: {
      if (mode == PhysicalMode::READ_WRITE) {
        THROW_EXCEPTION_WITH_FILE_LINE(
            "Cannot mix DDL and READ_WRITE operations in the same plan.");
      }
      mode = PhysicalMode::DDL;
      break;
    }
    case planner::LogicalOperatorType::COPY_FROM:
    case planner::LogicalOperatorType::INSERT:
    case planner::LogicalOperatorType::SET_PROPERTY:
    case planner::LogicalOperatorType::DELETE:
    case planner::LogicalOperatorType::TABLE_FUNCTION_CALL: {
      if (mode == PhysicalMode::DDL) {
        THROW_EXCEPTION_WITH_FILE_LINE(
            "Cannot mix READ_WRITE with DDL operations in the same plan.");
      }
      mode = PhysicalMode::READ_WRITE;
      break;
    }
    default:
      if (mode == PhysicalMode::UNRECOGNIZED) {
        mode = PhysicalMode::READ_ONLY;
      }
      // Keep existing mode (QUERY by default)
      break;
    }
  }

  bool isAdminOperator(const planner::LogicalOperator& op) {
    switch (op.getOperatorType()) {
    case planner::LogicalOperatorType::TRANSACTION: {
      auto transaction = op.constPtrCast<planner::LogicalTransaction>();
      auto action = transaction->getTransactionAction();
      return action == transaction::TransactionAction::CHECKPOINT;
    }
    case planner::LogicalOperatorType::EXTENSION:
      return true;
    default:
      return false;
    }
  }

 private:
  PhysicalMode mode;
};

}  // namespace gopt
}  // namespace gs
