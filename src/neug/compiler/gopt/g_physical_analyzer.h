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

namespace gs {
namespace gopt {

enum class PhysicalMode { READ_ONLY, READ_WRITE, DDL };

class GPhysicalAnalyzer {
 public:
  GPhysicalAnalyzer() : mode(PhysicalMode::READ_ONLY) {}

  PhysicalMode analyze(const planner::LogicalPlan& plan) {
    mode = PhysicalMode::READ_ONLY;  // Default mode is READ_ONLY
    analyzeOperator(*plan.getLastOperator());
    return mode;
  }

 private:
  void analyzeOperator(const planner::LogicalOperator& op) {
    // Visit children first
    for (auto child : op.getChildren()) {
      analyzeOperator(*child);
    }

    // Check operator type
    switch (op.getOperatorType()) {
    case planner::LogicalOperatorType::CREATE_TABLE:
    case planner::LogicalOperatorType::ALTER:
    case planner::LogicalOperatorType::DROP:
      if (mode == PhysicalMode::READ_WRITE) {
        throw exception::Exception(
            "Cannot mix DDL and READ_WRITE operations in the same plan.");
      }
      mode = PhysicalMode::DDL;
      break;
    case planner::LogicalOperatorType::COPY_FROM:
    case planner::LogicalOperatorType::INSERT:
    case planner::LogicalOperatorType::SET_PROPERTY:
    case planner::LogicalOperatorType::DELETE:
      if (mode == PhysicalMode::DDL) {
        throw exception::Exception(
            "Cannot mix READ_WRITE with DDL operations in the same plan.");
      }
      mode = PhysicalMode::READ_WRITE;
      break;
    default:
      // Keep existing mode (QUERY by default)
      break;
    }
  }

 private:
  PhysicalMode mode;
};

}  // namespace gopt
}  // namespace gs
