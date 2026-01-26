/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#include "neug/compiler/optimizer/remove_unnecessary_join_optimizer.h"

#include "neug/compiler/planner/operator/logical_hash_join.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"

using namespace neug::common;
using namespace neug::planner;

namespace neug {
namespace optimizer {

void RemoveUnnecessaryJoinOptimizer::rewrite(LogicalPlan* plan) {
  visitOperator(plan->getLastOperator());
}

std::shared_ptr<LogicalOperator> RemoveUnnecessaryJoinOptimizer::visitOperator(
    const std::shared_ptr<LogicalOperator>& op) {
  // bottom-up traversal
  for (auto i = 0u; i < op->getNumChildren(); ++i) {
    op->setChild(i, visitOperator(op->getChild(i)));
  }
  auto result = visitOperatorReplaceSwitch(op);
  result->computeFlatSchema();
  return result;
}

std::shared_ptr<LogicalOperator>
RemoveUnnecessaryJoinOptimizer::visitHashJoinReplace(
    std::shared_ptr<LogicalOperator> op) {
  auto hashJoin = (LogicalHashJoin*) op.get();
  switch (hashJoin->getJoinType()) {
  case JoinType::MARK:
  case JoinType::LEFT: {
    // Do not prune no-trivial join type
    return op;
  }
  default:
    break;
  }
  // TODO(Xiyang): Double check on these changes here.
  if (op->getChild(1)->getOperatorType() ==
      LogicalOperatorType::SCAN_NODE_TABLE) {
    const auto scanNode =
        neug_dynamic_cast<LogicalScanNodeTable*>(op->getChild(1).get());
    if (scanNode->getProperties().empty()) {
      // Build side is trivial. Prune build side.
      return op->getChild(0);
    }
  }
  if (op->getChild(0)->getOperatorType() ==
      LogicalOperatorType::SCAN_NODE_TABLE) {
    const auto scanNode =
        neug_dynamic_cast<LogicalScanNodeTable*>(op->getChild(0).get());
    if (scanNode->getProperties().empty()) {
      // Probe side is trivial. Prune probe side.
      return op->getChild(1);
    }
  }
  return op;
}

}  // namespace optimizer
}  // namespace neug
