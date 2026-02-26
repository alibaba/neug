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

#include "neug/compiler/optimizer/common_pattern_reuse_optimizer.h"
#include <algorithm>
#include <memory>
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression/variable_expression.h"
#include "neug/compiler/common/enums/join_type.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/gopt/g_graph_type.h"
#include "neug/compiler/optimizer/flat_join_to_expand_optimizer.h"
#include "neug/compiler/planner/operator/logical_distinct.h"
#include "neug/compiler/planner/operator/logical_hash_join.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/scan/logical_expressions_scan.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"

namespace neug {
namespace optimizer {

// get the sequential-parent (the parent cannot be join or union) operator on
// top of the scan operator
std::shared_ptr<planner::LogicalOperator>
CommonPatternReuseOptimizer::getScanParent(
    std::shared_ptr<planner::LogicalOperator> parent) {
  auto children = parent->getChildren();
  // guarantee sequential parent
  if (children.size() != 1) {
    return nullptr;
  }
  if (children[0]->getOperatorType() ==
      planner::LogicalOperatorType::SCAN_NODE_TABLE) {
    return parent;
  }
  return getScanParent(children[0]);
}

std::shared_ptr<planner::LogicalOperator>
CommonPatternReuseOptimizer::visitOperator(
    const std::shared_ptr<planner::LogicalOperator>& op) {
  // bottom-up traversal
  for (auto i = 0u; i < op->getNumChildren(); ++i) {
    op->setChild(i, visitOperator(op->getChild(i)));
  }
  auto result = visitOperatorReplaceSwitch(op);
  // schema of each operator is unchanged
  // result->computeFlatSchema();
  return result;
}

void CommonPatternReuseOptimizer::rewrite(planner::LogicalPlan* plan) {
  auto root = plan->getLastOperator();
  auto rootOpt = visitOperator(root);
  plan->setLastOperator(rootOpt);
}

std::shared_ptr<planner::LogicalOperator>
CommonPatternReuseOptimizer::visitHashJoinReplace(
    std::shared_ptr<planner::LogicalOperator> op) {
  auto joinOp = op->ptrCast<planner::LogicalHashJoin>();
  if (joinOp->getJoinType() != common::JoinType::INNER &&
      joinOp->getJoinType() != common::JoinType::LEFT) {
    return op;
  }
  auto join = op->ptrCast<planner::LogicalHashJoin>();
  auto joinIDs = join->getJoinNodeIDs();

  if (joinIDs.size() > 2) {
    return op;
  }

  auto rightChild = join->getChild(1);
  FlatJoinToExpandOptimizer flatOpt;
  auto includeOps = {planner::LogicalOperatorType::SCAN_NODE_TABLE,
                     planner::LogicalOperatorType::EXTEND,
                     planner::LogicalOperatorType::GET_V};
  if (!flatOpt.checkOperatorType(rightChild, includeOps)) {
    return op;
  }

  auto rightScanParent = getScanParent(rightChild);
  if (!rightScanParent || rightScanParent->getNumChildren() == 0) {
    return op;
  }
  auto rightScan =
      rightScanParent->getChild(0)->ptrCast<planner::LogicalScanNodeTable>();
  if (!rightScan) {
    return op;
  }
  auto rightScanNodeID = rightScan->getNodeID();
  if (std::all_of(
          joinIDs.begin(), joinIDs.end(), [rightScanNodeID](auto joinID) {
            return joinID->getUniqueName() != rightScanNodeID->getUniqueName();
          })) {
    return op;
  }
  joinOp->setPreQuery(true);
  auto rightScanUniqueName = rightScan->getAliasName();
  auto rightScanType = rightScan->getNodeType(ctx->getCatalog());
  // set distinct to guarantee the right scan node is not duplicated, distinct
  // key is the unique name of the right scan node
  auto distinctKey = std::make_shared<binder::VariableExpression>(
      common::LogicalType::NODE(std::make_unique<common::GNodeTypeInfo>(
          std::vector<common::StructField>(), std::move(rightScanType))),
      rightScanUniqueName, rightScanUniqueName);
  // convert right scan node to expression scan, and set expression scan as the
  // child of distinct
  auto expressionScan = std::make_shared<planner::LogicalExpressionsScan>(
      binder::expression_vector{rightScanNodeID});
  auto distinct = std::make_shared<planner::LogicalDistinct>(
      binder::expression_vector{distinctKey}, expressionScan);
  distinct->computeFactorizedSchema();
  rightScanParent->setChild(0, distinct);
  return op;
}

}  // namespace optimizer
}  // namespace neug