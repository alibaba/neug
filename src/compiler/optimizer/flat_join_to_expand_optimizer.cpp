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

#include "neug/compiler/optimizer/flat_join_to_expand_optimizer.h"
#include <iostream>
#include <memory>
#include "neug/compiler/common/enums/join_type.h"
#include "neug/compiler/planner/operator/extend/logical_extend.h"
#include "neug/compiler/planner/operator/logical_get_v.h"
#include "neug/compiler/planner/operator/logical_hash_join.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"

namespace neug {
namespace optimizer {

// get the sequential-parent (the parent cannot be join or union) operator on
// top of the scan operator
std::shared_ptr<planner::LogicalOperator>
FlatJoinToExpandOptimizer::getScanParent(
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
FlatJoinToExpandOptimizer::visitOperator(
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

void FlatJoinToExpandOptimizer::rewrite(planner::LogicalPlan* plan) {
  auto root = plan->getLastOperator();
  auto rootOpt = visitOperator(root);
  plan->setLastOperator(rootOpt);
}

void FlatJoinToExpandOptimizer::setOptional(
    std::shared_ptr<planner::LogicalOperator> plan) {
  if (plan->getOperatorType() == planner::LogicalOperatorType::EXTEND) {
    auto extend = plan->ptrCast<planner::LogicalExtend>();
    extend->setOptional(true);
  }
  for (auto child : plan->getChildren()) {
    setOptional(child);
  }
}

bool FlatJoinToExpandOptimizer::checkOperatorType(
    std::shared_ptr<planner::LogicalOperator> op,
    const std::vector<planner::LogicalOperatorType>& includeTypes) {
  if (std::find(includeTypes.begin(), includeTypes.end(),
                op->getOperatorType()) == includeTypes.end()) {
    return false;
  }
  for (auto child : op->getChildren()) {
    if (!checkOperatorType(child, includeTypes)) {
      return false;
    }
  }
  return true;
}

int edgeNum(std::shared_ptr<planner::LogicalOperator> op) {
  int total = 0;
  while (op) {
    if (op->getOperatorType() == planner::LogicalOperatorType::EXTEND) {
      total++;
    }
    if (!op->getNumChildren())
      break;
    op = op->getChild(0);
  }
  return total;
}

std::shared_ptr<planner::LogicalOperator>
FlatJoinToExpandOptimizer::visitHashJoinReplace(
    std::shared_ptr<planner::LogicalOperator> op) {
  auto joinOp = op->ptrCast<planner::LogicalHashJoin>();
  if (joinOp->getJoinType() != common::JoinType::INNER &&
      joinOp->getJoinType() != common::JoinType::LEFT) {
    return op;
  }
  auto join = op->ptrCast<planner::LogicalHashJoin>();
  auto joinIDs = join->getJoinNodeIDs();
  if (joinIDs.size() != 1) {
    return op;
  }
  auto joinID = joinIDs[0];
  auto rightChild = join->getChild(1);

  if (joinOp->getJoinType() == common::JoinType::LEFT) {
    // there should be only one edge in the right branch
    if (edgeNum(rightChild) > 1) {
      return op;
    }
    // the getV operator should not have predicates, otherwise it cannot be
    // flattened
    if (rightChild->getOperatorType() == planner::LogicalOperatorType::GET_V) {
      auto getV = rightChild->ptrCast<planner::LogicalGetV>();
      if (getV->getPredicates()) {
        return op;
      }
    }
  }

  std::vector<planner::LogicalOperatorType> includeOps;
  if (joinOp->getJoinType() == common::JoinType::INNER) {
    includeOps.push_back(planner::LogicalOperatorType::SCAN_NODE_TABLE);
    includeOps.push_back(planner::LogicalOperatorType::EXTEND);
    includeOps.push_back(planner::LogicalOperatorType::GET_V);
    includeOps.push_back(planner::LogicalOperatorType::RECURSIVE_EXTEND);
    includeOps.push_back(planner::LogicalOperatorType::FILTER);
  } else {
    includeOps.push_back(planner::LogicalOperatorType::SCAN_NODE_TABLE);
    includeOps.push_back(planner::LogicalOperatorType::EXTEND);
    includeOps.push_back(planner::LogicalOperatorType::GET_V);
  }

  if (!checkOperatorType(rightChild, includeOps)) {
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
  if (rightScanNodeID->getUniqueName() != joinID->getUniqueName()) {
    return op;
  }
  if (joinOp->getJoinType() == common::JoinType::LEFT) {
    setOptional(rightChild);
  }
  // set the left plan as the child of the right plan, to flat the join
  // structure and make it as a chain
  rightScanParent->setChild(0, join->getChild(0));
  return rightChild;
}

}  // namespace optimizer
}  // namespace neug