#include "neug/compiler/optimizer/flat_join_to_expand_optimizer.h"
#include <iostream>
#include <memory>
#include "neug/compiler/common/enums/join_type.h"
#include "neug/compiler/planner/operator/extend/logical_extend.h"
#include "neug/compiler/planner/operator/logical_hash_join.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"

namespace gs {
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

bool hasFilter(std::shared_ptr<planner::LogicalOperator> op) {
  if (op->getOperatorType() == planner::LogicalOperatorType::FILTER) {
    return true;
  }
  for (auto child : op->getChildren()) {
    if (hasFilter(child)) {
      return true;
    }
  }
  return false;
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

  // FlatJoinToExpandRule is applied after ExpandGetVFusionRule,
  // if the right plan still contains filtering, it means the edge optional
  // cannot be guaranteed
  if (joinOp->getJoinType() == common::JoinType::LEFT &&
      hasFilter(rightChild)) {
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
    std::cout << "set optional" << std::endl;
    setOptional(rightChild);
  }
  // set the left plan as the child of the right plan, to flat the join
  // structure and make it as a chain
  rightScanParent->setChild(0, join->getChild(0));
  return rightChild;
}

}  // namespace optimizer
}  // namespace gs