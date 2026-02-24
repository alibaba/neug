#include "neug/compiler/planner/join_order/cost_model.h"

#include <iostream>

#include "neug/compiler/common/constants.h"
#include "neug/compiler/planner/join_order/join_order_util.h"
#include "neug/compiler/planner/operator/logical_hash_join.h"

using namespace neug::common;

namespace neug {
namespace planner {

uint64_t CostModel::computeExtendCost(const LogicalPlan& childPlan) {
  return childPlan.getCost() + childPlan.getCardinality();
}

uint64_t CostModel::computeHashJoinCost(
    const std::vector<binder::expression_pair>& joinConditions,
    const LogicalPlan& probe, const LogicalPlan& build) {
  return computeHashJoinCost(LogicalHashJoin::getJoinNodeIDs(joinConditions),
                             probe, build);
}

uint64_t CostModel::computeHashJoinCost(
    const binder::expression_vector& joinNodeIDs, const LogicalPlan& probe,
    const LogicalPlan& build) {
  uint64_t cost = 0ul;
  cost += probe.getCost();
  cost += build.getCost();
  return cost;
}

uint64_t CostModel::computeMarkJoinCost(
    const std::vector<binder::expression_pair>& joinConditions,
    const LogicalPlan& probe, const LogicalPlan& build) {
  return computeMarkJoinCost(LogicalHashJoin::getJoinNodeIDs(joinConditions),
                             probe, build);
}

uint64_t CostModel::computeMarkJoinCost(
    const binder::expression_vector& joinNodeIDs, const LogicalPlan& probe,
    const LogicalPlan& build) {
  return computeHashJoinCost(joinNodeIDs, probe, build);
}

uint64_t CostModel::computeIntersectCost(
    const neug::planner::LogicalPlan& probePlan,
    const std::vector<std::unique_ptr<LogicalPlan>>& buildPlans) {
  uint64_t cost = 0ul;
  cost += probePlan.getCost();
  // TODO(Xiyang): think of how to calculate intersect cost such that it will be
  // picked in worst case.
  cost += probePlan.getCardinality();
  for (auto& buildPlan : buildPlans) {
    NEUG_ASSERT(buildPlan->getCardinality() >= 1);
    cost += buildPlan->getLastOperator()->getIntersectCard();
  }
  return cost;
}

cardinality_t CostModel::estimateIntersectCostByCard(
    const neug::planner::LogicalPlan& probePlan,
    const std::vector<cardinality_t>& buildCards) {
  uint64_t cost = 0ul;
  cost += probePlan.getCost();
  cost += probePlan.getCardinality();
  for (auto& card : buildCards) {
    cost += card;
  }
  return cost;
}

// Currently, we directly calculate the edge cost based on the number of triples
// <src, dst, edge>, which has already been computed in the extend operator.
// Here, getV simply returns 0.
uint64_t CostModel::computeGetVCost(const planner::LogicalExtend& extendOp) {
  // bool hasLabelFiltering = optimizer::ExpandGetVFusion::hasLabelFiltering(
  //     gopt::GNodeType{*extendOp.getNbrNode()},
  //     gopt::GRelType{*extendOp.getRel()},
  //     gopt::GNodeType{*extendOp.getBoundNode()}, extendOp.getDirection(),
  //     clientContext->getCatalog());
  // return hasLabelFiltering ? extendOp.getCardinality() : 0;
  return 0;
}

uint64_t CostModel::computeGetVCost(
    const planner::LogicalRecursiveExtend& extendOp) {
  // return extendOp.getCardinality();
  return 0;
}

}  // namespace planner
}  // namespace neug
