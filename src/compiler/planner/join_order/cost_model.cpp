#include "neug/compiler/planner/join_order/cost_model.h"

#include <iostream>

#include "neug/compiler/common/constants.h"
#include "neug/compiler/planner/join_order/join_order_util.h"
#include "neug/compiler/planner/operator/logical_hash_join.h"

using namespace gs::common;

namespace gs {
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
  // cost += probe.getCardinality();
  uint64_t flatCost = PlannerKnobs::BUILD_PENALTY *
                      JoinOrderUtil::getJoinKeysFlatCardinality(
                          joinNodeIDs, build.getLastOperatorRef());
  // std::cout << "flat cardinality: " << flatCost << std::endl << std::endl <<
  // std::endl;
  cost += flatCost;
  // cost += PlannerKnobs::BUILD_PENALTY * build.getCardinality();
  // std::cout << "probe cost: " << probe.getCost() << " build cost: " <<
  // build.getCost()
  //           << " flat cost: " << flatCost << " total cost:" << cost <<
  //           std::endl;
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
    const gs::planner::LogicalPlan& probePlan,
    const std::vector<std::unique_ptr<LogicalPlan>>& buildPlans) {
  uint64_t cost = 0ul;
  cost += probePlan.getCost();
  // TODO(Xiyang): think of how to calculate intersect cost such that it will be
  // picked in worst case.
  cost += probePlan.getCardinality();
  for (auto& buildPlan : buildPlans) {
    KU_ASSERT(buildPlan->getCardinality() >= 1);
    cost += buildPlan->getLastOperator()->getIntersectCard();
  }
  return cost;
}

cardinality_t CostModel::estimateIntersectCostByCard(
    const gs::planner::LogicalPlan& probePlan,
    const std::vector<cardinality_t>& buildCards) {
  uint64_t cost = 0ul;
  cost += probePlan.getCost();
  cost += probePlan.getCardinality();
  for (auto& card : buildCards) {
    cost += card;
  }
  return cost;
}

}  // namespace planner
}  // namespace gs
