#pragma once

#include "neug/compiler/planner/operator/extend/logical_recursive_extend.h"
#include "neug/compiler/planner/operator/logical_plan.h"

namespace neug {
namespace planner {

class CostModel {
 public:
  static uint64_t computeExtendCost(const LogicalPlan& childPlan);
  static uint64_t computeHashJoinCost(
      const std::vector<binder::expression_pair>& joinConditions,
      const LogicalPlan& probe, const LogicalPlan& build);
  static uint64_t computeHashJoinCost(
      const binder::expression_vector& joinNodeIDs, const LogicalPlan& probe,
      const LogicalPlan& build);
  static uint64_t computeMarkJoinCost(
      const std::vector<binder::expression_pair>& joinConditions,
      const LogicalPlan& probe, const LogicalPlan& build);
  static uint64_t computeMarkJoinCost(
      const binder::expression_vector& joinNodeIDs, const LogicalPlan& probe,
      const LogicalPlan& build);
  static uint64_t computeIntersectCost(
      const LogicalPlan& probePlan,
      const std::vector<std::unique_ptr<LogicalPlan>>& buildPlans);
  static cardinality_t estimateIntersectCostByCard(
      const neug::planner::LogicalPlan& probePlan,
      const std::vector<cardinality_t>& buildCards);
  static uint64_t computeGetVCost(
      const planner::LogicalRecursiveExtend& extendOp);
  static uint64_t computeGetVCost(const planner::LogicalExtend& extendOp);
};

}  // namespace planner
}  // namespace neug
