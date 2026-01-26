#pragma once
#include "neug/compiler/optimizer/logical_operator_visitor.h"
#include "neug/compiler/planner/operator/logical_plan.h"
namespace neug {
namespace optimizer {
/**
 * To guarantee all subqueries have the same ouput schema by adding
 * LogicalAliasMap at the tail.
 */
class UnionAliasMapOptimizer : public LogicalOperatorVisitor {
 public:
  void rewrite(planner::LogicalPlan* plan);
  std::shared_ptr<planner::LogicalOperator> visitOperator(
      const std::shared_ptr<planner::LogicalOperator>& op);
  std::shared_ptr<planner::LogicalOperator> visitUnionReplace(
      std::shared_ptr<planner::LogicalOperator> op) override;
};
}  // namespace optimizer
}  // namespace neug