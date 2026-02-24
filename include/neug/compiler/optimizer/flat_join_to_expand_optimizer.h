#pragma once

#include "neug/compiler/optimizer/logical_operator_visitor.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/logical_plan.h"
namespace neug {
namespace optimizer {
class FlatJoinToExpandOptimizer : public LogicalOperatorVisitor {
 public:
  void rewrite(planner::LogicalPlan* plan);
  std::shared_ptr<planner::LogicalOperator> visitOperator(
      const std::shared_ptr<planner::LogicalOperator>& op);
  std::shared_ptr<planner::LogicalOperator> visitHashJoinReplace(
      std::shared_ptr<planner::LogicalOperator> op) override;
  bool checkOperatorType(
      std::shared_ptr<planner::LogicalOperator> op,
      const std::vector<planner::LogicalOperatorType>& types);
  std::shared_ptr<planner::LogicalOperator> getScanParent(
      std::shared_ptr<planner::LogicalOperator> parent);

 private:
  void setOptional(std::shared_ptr<planner::LogicalOperator> plan);
};
}  // namespace optimizer
}  // namespace neug