#pragma once

#include "logical_operator_visitor.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/logical_plan.h"

namespace neug {
namespace optimizer {

// ProjectIntoDataSourceOptimizer pushes a Projection directly into
// LogicalTableFunctionCall when the pattern is:
//   LogicalTableFunctionCall -> LogicalProjection
// After rewrite, the Projection is removed and projected expressions are
// carried by LogicalTableFunctionCall::bindData->projectColumns.
class ProjectIntoDataSourceOptimizer : public LogicalOperatorVisitor {
 public:
  void rewrite(planner::LogicalPlan* plan);

 private:
  std::shared_ptr<planner::LogicalOperator> visitOperator(
      const std::shared_ptr<planner::LogicalOperator>& op);
  std::shared_ptr<planner::LogicalOperator> visitProjectionReplace(
      std::shared_ptr<planner::LogicalOperator> op) override;

 private:
  planner::LogicalPlan* plan;
};

}  // namespace optimizer
}  // namespace neug
