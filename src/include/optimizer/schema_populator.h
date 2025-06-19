#pragma once

#include "src/include/optimizer/logical_operator_visitor.h"
#include "src/include/planner/operator/logical_plan.h"
namespace gs {
namespace optimizer {
class SchemaPopulator : public LogicalOperatorVisitor {
 public:
  void rewrite(planner::LogicalPlan* plan);
};
}  // namespace optimizer
}  // namespace gs
