#pragma once

#include "neug/compiler/optimizer/logical_operator_visitor.h"
#include "neug/compiler/planner/operator/logical_plan.h"
namespace gs {
namespace optimizer {
class SchemaPopulator : public LogicalOperatorVisitor {
 public:
  void rewrite(planner::LogicalPlan* plan);
};
}  // namespace optimizer
}  // namespace gs
