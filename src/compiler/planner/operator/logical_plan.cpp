#include "neug/compiler/planner/operator/logical_plan.h"

namespace neug {
namespace planner {

bool LogicalPlan::hasUpdate() const {
  return lastOperator->hasUpdateRecursive();
}

std::unique_ptr<LogicalPlan> LogicalPlan::shallowCopy() const {
  auto plan = std::make_unique<LogicalPlan>();
  plan->lastOperator = lastOperator;  // shallow copy sub-plan
  plan->cost = cost;
  return plan;
}

std::unique_ptr<LogicalPlan> LogicalPlan::deepCopy() const {
  NEUG_ASSERT(!isEmpty());
  auto plan = std::make_unique<LogicalPlan>();
  plan->lastOperator = lastOperator->copy();  // deep copy sub-plan
  plan->cost = cost;
  return plan;
}

}  // namespace planner
}  // namespace neug
