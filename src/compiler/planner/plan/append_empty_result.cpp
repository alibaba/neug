#include "neug/compiler/planner/operator/logical_empty_result.h"
#include "neug/compiler/planner/planner.h"

namespace neug {
namespace planner {

void Planner::appendEmptyResult(LogicalPlan& plan) {
  auto op = std::make_shared<LogicalEmptyResult>(*plan.getSchema());
  op->computeFactorizedSchema();
  plan.setLastOperator(std::move(op));
}

}  // namespace planner
}  // namespace neug
