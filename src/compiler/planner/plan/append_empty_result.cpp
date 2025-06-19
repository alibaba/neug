#include "src/include/planner/operator/logical_empty_result.h"
#include "src/include/planner/planner.h"

namespace gs {
namespace planner {

void Planner::appendEmptyResult(LogicalPlan& plan) {
  auto op = std::make_shared<LogicalEmptyResult>(*plan.getSchema());
  op->computeFactorizedSchema();
  plan.setLastOperator(std::move(op));
}

}  // namespace planner
}  // namespace gs
