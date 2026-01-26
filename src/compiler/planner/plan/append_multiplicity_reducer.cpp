#include "neug/compiler/planner/operator/logical_multiplcity_reducer.h"
#include "neug/compiler/planner/planner.h"

namespace neug {
namespace planner {

void Planner::appendMultiplicityReducer(LogicalPlan& plan) {
  auto multiplicityReducer =
      make_shared<LogicalMultiplicityReducer>(plan.getLastOperator());
  multiplicityReducer->computeFactorizedSchema();
  plan.setLastOperator(std::move(multiplicityReducer));
}

}  // namespace planner
}  // namespace neug
