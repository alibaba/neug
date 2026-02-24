#include "neug/compiler/planner/operator/scan/logical_dummy_scan.h"
#include "neug/compiler/planner/planner.h"

namespace neug {
namespace planner {

void Planner::appendDummyScan(LogicalPlan& plan, bool updateClause) {
  NEUG_ASSERT(plan.isEmpty());
  auto dummyScan = std::make_shared<LogicalDummyScan>(updateClause);
  dummyScan->computeFactorizedSchema();
  plan.setLastOperator(std::move(dummyScan));
}

}  // namespace planner
}  // namespace neug
