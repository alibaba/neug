#include "src/include/planner/operator/logical_distinct.h"
#include "src/include/planner/planner.h"

using namespace gs::binder;

namespace gs {
namespace planner {

void Planner::appendDistinct(const expression_vector& keys, LogicalPlan& plan) {
  auto distinct = make_shared<LogicalDistinct>(keys, plan.getLastOperator());
  appendFlattens(distinct->getGroupsPosToFlatten(), plan);
  distinct->setChild(0, plan.getLastOperator());
  distinct->computeFactorizedSchema();
  plan.setLastOperator(std::move(distinct));
}

}  // namespace planner
}  // namespace gs
