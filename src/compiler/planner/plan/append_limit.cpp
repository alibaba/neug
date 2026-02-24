#include "neug/compiler/planner/operator/logical_limit.h"
#include "neug/compiler/planner/planner.h"

namespace neug {
namespace planner {

void Planner::appendLimit(std::shared_ptr<binder::Expression> skipNum,
                          std::shared_ptr<binder::Expression> limitNum,
                          LogicalPlan& plan) {
  auto limit =
      make_shared<LogicalLimit>(skipNum, limitNum, plan.getLastOperator());
  appendFlattens(limit->getGroupsPosToFlatten(), plan);
  limit->setChild(0, plan.getLastOperator());
  limit->computeFactorizedSchema();
  plan.setLastOperator(std::move(limit));
}

}  // namespace planner
}  // namespace neug
