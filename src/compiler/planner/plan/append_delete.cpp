#include "neug/compiler/binder/query/updating_clause/bound_delete_info.h"
#include "neug/compiler/planner/operator/persistent/logical_delete.h"
#include "neug/compiler/planner/planner.h"

using namespace neug::binder;

namespace neug {
namespace planner {

void Planner::appendDelete(const std::vector<BoundDeleteInfo>& infos,
                           LogicalPlan& plan) {
  auto delete_ = std::make_shared<LogicalDelete>(copyVector(infos),
                                                 plan.getLastOperator());
  appendFlattens(delete_->getGroupsPosToFlatten(), plan);
  delete_->setChild(0, plan.getLastOperator());
  delete_->computeFactorizedSchema();
  plan.setLastOperator(std::move(delete_));
}

}  // namespace planner
}  // namespace neug
