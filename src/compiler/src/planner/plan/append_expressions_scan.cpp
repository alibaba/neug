#include "planner/operator/scan/logical_expressions_scan.h"
#include "planner/planner.h"

using namespace gs::binder;

namespace gs {
namespace planner {

void Planner::appendExpressionsScan(const expression_vector& expressions,
                                    LogicalPlan& plan) {
  auto expressionsScan = std::make_shared<LogicalExpressionsScan>(expressions);
  expressionsScan->computeFactorizedSchema();
  plan.setLastOperator(expressionsScan);
}

}  // namespace planner
}  // namespace gs
