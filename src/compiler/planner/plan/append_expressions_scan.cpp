#include "neug/compiler/planner/operator/scan/logical_expressions_scan.h"
#include "neug/compiler/planner/planner.h"

using namespace neug::binder;

namespace neug {
namespace planner {

void Planner::appendExpressionsScan(const expression_vector& expressions,
                                    LogicalPlan& plan) {
  auto expressionsScan = std::make_shared<LogicalExpressionsScan>(expressions);
  expressionsScan->computeFactorizedSchema();
  plan.setLastOperator(expressionsScan);
}

}  // namespace planner
}  // namespace neug
