#include "neug/compiler/planner/operator/logical_accumulate.h"
#include "neug/compiler/planner/planner.h"

using namespace neug::binder;
using namespace neug::common;

namespace neug {
namespace planner {

void Planner::tryAppendAccumulate(LogicalPlan& plan) {
  if (plan.getLastOperator()->getOperatorType() ==
      LogicalOperatorType::ACCUMULATE) {
    return;
  }
  appendAccumulate(plan);
}

void Planner::appendAccumulate(LogicalPlan& plan) {
  appendAccumulate(AccumulateType::REGULAR, expression_vector{},
                   nullptr /* mark */, plan);
}

void Planner::appendOptionalAccumulate(std::shared_ptr<Expression> mark,
                                       LogicalPlan& plan) {
  appendAccumulate(AccumulateType::OPTIONAL_, expression_vector{}, mark, plan);
}

void Planner::appendAccumulate(const expression_vector& flatExprs,
                               LogicalPlan& plan) {
  appendAccumulate(AccumulateType::REGULAR, flatExprs, nullptr /* mark */,
                   plan);
}

void Planner::appendAccumulate(AccumulateType accumulateType,
                               const expression_vector& flatExprs,
                               std::shared_ptr<Expression> mark,
                               LogicalPlan& plan) {
  // do nothing, accumulate operator is unnecessary for neug execution, here we
  // skip to generate the operator
}

}  // namespace planner
}  // namespace neug
