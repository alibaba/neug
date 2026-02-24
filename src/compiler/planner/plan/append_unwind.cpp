#include "neug/compiler/binder/query/reading_clause/bound_unwind_clause.h"
#include "neug/compiler/planner/operator/logical_unwind.h"
#include "neug/compiler/planner/planner.h"

using namespace neug::binder;
using namespace neug::common;

namespace neug {
namespace planner {

void Planner::appendUnwind(const BoundReadingClause& readingClause,
                           LogicalPlan& plan) {
  auto& unwindClause =
      neug_dynamic_cast<const BoundUnwindClause&>(readingClause);
  auto unwind = make_shared<LogicalUnwind>(
      unwindClause.getInExpr(), unwindClause.getOutExpr(),
      unwindClause.getIDExpr(), plan.getLastOperator());
  appendFlattens(unwind->getGroupsPosToFlatten(), plan);
  unwind->setChild(0, plan.getLastOperator());
  unwind->computeFactorizedSchema();
  plan.setLastOperator(unwind);
}

}  // namespace planner
}  // namespace neug
