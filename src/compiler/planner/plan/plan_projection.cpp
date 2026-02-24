#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression_visitor.h"
#include "neug/compiler/binder/query/return_with_clause/bound_projection_body.h"
#include "neug/compiler/planner/operator/scan/logical_dummy_scan.h"
#include "neug/compiler/planner/planner.h"

using namespace neug::binder;

namespace neug {
namespace planner {

void Planner::planProjectionBody(
    const BoundProjectionBody* projectionBody,
    const std::vector<std::unique_ptr<LogicalPlan>>& plans) {
  for (auto& plan : plans) {
    planProjectionBody(projectionBody, *plan);
  }
}

void Planner::planProjectionBody(const BoundProjectionBody* projectionBody,
                                 LogicalPlan& plan) {
  auto expressionsToProject = projectionBody->getProjectionExpressions();
  if (expressionsToProject.empty()) {
    return;
  }
  if (plan.isEmpty()) {  // e.g. RETURN 1, COUNT(2)
    // if the pre query is not null, we set updateClause as true to skip the
    // dummy scan in physical plan convertor
    bool updateClause = (this->preQueryPlan) ? true : false;
    appendDummyScan(plan, updateClause);
  }
  auto expressionsToAggregate = projectionBody->getAggregateExpressions();
  auto expressionsToGroupBy = projectionBody->getGroupByExpressions();
  if (!expressionsToAggregate.empty()) {
    planAggregate(expressionsToAggregate, expressionsToGroupBy, plan);
  }
  // We might order by an expression that is not in projection list, so after
  // order by we always need to append a projection. If distinct is presented in
  // projection list, we need to first append project to evaluate the list, then
  // take the distinct. Order by should always be the last operator (except for
  // skip/limit) because other operators will break the order.
  if (projectionBody->isDistinct() && projectionBody->hasOrderByExpressions()) {
    appendProjection(expressionsToProject, plan);
    appendDistinct(expressionsToProject, plan);
    planOrderBy(expressionsToProject, projectionBody->getOrderByExpressions(),
                projectionBody->getSortingOrders(), plan);
    appendProjection(expressionsToProject, plan);
  } else if (projectionBody->isDistinct()) {
    appendProjection(expressionsToProject, plan);
    appendDistinct(expressionsToProject, plan);
  } else if (projectionBody->hasOrderByExpressions()) {
    planOrderBy(expressionsToProject, projectionBody->getOrderByExpressions(),
                projectionBody->getSortingOrders(), plan);
    appendProjection(expressionsToProject, plan);
  } else {
    appendProjection(expressionsToProject, plan);
  }
  if (projectionBody->hasSkipOrLimit()) {
    appendMultiplicityReducer(plan);
    appendLimit(projectionBody->getSkipNumber(),
                projectionBody->getLimitNumber(), plan);
  }
}

void Planner::planAggregate(const expression_vector& expressionsToAggregate,
                            const expression_vector& expressionsToGroupBy,
                            LogicalPlan& plan) {
  NEUG_ASSERT(!expressionsToAggregate.empty());
  expression_vector expressionsToProject;
  for (auto& expressionToAggregate : expressionsToAggregate) {
    if (ExpressionChildrenCollector::collectChildren(*expressionToAggregate)
            .empty()) {  // skip COUNT(*)
      continue;
    }
    expressionsToProject.push_back(expressionToAggregate->getChild(0));
  }
  for (auto& expressionToGroupBy : expressionsToGroupBy) {
    expressionsToProject.push_back(expressionToGroupBy);
  }
  // remove duplication in pre projection before aggregate
  appendProjection(ExpressionUtil::removeDuplication(expressionsToProject),
                   plan);
  // guarantee the deduplication of each group key or value to avoid unnecessary
  // computation, and a projection will be added after aggregate to guarantee
  // the ouput schema
  appendAggregate(ExpressionUtil::removeDuplication(expressionsToGroupBy),
                  ExpressionUtil::removeDuplication(expressionsToAggregate),
                  plan);
}

void Planner::planOrderBy(const binder::expression_vector& expressionsToProject,
                          const binder::expression_vector& expressionsToOrderBy,
                          const std::vector<bool>& isAscOrders,
                          LogicalPlan& plan) {
  auto expressionsToProjectBeforeOrderBy = expressionsToProject;
  auto expressionsToProjectSet =
      expression_set{expressionsToProject.begin(), expressionsToProject.end()};
  for (auto& expression : expressionsToOrderBy) {
    if (!expressionsToProjectSet.contains(expression)) {
      expressionsToProjectBeforeOrderBy.push_back(expression);
    }
  }
  // remove duplication in pre projection before orderby
  appendProjection(
      ExpressionUtil::removeDuplication(expressionsToProjectBeforeOrderBy),
      plan);
  appendOrderBy(expressionsToOrderBy, isAscOrders, plan);
}

}  // namespace planner
}  // namespace neug
