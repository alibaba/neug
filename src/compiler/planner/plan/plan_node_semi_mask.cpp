#include "src/include/binder/expression/expression_util.h"
#include "src/include/binder/expression/property_expression.h"
#include "src/include/binder/expression_visitor.h"
#include "src/include/main/client_context.h"
#include "src/include/planner/operator/logical_dummy_sink.h"
#include "src/include/planner/operator/sip/logical_semi_masker.h"
#include "src/include/planner/planner.h"

using namespace gs::binder;
using namespace gs::common;

namespace gs {
namespace planner {

// Create a plan with a root semi masker for given node and node predicate.
LogicalPlan Planner::getNodeSemiMaskPlan(
    SemiMaskTargetType targetType, const NodeExpression& node,
    std::shared_ptr<Expression> nodePredicate) {
  auto plan = LogicalPlan();
  auto prevCollection = enterNewPropertyExprCollection();
  auto collector = PropertyExprCollector();
  collector.visit(nodePredicate);
  for (auto& expr :
       ExpressionUtil::removeDuplication(collector.getPropertyExprs())) {
    auto& propExpr = expr->constCast<PropertyExpression>();
    propertyExprCollection.addProperties(propExpr.getVariableName(), expr);
  }
  cardinalityEstimator.addNodeIDDomAndStats(clientContext->getTransaction(),
                                            *node.getInternalID(),
                                            node.getTableIDs());
  appendScanNodeTable(node.getInternalID(), node.getTableIDs(),
                      getProperties(node), plan);
  appendFilter(nodePredicate, plan);
  exitPropertyExprCollection(std::move(prevCollection));
  auto semiMasker = std::make_shared<LogicalSemiMasker>(
      SemiMaskKeyType::NODE, targetType, node.getInternalID(),
      node.getTableIDs(), plan.getLastOperator());
  semiMasker->computeFactorizedSchema();
  auto dummySink = std::make_shared<LogicalDummySink>(semiMasker);
  dummySink->computeFactorizedSchema();
  plan.setLastOperator(dummySink);
  return plan;
}

}  // namespace planner
}  // namespace gs
