#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"
#include "neug/compiler/planner/planner.h"

using namespace neug::common;
using namespace neug::binder;

namespace neug {
namespace planner {

static expression_vector removeInternalIDProperty(
    const expression_vector& expressions) {
  expression_vector result;
  for (auto expr : expressions) {
    if (expr->constCast<PropertyExpression>().isInternalID()) {
      continue;
    }
    result.push_back(expr);
  }
  return result;
}

void Planner::appendScanNodeTable(std::shared_ptr<Expression> nodeID,
                                  std::vector<table_id_t> tableIDs,
                                  const expression_vector& properties,
                                  LogicalPlan& plan, bool nodeIDScan) {
  auto propertiesToScan_ = removeInternalIDProperty(properties);
  auto scan = make_shared<LogicalScanNodeTable>(
      std::move(nodeID), std::move(tableIDs), propertiesToScan_);
  scan->setNodeIDScan(nodeIDScan);
  scan->computeFactorizedSchema();
  scan->setCardinality(cardinalityEstimator.estimateScanNode(*scan));
  plan.setLastOperator(std::move(scan));
}

}  // namespace planner
}  // namespace neug
