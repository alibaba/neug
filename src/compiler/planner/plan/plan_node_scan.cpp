#include "neug/compiler/main/client_context.h"
#include "neug/compiler/planner/planner.h"

using namespace gs::binder;

namespace gs {
namespace planner {

LogicalPlan Planner::getNodePropertyScanPlan(const NodeExpression& node) {
  auto properties = getProperties(node);
  auto scanPlan = LogicalPlan();
  if (properties.empty()) {
    return scanPlan;
  }
  // Because the node is not
  cardinalityEstimator.addNodeIDDomAndStats(clientContext->getTransaction(),
                                            *node.getInternalID(),
                                            node.getTableIDs());
  appendScanNodeTable(node.getInternalID(), node.getTableIDs(), properties,
                      scanPlan);
  return scanPlan;
}

}  // namespace planner
}  // namespace gs
