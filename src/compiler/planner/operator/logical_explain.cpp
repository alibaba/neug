#include "neug/compiler/planner/operator/logical_explain.h"

using namespace neug::common;

namespace neug {
namespace planner {

void LogicalExplain::computeSchema() {
  switch (explainType) {
  case ExplainType::PROFILE:
    copyChildSchema(0);
    break;
  case ExplainType::PHYSICAL_PLAN:
  case ExplainType::LOGICAL_PLAN:
    createEmptySchema();
    break;
  default:
    NEUG_UNREACHABLE;
  }
}

void LogicalExplain::computeFlatSchema() {
  computeSchema();
  schema->createGroup();
  schema->insertToGroupAndScope(outputExpression, 0);
}

void LogicalExplain::computeFactorizedSchema() {
  computeSchema();
  auto groupPos = schema->createGroup();
  schema->insertToGroupAndScope(outputExpression, groupPos);
  schema->setGroupAsSingleState(groupPos);
}

}  // namespace planner
}  // namespace neug
