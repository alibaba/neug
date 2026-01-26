#include "neug/compiler/planner/operator/simple/logical_simple.h"

namespace neug {
namespace planner {

void LogicalSimple::computeFlatSchema() {
  createEmptySchema();
  schema->createGroup();
  schema->insertToGroupAndScope(outputExpression, 0);
}

void LogicalSimple::computeFactorizedSchema() {
  createEmptySchema();
  auto groupPos = schema->createGroup();
  schema->insertToGroupAndScope(outputExpression, groupPos);
  schema->setGroupAsSingleState(groupPos);
}

}  // namespace planner
}  // namespace neug
