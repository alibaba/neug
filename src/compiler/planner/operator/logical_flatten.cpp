#include "neug/compiler/planner/operator/logical_flatten.h"

using namespace neug::common;

namespace neug {
namespace planner {

void LogicalFlatten::computeFactorizedSchema() {
  copyChildSchema(0);
  schema->flattenGroup(groupPos);
}

void LogicalFlatten::computeFlatSchema() {
  THROW_INTERNAL_EXCEPTION(
      "LogicalFlatten::computeFlatSchema() should never be used.");
}

}  // namespace planner
}  // namespace neug
