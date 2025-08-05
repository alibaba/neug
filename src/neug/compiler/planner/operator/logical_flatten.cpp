#include "neug/compiler/planner/operator/logical_flatten.h"

using namespace gs::common;

namespace gs {
namespace planner {

void LogicalFlatten::computeFactorizedSchema() {
  copyChildSchema(0);
  schema->flattenGroup(groupPos);
}

void LogicalFlatten::computeFlatSchema() {
  throw exception::InternalException(
      "LogicalFlatten::computeFlatSchema() should never be used.");
}

}  // namespace planner
}  // namespace gs
