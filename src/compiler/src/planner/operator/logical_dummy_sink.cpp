#include "planner/operator/logical_dummy_sink.h"

namespace gs {
namespace planner {

void LogicalDummySink::computeFactorizedSchema() {
  createEmptySchema();
  copyChildSchema(0);
}

void LogicalDummySink::computeFlatSchema() {
  createEmptySchema();
  copyChildSchema(0);
}

}  // namespace planner
}  // namespace gs
