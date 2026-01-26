#include "neug/compiler/planner/operator/logical_dummy_sink.h"

namespace neug {
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
}  // namespace neug
