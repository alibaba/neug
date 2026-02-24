#include "neug/compiler/planner/operator/scan/logical_dummy_scan.h"

#include "neug/compiler/binder/expression/literal_expression.h"
#include "neug/compiler/common/constants.h"

using namespace neug::common;

namespace neug {
namespace planner {

void LogicalDummyScan::computeFactorizedSchema() {
  createEmptySchema();
  schema->createGroup();
}

void LogicalDummyScan::computeFlatSchema() {
  createEmptySchema();
  schema->createGroup();
}

std::shared_ptr<binder::Expression> LogicalDummyScan::getDummyExpression() {
  return std::make_shared<binder::LiteralExpression>(
      Value::createNullValue(LogicalType::STRING()),
      InternalKeyword::PLACE_HOLDER);
}

}  // namespace planner
}  // namespace neug
