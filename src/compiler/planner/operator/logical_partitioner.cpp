#include "neug/compiler/planner/operator/logical_partitioner.h"

#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace planner {

static void validateSingleGroup(const Schema& schema) {
  if (schema.getNumGroups() != 1) {
    THROW_RUNTIME_ERROR(
        "Try to partition multiple factorization group. This should not "
        "happen.");
  }
}

void LogicalPartitioner::computeFactorizedSchema() {
  copyChildSchema(0);
  // LCOV_EXCL_START
  validateSingleGroup(*schema);
  // LCOV_EXCL_STOP
  schema->insertToGroupAndScope(info.offset, 0);
}

void LogicalPartitioner::computeFlatSchema() {
  copyChildSchema(0);
  // LCOV_EXCL_START
  validateSingleGroup(*schema);
  // LCOV_EXCL_STOP
  schema->insertToGroupAndScope(info.offset, 0);
}

std::string LogicalPartitioner::getExpressionsForPrinting() const {
  binder::expression_vector expressions;
  for (auto& partitioningInfo : info.partitioningInfos) {
    expressions.push_back(copyFromInfo.columnExprs[partitioningInfo.keyIdx]);
  }
  return binder::ExpressionUtil::toString(expressions);
}

}  // namespace planner
}  // namespace neug
