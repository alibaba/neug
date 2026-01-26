#include <memory>
#include "neug/compiler/binder/bound_table_scan_info.h"
#include "neug/compiler/binder/copy/bound_copy_from.h"
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/query/reading_clause/bound_table_function_call.h"
#include "neug/compiler/planner/operator/logical_table_function_call.h"
#include "neug/compiler/planner/planner.h"

using namespace neug::binder;

namespace neug {
namespace planner {

void Planner::appendTableFunctionCall(const BoundTableScanInfo& info,
                                      LogicalPlan& plan) {
  std::shared_ptr<LogicalTableFunctionCall> call =
      std::make_shared<LogicalTableFunctionCall>(info.func,
                                                 info.bindData->copy());
  call->computeFactorizedSchema();
  plan.setLastOperator(std::move(call));
}

std::shared_ptr<LogicalOperator> Planner::getTableFunctionCall(
    const BoundTableScanInfo& info) {
  auto call = std::make_shared<LogicalTableFunctionCall>(info.func,
                                                         info.bindData->copy());
  call->computeFactorizedSchema();
  return call;
}

std::shared_ptr<LogicalOperator> Planner::getTableFunctionCall(
    const BoundReadingClause& readingClause) {
  auto& call = readingClause.constCast<BoundTableFunctionCall>();
  auto op = std::make_shared<LogicalTableFunctionCall>(
      call.getTableFunc(), call.getBindData()->copy());
  op->computeFactorizedSchema();
  return op;
}

}  // namespace planner
}  // namespace neug
