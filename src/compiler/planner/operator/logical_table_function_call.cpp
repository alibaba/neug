#include "neug/compiler/planner/operator/logical_table_function_call.h"
#include "neug/compiler/binder/expression/expression.h"

namespace neug {
namespace planner {

void LogicalTableFunctionCall::computeFlatSchema() {
  createEmptySchema();
  auto groupPos = schema->createGroup();
  auto columnSkips = bindData->getColumnSkips();
  if (!columnSkips.empty()) {
    for (auto idx = 0; idx < bindData->columns.size(); idx++) {
      if (!columnSkips[idx]) {
        schema->insertToGroupAndScope(bindData->columns[idx], groupPos);
      }
    }
    return;
  }
  for (auto& expr : bindData->columns) {
    schema->insertToGroupAndScope(expr, groupPos);
  }
}

void LogicalTableFunctionCall::computeFactorizedSchema() {
  createEmptySchema();
  auto groupPos = schema->createGroup();
  for (auto& expr : bindData->columns) {
    schema->insertToGroupAndScope(expr, groupPos);
  }
}

}  // namespace planner
}  // namespace neug
