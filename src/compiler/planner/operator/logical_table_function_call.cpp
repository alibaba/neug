#include "neug/compiler/planner/operator/logical_table_function_call.h"

namespace gs {
namespace planner {

void LogicalTableFunctionCall::computeFlatSchema() {
  createEmptySchema();
  if (bindData) {
    auto groupPos = schema->createGroup();
    for (auto& expr : bindData->columns) {
      schema->insertToGroupAndScope(expr, groupPos);
    }
  } else if (!callOutput.empty()) {
    auto groupPos = schema->createGroup();
    for (auto& expr : callOutput) {
      schema->insertToGroupAndScope(expr, groupPos);
    }
  }
}

void LogicalTableFunctionCall::computeFactorizedSchema() {
  createEmptySchema();
  if (bindData) {
    auto groupPos = schema->createGroup();
    for (auto& expr : bindData->columns) {
      schema->insertToGroupAndScope(expr, groupPos);
    }
  } else if (!callOutput.empty()) {
    auto groupPos = schema->createGroup();
    for (auto& expr : callOutput) {
      schema->insertToGroupAndScope(expr, groupPos);
    }
  }
}

}  // namespace planner
}  // namespace gs
