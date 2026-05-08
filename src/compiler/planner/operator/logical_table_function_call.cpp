#include "neug/compiler/planner/operator/logical_table_function_call.h"
#include <vector>
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/expression_util.h"

namespace neug {
namespace planner {

void LogicalTableFunctionCall::computeFlatSchema() {
  createEmptySchema();
  auto groupPos = schema->createGroup();
  const auto& projectColumns = bindData->getProjectColumns();
  const auto& allColumns = bindData->columns;
  if (projectColumns.empty()) {
    for (auto& expr : allColumns) {
      schema->insertToGroupAndScope(expr, groupPos);
    }
  } else {
    for (const auto& name : projectColumns) {
      auto it = std::find_if(
          allColumns.begin(), allColumns.end(),
          [&name](const auto& column) { return column->rawName() == name; });
      if (it == allColumns.end()) {
        THROW_EXCEPTION_WITH_FILE_LINE(
            "Column not found in table function call: " + name);
      }
      schema->insertToGroupAndScope(*it, groupPos);
    }
  }
}

void LogicalTableFunctionCall::computeFactorizedSchema() {
  computeFlatSchema();
}

}  // namespace planner
}  // namespace neug
