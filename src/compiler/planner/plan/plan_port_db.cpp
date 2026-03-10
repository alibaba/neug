#include <memory>
#include "neug/compiler/binder/bound_export_database.h"
#include "neug/compiler/binder/bound_import_database.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/function/built_in_function_utils.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/planner/operator/persistent/logical_copy_to.h"
#include "neug/compiler/planner/operator/simple/logical_export_db.h"
#include "neug/compiler/planner/operator/simple/logical_import_db.h"
#include "neug/compiler/planner/planner.h"

using namespace neug::binder;
using namespace neug::storage;
using namespace neug::catalog;
using namespace neug::common;
using namespace neug::transaction;

namespace neug {
namespace planner {

std::unique_ptr<LogicalPlan> Planner::planExportDatabase(
    const BoundStatement& statement) {
  return nullptr;
}

std::unique_ptr<LogicalPlan> Planner::planImportDatabase(
    const BoundStatement& statement) {
  auto& boundImportDatabase = statement.constCast<BoundImportDatabase>();
  auto plan = std::make_unique<LogicalPlan>();
  auto importDatabase = make_shared<LogicalImportDatabase>(
      boundImportDatabase.getQuery(), boundImportDatabase.getIndexQuery(),
      statement.getStatementResult()->getSingleColumnExpr());
  plan->setLastOperator(std::move(importDatabase));
  return plan;
}

}  // namespace planner
}  // namespace neug
