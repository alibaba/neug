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
  auto& boundExportDatabase = statement.constCast<BoundExportDatabase>();
  auto filePath = boundExportDatabase.getFilePath();
  auto fileType = boundExportDatabase.getFileType();
  auto exportData = boundExportDatabase.getExportData();
  auto logicalOperators = std::vector<std::shared_ptr<LogicalOperator>>();
  auto plan = std::make_unique<LogicalPlan>();
  auto fileTypeStr = FileTypeUtils::toString(fileType);
  StringUtils::toLower(fileTypeStr);
  auto copyToSuffix = "." + fileTypeStr;
  std::string name =
      common::stringFormat("COPY_{}", FileTypeUtils::toString(fileType));
  auto entry = clientContext->getCatalog()->getFunctionEntry(
      clientContext->getTransaction(), name);
  auto func = function::BuiltInFunctionsUtils::matchFunction(
      name, entry->ptrCast<FunctionCatalogEntry>());
  NEUG_ASSERT(func != nullptr);
  auto exportFunc = *func->constPtrCast<function::NeugCallFunction>();
  for (auto& exportTableData : *exportData) {
    auto regularQuery = exportTableData.getRegularQuery();
    NEUG_ASSERT(regularQuery->getStatementType() == StatementType::QUERY);
    auto tablePlan = getBestPlan(*regularQuery);
    auto path = filePath + "/" + exportTableData.tableName + copyToSuffix;
    auto bindData = std::make_unique<function::ExportFuncBindData>(
        exportTableData.columnNames, std::move(path));
    auto copyTo = std::make_shared<LogicalCopyTo>(
        std::move(bindData), exportFunc, tablePlan->getLastOperator());
    logicalOperators.push_back(std::move(copyTo));
  }
  auto exportDatabase = make_shared<LogicalExportDatabase>(
      boundExportDatabase.getBoundFileInfo()->copy(),
      statement.getStatementResult()->getSingleColumnExpr(),
      std::move(logicalOperators));
  plan->setLastOperator(std::move(exportDatabase));
  return plan;
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
