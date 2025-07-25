#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/copy/bound_copy_to.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/function/built_in_function_utils.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/copy.h"
#include "neug/compiler/parser/query/regular_query.h"

using namespace gs::common;
using namespace gs::parser;

namespace gs {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCopyToClause(
    const Statement& statement) {
  auto& copyToStatement = statement.constCast<CopyTo>();
  auto boundFilePath = copyToStatement.getFilePath();
  auto fileTypeInfo = bindFileTypeInfo({boundFilePath});
  std::vector<std::string> columnNames;
  auto parsedQuery =
      copyToStatement.getStatement()->constPtrCast<RegularQuery>();
  auto query = bindQuery(*parsedQuery);
  auto columns = query->getStatementResult()->getColumns();
  auto fileTypeStr = fileTypeInfo.fileTypeStr;
  auto name = stringFormat("COPY_{}", fileTypeStr);
  auto entry = clientContext->getCatalog()->getFunctionEntry(
      clientContext->getTransaction(), name);
  auto exportFunc = function::BuiltInFunctionsUtils::matchFunction(
                        name, entry->ptrCast<catalog::FunctionCatalogEntry>())
                        ->constPtrCast<function::ExportFunction>();
  for (auto& column : columns) {
    auto columnName =
        column->hasAlias() ? column->getAlias() : column->toString();
    columnNames.push_back(columnName);
  }
  function::ExportFuncBindInput bindInput{
      std::move(columnNames), std::move(boundFilePath),
      bindParsingOptions(copyToStatement.getParsingOptions())};
  auto bindData = exportFunc->bind(bindInput);
  return std::make_unique<BoundCopyTo>(std::move(bindData), *exportFunc,
                                       std::move(query));
}

}  // namespace binder
}  // namespace gs
