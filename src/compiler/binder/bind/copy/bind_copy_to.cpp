/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/copy/bound_copy_to.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/function/built_in_function_utils.h"
#include "neug/compiler/function/export/export_function.h"
#include "neug/compiler/function/table/table_function.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/copy.h"
#include "neug/compiler/parser/query/regular_query.h"

using namespace neug::common;
using namespace neug::parser;

namespace neug {
namespace binder {

/**
 * @brief Get the export function by file type info
 * Create table function directly if file type is CSV (CSV will be integrated
 * into extension framework later), Otherwise, get the function from catalog
 * which has been registered into extension system.
 * @param typeInfo
 * @return function::TableFunction
 */
function::ExportFunction Binder::getExportFunction(
    const common::FileTypeInfo& typeInfo) {
  auto fileTypeStr = typeInfo.fileTypeStr;
  std::transform(fileTypeStr.begin(), fileTypeStr.end(), fileTypeStr.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  auto name = stringFormat("COPY_{}", fileTypeStr);
  auto entry = clientContext->getCatalog()->getFunctionEntry(
      clientContext->getTransaction(), name);
  return *function::BuiltInFunctionsUtils::matchFunction(
              name, entry->ptrCast<catalog::FunctionCatalogEntry>())
              ->constPtrCast<function::ExportFunction>();
}

/**
 * @brief Get the export function bind data by file type info
 * Create ExportCSVBindData specifically if file type is CSV (CSV will be
 * integrated into extension framework later), Otherwise, create
 * ExportFuncBindData for other common cases.
 * @param typeInfo
 * @param bindInput
 * @return std::unique_ptr<function::ExportFuncBindData>
 */
std::unique_ptr<function::ExportFuncBindData> Binder::getExportFuncBindData(
    const common::FileTypeInfo& typeInfo,
    const function::ExportFuncBindInput& bindInput) {
  return std::make_unique<function::ExportFuncBindData>(
      bindInput.columnNames, bindInput.filePath, bindInput.parsingOptions);
}

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
  auto exportFunc = getExportFunction(fileTypeInfo);
  for (auto& column : columns) {
    auto columnName =
        column->hasAlias() ? column->getAlias() : column->toString();
    columnNames.push_back(columnName);
  }
  function::ExportFuncBindInput bindInput{
      std::move(columnNames), std::move(boundFilePath),
      bindParsingOptions(copyToStatement.getParsingOptions())};
  auto bindData = getExportFuncBindData(fileTypeInfo, bindInput);
  return std::make_unique<BoundCopyTo>(std::move(bindData), exportFunc,
                                       std::move(query));
}

}  // namespace binder
}  // namespace neug
