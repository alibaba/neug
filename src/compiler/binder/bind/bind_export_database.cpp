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

#include "neug/compiler/binder/bound_export_database.h"
#include "neug/compiler/binder/query/bound_regular_query.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/catalog/catalog_entry/index_catalog_entry.h"
#include "neug/compiler/catalog/catalog_entry/node_table_catalog_entry.h"
#include "neug/compiler/catalog/catalog_entry/rel_table_catalog_entry.h"
#include "neug/compiler/common/file_system/virtual_file_system.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/parser.h"
#include "neug/compiler/parser/port_db.h"
#include "neug/compiler/parser/query/regular_query.h"
#include "neug/utils/exception/exception.h"

using namespace neug::binder;
using namespace neug::common;
using namespace neug::parser;
using namespace neug::catalog;
using namespace neug::transaction;
using namespace neug::storage;

namespace neug {
namespace binder {

static std::vector<ExportedTableData> getExportInfo(
    const Catalog& catalog, main::ClientContext* context, Binder* binder) {
  auto transaction = context->getTransaction();
  std::vector<ExportedTableData> exportData;
  for (auto tableEntry :
       catalog.getTableEntries(transaction, false /*useInternal*/)) {
    ExportedTableData tableData;
    if (binder->bindExportTableData(tableData, *tableEntry, catalog,
                                    transaction)) {
      exportData.push_back(std::move(tableData));
    }
  }
  for (auto indexEntry : catalog.getIndexEntries(transaction)) {
    ExportedTableData tableData;
    auto tableToExport = indexEntry->getTableEntryToExport(context);
    if (tableToExport == nullptr) {
      continue;
    }
    binder->bindExportTableData(tableData, *tableToExport, catalog,
                                transaction);
    exportData.push_back(std::move(tableData));
  }
  return exportData;
}

FileTypeInfo getFileType(case_insensitive_map_t<Value>& options) {
  auto fileTypeInfo = FileTypeInfo{FileType::PARQUET, "PARQUET"};
  if (options.contains("FORMAT")) {
    auto value = options.at("FORMAT");
    if (value.getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
      THROW_BINDER_EXCEPTION("The type of format option must be a string.");
    }
    auto valueStr = value.getValue<std::string>();
    StringUtils::toUpper(valueStr);
    fileTypeInfo = FileTypeInfo{FileTypeUtils::fromString(valueStr), valueStr};
    options.erase("FORMAT");
  }
  return fileTypeInfo;
}

static void bindExportNodeTableDataQuery(const TableCatalogEntry& entry,
                                         std::string& exportQuery) {
  exportQuery = stringFormat("match (a:`{}`) return a.*", entry.getName());
}

static void bindExportRelTableDataQuery(const TableCatalogEntry& entry,
                                        std::string& exportQuery,
                                        const Catalog& catalog,
                                        const Transaction* transaction) {
  auto relTableEntry = entry.constPtrCast<RelTableCatalogEntry>();
  auto& srcTableEntry =
      catalog.getTableCatalogEntry(transaction, relTableEntry->getSrcTableID())
          ->constCast<NodeTableCatalogEntry>();
  auto& dstTableEntry =
      catalog.getTableCatalogEntry(transaction, relTableEntry->getDstTableID())
          ->constCast<NodeTableCatalogEntry>();
  exportQuery =
      stringFormat("match (a:`{}`)-[r:`{}`]->(b:`{}`) return a.{},b.{},r.*;",
                   srcTableEntry.getName(), relTableEntry->getName(),
                   dstTableEntry.getName(), srcTableEntry.getPrimaryKeyName(),
                   dstTableEntry.getPrimaryKeyName());
}

static bool bindExportQuery(std::string& exportQuery,
                            const TableCatalogEntry& entry,
                            const Catalog& catalog,
                            const Transaction* transaction) {
  switch (entry.getTableType()) {
  case TableType::NODE: {
    bindExportNodeTableDataQuery(entry, exportQuery);
  } break;
  case TableType::REL: {
    bindExportRelTableDataQuery(entry, exportQuery, catalog, transaction);
  } break;
  default:
    return false;
  }
  return true;
}

bool Binder::bindExportTableData(ExportedTableData& tableData,
                                 const TableCatalogEntry& entry,
                                 const Catalog& catalog,
                                 const Transaction* transaction) {
  std::string exportQuery;
  tableData.tableName = entry.getName();
  if (!bindExportQuery(exportQuery, entry, catalog, transaction)) {
    return false;
  }
  auto parsedStatement = Parser::parseQuery(exportQuery);
  NEUG_ASSERT(parsedStatement.size() == 1);
  auto parsedQuery = parsedStatement[0]->constPtrCast<RegularQuery>();
  clientContext->setUseInternalCatalogEntry(true /* useInternalCatalogEntry */);
  auto query = bindQuery(*parsedQuery);
  clientContext->setUseInternalCatalogEntry(
      false /* useInternalCatalogEntry */);
  auto columns = query->getStatementResult()->getColumns();
  for (auto& column : columns) {
    auto columnName =
        column->hasAlias() ? column->getAlias() : column->toString();
    tableData.columnNames.push_back(columnName);
    tableData.columnTypes.push_back(column->getDataType().copy());
  }
  tableData.regularQuery = std::move(query);
  return true;
}

std::unique_ptr<BoundStatement> Binder::bindExportDatabaseClause(
    const Statement& statement) {
  auto& exportDB = statement.constCast<ExportDB>();
  auto boundFilePath = clientContext->getVFSUnsafe()->expandPath(
      clientContext, exportDB.getFilePath());
  auto exportData =
      getExportInfo(*clientContext->getCatalog(), clientContext, this);
  auto parsedOptions = bindParsingOptions(exportDB.getParsingOptionsRef());
  auto fileTypeInfo = getFileType(parsedOptions);
  switch (fileTypeInfo.fileType) {
  case FileType::CSV:
  case FileType::PARQUET:
    break;
  default:
    THROW_BINDER_EXCEPTION(
        "Export database currently only supports csv and parquet files.");
  }
  if (fileTypeInfo.fileType != FileType::CSV && parsedOptions.size() != 0) {
    THROW_BINDER_EXCEPTION("Only export to csv can have options.");
  }
  return std::make_unique<BoundExportDatabase>(boundFilePath, fileTypeInfo,
                                               std::move(exportData),
                                               std::move(parsedOptions));
}

}  // namespace binder
}  // namespace neug
