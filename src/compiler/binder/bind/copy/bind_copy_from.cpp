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
#include "neug/compiler/binder/copy/bound_copy_from.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/catalog/catalog_entry/node_table_catalog_entry.h"
#include "neug/compiler/catalog/catalog_entry/rel_group_catalog_entry.h"
#include "neug/compiler/catalog/catalog_entry/rel_table_catalog_entry.h"
#include "neug/compiler/common/string_format.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/copy.h"
#include "neug/utils/exception/exception.h"

using namespace neug::binder;
using namespace neug::catalog;
using namespace neug::common;
using namespace neug::parser;
using namespace neug::function;

namespace neug {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCopyFromClause(
    const Statement& statement) {
  auto& copyStatement = neug_dynamic_cast<const CopyFrom&>(statement);
  auto tableName = copyStatement.getTableName();
  auto catalog = clientContext->getCatalog();
  auto transaction = clientContext->getTransaction();
  if (catalog->containsRelGroup(transaction, tableName)) {
    auto entry = catalog->getRelGroupEntry(transaction, tableName);
    if (entry->getNumRelTables() == 1) {
      auto tableEntry = catalog->getTableCatalogEntry(
          transaction, entry->getRelTableIDs()[0]);
      return bindCopyRelFrom(statement,
                             tableEntry->ptrCast<RelTableCatalogEntry>());
    } else {
      auto options = bindParsingOptions(copyStatement.getParsingOptions());
      if (!options.contains(CopyConstants::FROM_OPTION_NAME) ||
          !options.contains(CopyConstants::TO_OPTION_NAME)) {
        THROW_BINDER_EXCEPTION(
            stringFormat("The table {} has multiple FROM and TO pairs defined "
                         "in the schema. A specific "
                         "pair of FROM and TO options is expected when copying "
                         "data into the {} table.",
                         tableName, tableName));
      }
      auto from =
          options.at(CopyConstants::FROM_OPTION_NAME).getValue<std::string>();
      auto to =
          options.at(CopyConstants::TO_OPTION_NAME).getValue<std::string>();
      auto relTableName =
          RelGroupCatalogEntry::getChildTableName(tableName, from, to);
      if (catalog->containsTable(transaction, relTableName)) {
        auto relEntry =
            catalog->getTableCatalogEntry(transaction, relTableName);
        return bindCopyRelFrom(statement,
                               relEntry->ptrCast<RelTableCatalogEntry>());
      }
    }
    THROW_BINDER_EXCEPTION(
        stringFormat("REL GROUP {} does not exist.", tableName));
  } else if (catalog->getTableCatalogEntry(transaction, tableName)) {
    auto tableEntry = catalog->getTableCatalogEntry(transaction, tableName);
    switch (tableEntry->getType()) {
    case CatalogEntryType::NODE_TABLE_ENTRY: {
      auto nodeTableEntry = tableEntry->ptrCast<NodeTableCatalogEntry>();
      return bindCopyNodeFrom(statement, nodeTableEntry);
    }
    case CatalogEntryType::REL_TABLE_ENTRY: {
      auto relTableEntry = tableEntry->ptrCast<RelTableCatalogEntry>();
      return bindCopyRelFrom(statement, relTableEntry);
    }
    default: {
      NEUG_UNREACHABLE;
    }
    }
  }
  THROW_BINDER_EXCEPTION(stringFormat("Table {} does not exist.", tableName));
}

static void bindExpectedNodeColumns(const NodeTableCatalogEntry* nodeTableEntry,
                                    const CopyFromColumnInfo& info,
                                    std::vector<std::string>& columnNames,
                                    std::vector<LogicalType>& columnTypes);
static void bindExpectedRelColumns(const RelTableCatalogEntry* relTableEntry,
                                   const CopyFromColumnInfo& info,
                                   std::vector<std::string>& columnNames,
                                   std::vector<LogicalType>& columnTypes,
                                   const main::ClientContext* context);

static std::pair<ColumnEvaluateType, std::shared_ptr<Expression>>
matchColumnExpression(const expression_vector& columns,
                      const PropertyDefinition& property,
                      ExpressionBinder& expressionBinder) {
  for (auto& column : columns) {
    if (property.getName() == column->toString()) {
      if (column->dataType == property.getType()) {
        return {ColumnEvaluateType::REFERENCE, column};
      } else {
        return {ColumnEvaluateType::CAST,
                expressionBinder.forceCast(column, property.getType())};
      }
    }
  }
  return {ColumnEvaluateType::DEFAULT,
          expressionBinder.bindExpression(*property.defaultExpr)};
}

std::unique_ptr<BoundStatement> Binder::bindCopyNodeFrom(
    const Statement& statement, NodeTableCatalogEntry* nodeTableEntry) {
  auto& copyStatement = neug_dynamic_cast<const CopyFrom&>(statement);
  // Bind expected columns based on catalog information.
  std::vector<std::string> expectedColumnNames;
  std::vector<LogicalType> expectedColumnTypes;
  bindExpectedNodeColumns(nodeTableEntry, copyStatement.getCopyColumnInfo(),
                          expectedColumnNames, expectedColumnTypes);
  auto boundSource = bindScanSource(copyStatement.getSource(),
                                    copyStatement.getParsingOptions(),
                                    expectedColumnNames, expectedColumnTypes);
  expression_vector columns =
      createVariables(expectedColumnNames, expectedColumnTypes);
  std::vector<ColumnEvaluateType> evaluateTypes(columns.size(),
                                                ColumnEvaluateType::REFERENCE);
  auto offset = createInvisibleVariable(
      std::string(InternalKeyword::ROW_OFFSET), LogicalType::INT64());
  auto boundCopyFromInfo = BoundCopyFromInfo(
      nodeTableEntry, std::move(boundSource), std::move(offset),
      std::move(columns), std::move(evaluateTypes), nullptr /* extraInfo */);
  return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

static options_t getScanSourceOptions(const CopyFrom& copyFrom) {
  options_t options;
  static case_insensitve_set_t copyFromPairsOptions = {
      CopyConstants::FROM_OPTION_NAME, CopyConstants::TO_OPTION_NAME};
  for (auto& option : copyFrom.getParsingOptions()) {
    if (copyFromPairsOptions.contains(option.first)) {
      continue;
    }
    options.emplace(option.first, option.second->copy());
  }
  return options;
}

std::unique_ptr<BoundStatement> Binder::bindCopyRelFrom(
    const Statement& statement, RelTableCatalogEntry* relTableEntry) {
  auto& copyStatement = statement.constCast<CopyFrom>();
  if (copyStatement.byColumn()) {
    THROW_BINDER_EXCEPTION(stringFormat(
        "Copy by column is not supported for relationship table."));
  }
  // Bind expected columns based on catalog information.
  std::vector<std::string> expectedColumnNames;
  std::vector<LogicalType> expectedColumnTypes;
  bindExpectedRelColumns(relTableEntry, copyStatement.getCopyColumnInfo(),
                         expectedColumnNames, expectedColumnTypes,
                         clientContext);
  auto boundSource = bindScanSource(copyStatement.getSource(),
                                    getScanSourceOptions(copyStatement),
                                    expectedColumnNames, expectedColumnTypes);
  expression_vector warningDataExprs;
  auto offset = createInvisibleVariable(
      std::string(InternalKeyword::ROW_OFFSET), LogicalType::INT64());
  auto srcTableID = relTableEntry->getSrcTableID();
  auto dstTableID = relTableEntry->getDstTableID();

  auto srcOffset = createVariable(std::string(InternalKeyword::SRC_OFFSET),
                                  LogicalType::INT64());
  auto dstOffset = createVariable(std::string(InternalKeyword::DST_OFFSET),
                                  LogicalType::INT64());
  expression_vector columns =
      createVariables(expectedColumnNames, expectedColumnTypes);
  std::vector<ColumnEvaluateType> evaluateTypes{columns.size(),
                                                ColumnEvaluateType::REFERENCE};
  std::shared_ptr<Expression> srcKey = nullptr, dstKey = nullptr;
  if (expectedColumnTypes[0] != columns[0]->getDataType()) {
    srcKey = expressionBinder.forceCast(columns[0], expectedColumnTypes[0]);
  } else {
    srcKey = columns[0];
  }
  if (expectedColumnTypes[1] != columns[1]->getDataType()) {
    dstKey = expressionBinder.forceCast(columns[1], expectedColumnTypes[1]);
  } else {
    dstKey = columns[1];
  }
  auto srcLookUpInfo =
      IndexLookupInfo(srcTableID, srcOffset, srcKey, warningDataExprs);
  auto dstLookUpInfo =
      IndexLookupInfo(dstTableID, dstOffset, dstKey, warningDataExprs);
  auto lookupInfos = std::vector<IndexLookupInfo>{srcLookUpInfo, dstLookUpInfo};
  auto internalIDColumnIndices = std::vector<idx_t>{0, 1, 2};
  auto extraCopyRelInfo = std::make_unique<ExtraBoundCopyRelInfo>(
      internalIDColumnIndices, lookupInfos);
  auto boundCopyFromInfo = BoundCopyFromInfo(
      relTableEntry, boundSource->copy(), offset, std::move(columns),
      std::move(evaluateTypes), std::move(extraCopyRelInfo));
  return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

static bool skipPropertyInFile(const PropertyDefinition& property) {
  if (property.getName() == InternalKeyword::ID) {
    return true;
  }
  return false;
}

static bool skipPropertyInSchema(const PropertyDefinition& property) {
  if (property.getType().getLogicalTypeID() == LogicalTypeID::SERIAL) {
    return true;
  }
  if (property.getName() == InternalKeyword::ID) {
    return true;
  }
  if (property.getName() == common::InternalKeyword::ID ||
      property.getName() == common::InternalKeyword::LABEL ||
      property.getName() == common::InternalKeyword::ROW_OFFSET ||
      property.getName() == common::InternalKeyword::SRC_OFFSET ||
      property.getName() == common::InternalKeyword::DST_OFFSET ||
      property.getName() == common::InternalKeyword::SRC ||
      property.getName() == common::InternalKeyword::DST) {
    return true;
  }
  return false;
}

static void bindExpectedColumns(const TableCatalogEntry* tableEntry,
                                const CopyFromColumnInfo& info,
                                std::vector<std::string>& columnNames,
                                std::vector<LogicalType>& columnTypes) {
  for (auto& property : tableEntry->getProperties()) {
    if (skipPropertyInSchema(property)) {
      continue;
    }
    columnNames.push_back(property.getName());
    columnTypes.push_back(property.getType().copy());
  }
}

void bindExpectedNodeColumns(const NodeTableCatalogEntry* nodeTableEntry,
                             const CopyFromColumnInfo& info,
                             std::vector<std::string>& columnNames,
                             std::vector<LogicalType>& columnTypes) {
  NEUG_ASSERT(columnNames.empty() && columnTypes.empty());
  bindExpectedColumns(nodeTableEntry, info, columnNames, columnTypes);
}

void bindExpectedRelColumns(const RelTableCatalogEntry* relTableEntry,
                            const CopyFromColumnInfo& info,
                            std::vector<std::string>& columnNames,
                            std::vector<LogicalType>& columnTypes,
                            const main::ClientContext* context) {
  NEUG_ASSERT(columnNames.empty() && columnTypes.empty());
  auto catalog = context->getCatalog();
  auto transaction = context->getTransaction();
  auto srcTable =
      catalog->getTableCatalogEntry(transaction, relTableEntry->getSrcTableID())
          ->ptrCast<NodeTableCatalogEntry>();
  auto dstTable =
      catalog->getTableCatalogEntry(transaction, relTableEntry->getDstTableID())
          ->ptrCast<NodeTableCatalogEntry>();
  columnNames.push_back("from");
  columnNames.push_back("to");
  auto srcPKColumnType = srcTable->getPrimaryKeyDefinition().getType().copy();
  if (srcPKColumnType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
    srcPKColumnType = LogicalType::INT64();
  }
  auto dstPKColumnType = dstTable->getPrimaryKeyDefinition().getType().copy();
  if (dstPKColumnType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
    dstPKColumnType = LogicalType::INT64();
  }
  columnTypes.push_back(std::move(srcPKColumnType));
  columnTypes.push_back(std::move(dstPKColumnType));
  bindExpectedColumns(relTableEntry, info, columnNames, columnTypes);
}

}  // namespace binder
}  // namespace neug
