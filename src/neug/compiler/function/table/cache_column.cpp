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
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/catalog/catalog_entry/table_catalog_entry.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/simple_table_function.h"
#include "neug/compiler/storage/storage_manager.h"
#include "neug/compiler/storage/store/node_table.h"
#include "neug/compiler/storage/store/table.h"
#include "neug/compiler/transaction/transaction.h"
#include "neug/utils/exception/exception.h"

namespace gs::catalog {
class TableCatalogEntry;
}

using namespace gs::common;

namespace gs {
namespace function {

struct CacheArrayColumnBindData final : TableFuncBindData {
  catalog::TableCatalogEntry* tableEntry;
  property_id_t propertyID;

  CacheArrayColumnBindData(catalog::TableCatalogEntry* tableEntry,
                           property_id_t propertyID)
      : tableEntry{tableEntry}, propertyID{propertyID} {}

  std::unique_ptr<TableFuncBindData> copy() const override {
    return std::make_unique<CacheArrayColumnBindData>(tableEntry, propertyID);
  }
};

static void validateArrayColumnType(const catalog::TableCatalogEntry* entry,
                                    property_id_t propertyID) {
  auto& type = entry->getProperty(propertyID).getType();
  if (type.getLogicalTypeID() != LogicalTypeID::ARRAY) {
    THROW_BINDER_EXCEPTION(
        stringFormat("Column {} is not of the expected type {}.",
                     entry->getProperty(propertyID).getName(),
                     LogicalTypeUtils::toString(LogicalTypeID::ARRAY)));
  }
}

static std::unique_ptr<TableFuncBindData> bindFunc(
    main::ClientContext* context, const TableFuncBindInput* input) {
  return nullptr;
}

static std::unique_ptr<TableFuncSharedState> initSharedState(
    const TableFuncInitSharedStateInput& input) {
  return nullptr;
}

static std::unique_ptr<TableFuncLocalState> initLocalState(
    const TableFuncInitLocalStateInput& input) {
  return nullptr;
}

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
  return 0;
}

static void finalizeFunc(const processor::ExecutionContext* context,
                         TableFuncSharedState* sharedState) {}

function_set LocalCacheArrayColumnFunction::getFunctionSet() {
  function_set functionSet;
  std::vector inputTypes = {LogicalTypeID::STRING, LogicalTypeID::STRING};
  auto func = std::make_unique<TableFunction>(name, inputTypes);
  func->bindFunc = bindFunc;
  func->initSharedStateFunc = initSharedState;
  func->initLocalStateFunc = initLocalState;
  func->tableFunc = tableFunc;
  func->finalizeFunc = finalizeFunc;
  func->canParallelFunc = [] { return true; };
  functionSet.push_back(std::move(func));
  return functionSet;
}

}  // namespace function
}  // namespace gs
