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
#include "neug/compiler/catalog/catalog_entry/node_table_catalog_entry.h"
#include "neug/compiler/catalog/catalog_entry/rel_group_catalog_entry.h"
#include "neug/compiler/catalog/catalog_entry/rel_table_catalog_entry.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/bind_input.h"
#include "neug/compiler/function/table/simple_table_function.h"
#include "neug/compiler/main/client_context.h"
#include "neug/utils/exception/exception.h"

using namespace gs::catalog;
using namespace gs::common;
using namespace gs::main;

namespace gs {
namespace function {

struct ShowConnectionBindData final : TableFuncBindData {
  std::vector<TableCatalogEntry*> entries;

  ShowConnectionBindData(std::vector<TableCatalogEntry*> entries,
                         binder::expression_vector columns, offset_t maxOffset)
      : TableFuncBindData{std::move(columns), maxOffset},
        entries{std::move(entries)} {}

  std::unique_ptr<TableFuncBindData> copy() const override {
    return std::make_unique<ShowConnectionBindData>(entries, columns, numRows);
  }
};

static void outputRelTableConnection(DataChunk& outputDataChunk,
                                     uint64_t outputPos,
                                     const ClientContext& context,
                                     TableCatalogEntry* entry) {
  const auto catalog = context.getCatalog();
  NEUG_ASSERT(entry->getType() == CatalogEntryType::REL_TABLE_ENTRY);
  const auto relTableEntry = neug_dynamic_cast<RelTableCatalogEntry*>(entry);

  const auto srcTableID = relTableEntry->getSrcTableID();
  const auto dstTableID = relTableEntry->getDstTableID();
  const auto srcTableEntry =
      catalog->getTableCatalogEntry(context.getTransaction(), srcTableID);
  const auto dstTableEntry =
      catalog->getTableCatalogEntry(context.getTransaction(), dstTableID);
  const auto srcTablePrimaryKey =
      srcTableEntry->constCast<NodeTableCatalogEntry>().getPrimaryKeyName();
  const auto dstTablePrimaryKey =
      dstTableEntry->constCast<NodeTableCatalogEntry>().getPrimaryKeyName();
  outputDataChunk.getValueVectorMutable(0).setValue(outputPos,
                                                    srcTableEntry->getName());
  outputDataChunk.getValueVectorMutable(1).setValue(outputPos,
                                                    dstTableEntry->getName());
  outputDataChunk.getValueVectorMutable(2).setValue(outputPos,
                                                    srcTablePrimaryKey);
  outputDataChunk.getValueVectorMutable(3).setValue(outputPos,
                                                    dstTablePrimaryKey);
}

static offset_t internalTableFunc(const TableFuncMorsel& morsel,
                                  const TableFuncInput& input,
                                  DataChunk& output) {
  return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(
    const ClientContext* context, const TableFuncBindInput* input) {
  return nullptr;
}

function_set ShowConnectionFunction::getFunctionSet() {
  function_set functionSet;
  auto function =
      std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::STRING});
  function->tableFunc = SimpleTableFunc::getTableFunc(internalTableFunc);
  function->bindFunc = bindFunc;
  function->initSharedStateFunc = SimpleTableFunc::initSharedState;
  function->initLocalStateFunc = TableFunction::initEmptyLocalState;
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace gs
