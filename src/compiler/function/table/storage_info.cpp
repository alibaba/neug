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

#include <concepts>
#include "neug/compiler/binder/binder.h"
#include "neug/compiler/common/data_chunk/data_chunk_collection.h"
#include "neug/compiler/common/type_utils.h"
#include "neug/compiler/common/types/interval_t.h"
#include "neug/compiler/common/types/ku_string.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/bind_input.h"
#include "neug/compiler/function/table/simple_table_function.h"
#include "neug/compiler/storage/stats_manager.h"
#include "neug/compiler/storage/store/node_table.h"
#include "neug/compiler/storage/store/rel_table.h"
#include "neug/utils/exception/exception.h"

using namespace gs::common;
using namespace gs::catalog;
using namespace gs::storage;
using namespace gs::main;

namespace gs {
namespace function {

struct StorageInfoLocalState final : TableFuncLocalState {
  std::unique_ptr<DataChunkCollection> dataChunkCollection;
  idx_t currChunkIdx;

  explicit StorageInfoLocalState(MemoryManager* mm) : currChunkIdx{0} {
    dataChunkCollection = std::make_unique<DataChunkCollection>(mm);
  }
};

struct StorageInfoBindData final : TableFuncBindData {
  TableCatalogEntry* tableEntry;
  Table* table;
  const ClientContext* context;

  StorageInfoBindData(binder::expression_vector columns,
                      TableCatalogEntry* tableEntry, Table* table,
                      const ClientContext* context)
      : TableFuncBindData{std::move(columns), 1 /*maxOffset*/},
        tableEntry{tableEntry},
        table{table},
        context{context} {}

  std::unique_ptr<TableFuncBindData> copy() const override {
    return std::make_unique<StorageInfoBindData>(columns, tableEntry, table,
                                                 context);
  }
};

static std::unique_ptr<TableFuncLocalState> initLocalState(
    const TableFuncInitLocalStateInput& input) {
  return nullptr;
}

static void resetOutputIfNecessary(const StorageInfoLocalState* localState,
                                   DataChunk& outputChunk) {
  if (outputChunk.state->getSelVector().getSelSize() ==
      DEFAULT_VECTOR_CAPACITY) {
    localState->dataChunkCollection->append(outputChunk);
    outputChunk.resetAuxiliaryBuffer();
    outputChunk.state->getSelVectorUnsafe().setSelSize(0);
  }
}

static offset_t tableFunc(const TableFuncInput& input,
                          TableFuncOutput& output) {
  return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(
    const ClientContext* context, const TableFuncBindInput* input) {
  return nullptr;
}

function_set StorageInfoFunction::getFunctionSet() {
  function_set functionSet;
  auto function =
      std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::STRING});
  function->tableFunc = tableFunc;
  function->bindFunc = bindFunc;
  function->initSharedStateFunc = SimpleTableFunc::initSharedState;
  function->initLocalStateFunc = initLocalState;
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace gs
