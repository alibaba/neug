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
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/simple_table_function.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/main/database.h"
#include "neug/compiler/storage/buffer_manager/memory_manager.h"

namespace gs {
namespace function {

struct BMInfoBindData final : TableFuncBindData {
  uint64_t memLimit;
  uint64_t memUsage;

  BMInfoBindData(uint64_t memLimit, uint64_t memUsage,
                 binder::expression_vector columns)
      : TableFuncBindData{std::move(columns), 1},
        memLimit{memLimit},
        memUsage{memUsage} {}

  std::unique_ptr<TableFuncBindData> copy() const override {
    return std::make_unique<BMInfoBindData>(memLimit, memUsage, columns);
  }
};

static common::offset_t internalTableFunc(const TableFuncMorsel& /*morsel*/,
                                          const TableFuncInput& input,
                                          common::DataChunk& output) {
  KU_ASSERT(output.getNumValueVectors() == 2);
  auto bmInfoBindData = input.bindData->constPtrCast<BMInfoBindData>();
  output.getValueVectorMutable(0).setValue<uint64_t>(0,
                                                     bmInfoBindData->memLimit);
  output.getValueVectorMutable(1).setValue<uint64_t>(0,
                                                     bmInfoBindData->memUsage);
  return 1;
}

static std::unique_ptr<TableFuncBindData> bindFunc(
    const main::ClientContext* context, const TableFuncBindInput* input) {
  return nullptr;
}

function_set BMInfoFunction::getFunctionSet() {
  function_set functionSet;
  auto function = std::make_unique<TableFunction>(
      name, std::vector<common::LogicalTypeID>{});
  function->tableFunc = SimpleTableFunc::getTableFunc(internalTableFunc);
  function->bindFunc = bindFunc;
  function->initSharedStateFunc = SimpleTableFunc::initSharedState;
  function->initLocalStateFunc = TableFunction::initEmptyLocalState;
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace gs
