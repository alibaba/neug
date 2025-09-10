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
#include "neug/utils/exception/exception.h"

using namespace gs::common;
using namespace gs::catalog;

namespace gs {
namespace function {

static offset_t internalTableFunc(const TableFuncMorsel& morsel,
                                  const TableFuncInput& input,
                                  DataChunk& output) {
  return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(
    const main::ClientContext* context, const TableFuncBindInput* input) {
  THROW_EXCEPTION_WITH_FILE_LINE("show attached databases not implemented");
}

function_set ShowAttachedDatabasesFunction::getFunctionSet() {
  function_set functionSet;
  auto function =
      std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{});
  function->tableFunc = SimpleTableFunc::getTableFunc(internalTableFunc);
  function->bindFunc = bindFunc;
  function->initSharedStateFunc = SimpleTableFunc::initSharedState;
  function->initLocalStateFunc = TableFunction::initEmptyLocalState;
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace gs
