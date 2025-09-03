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

#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/standalone_call_function.h"
#include "neug/compiler/function/table/table_function.h"

using namespace gs::common;

namespace gs {
namespace function {

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
  return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(const main::ClientContext*,
                                                   const TableFuncBindInput*) {
  return std::make_unique<TableFuncBindData>(0);
}

function_set ClearWarningsFunction::getFunctionSet() {
  function_set functionSet;
  auto func =
      std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{});
  func->tableFunc = tableFunc;
  func->bindFunc = bindFunc;
  func->initSharedStateFunc = TableFunction::initEmptySharedState;
  func->initLocalStateFunc = TableFunction::initEmptyLocalState;
  func->canParallelFunc = []() { return false; };
  functionSet.push_back(std::move(func));
  return functionSet;
}

}  // namespace function
}  // namespace gs
