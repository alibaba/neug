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
#include "neug/compiler/graph/graph_entry.h"
#include "neug/utils/exception/exception.h"

using namespace gs::common;

namespace gs {
namespace function {

struct DropProjectedGraphBindData final : TableFuncBindData {
  std::string graphName;

  explicit DropProjectedGraphBindData(std::string graphName)
      : TableFuncBindData{0}, graphName{std::move(graphName)} {}

  std::unique_ptr<TableFuncBindData> copy() const override {
    return std::make_unique<DropProjectedGraphBindData>(graphName);
  }
};

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
  return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(
    const main::ClientContext*, const TableFuncBindInput* input) {
  return nullptr;
}

function_set DropProjectedGraphFunction::getFunctionSet() {
  function_set functionSet;
  auto func =
      std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::STRING});
  func->bindFunc = bindFunc;
  func->tableFunc = tableFunc;
  func->initSharedStateFunc = TableFunction::initEmptySharedState;
  func->initLocalStateFunc = TableFunction::initEmptyLocalState;
  func->canParallelFunc = []() { return false; };
  functionSet.push_back(std::move(func));
  return functionSet;
}

}  // namespace function
}  // namespace gs
