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

#include "neug/compiler/function/table/simple_table_function.h"

#include "neug/compiler/function/table/bind_data.h"

namespace gs {
namespace function {

TableFuncMorsel SimpleTableFuncSharedState::getMorsel() {
  std::lock_guard lck{mtx};
  NEUG_ASSERT(curRowIdx <= numRows);
  if (curRowIdx == numRows) {
    return TableFuncMorsel::createInvalidMorsel();
  }
  const auto numValuesToOutput = std::min(maxMorselSize, numRows - curRowIdx);
  curRowIdx += numValuesToOutput;
  return {curRowIdx - numValuesToOutput, curRowIdx};
}

std::unique_ptr<TableFuncSharedState> SimpleTableFunc::initSharedState(
    const TableFuncInitSharedStateInput& input) {
  return std::make_unique<SimpleTableFuncSharedState>(input.bindData->numRows);
}

common::offset_t tableFunc(simple_internal_table_func internalTableFunc,
                           const TableFuncInput& input,
                           TableFuncOutput& output) {
  const auto sharedState =
      input.sharedState->ptrCast<SimpleTableFuncSharedState>();
  auto morsel = sharedState->getMorsel();
  if (!morsel.hasMoreToOutput()) {
    return 0;
  }
  return internalTableFunc(morsel, input, output.dataChunk);
}

table_func_t SimpleTableFunc::getTableFunc(
    simple_internal_table_func internalTableFunc) {
  return std::bind(tableFunc, internalTableFunc, std::placeholders::_1,
                   std::placeholders::_2);
}

}  // namespace function
}  // namespace gs
