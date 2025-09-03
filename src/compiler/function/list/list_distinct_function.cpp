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

#include "neug/compiler/function/list/functions/list_unique_function.h"
#include "neug/compiler/function/list/vector_list_functions.h"
#include "neug/compiler/function/scalar_function.h"

using namespace gs::common;

namespace gs {
namespace function {

struct ListDistinct {
  static void operation(common::list_entry_t& input,
                        common::list_entry_t& result,
                        common::ValueVector& inputVector,
                        common::ValueVector& resultVector) {
    auto numUniqueValues =
        ListUnique::appendListElementsToValueSet(input, inputVector);
    result = common::ListVector::addList(&resultVector, numUniqueValues);
    auto resultDataVector = common::ListVector::getDataVector(&resultVector);
    auto resultDataVectorBuffer = common::ListVector::getListValuesWithOffset(
        &resultVector, result, 0 /* offset */);
    ListUnique::appendListElementsToValueSet(
        input, inputVector, nullptr,
        [&resultDataVector, &resultDataVectorBuffer](
            common::ValueVector& dataVector, uint64_t pos) -> void {
          resultDataVector->copyFromVectorData(
              resultDataVectorBuffer, &dataVector,
              dataVector.getData() + pos * dataVector.getNumBytesPerValue());
          resultDataVectorBuffer += dataVector.getNumBytesPerValue();
        });
  }
};

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  return FunctionBindData::getSimpleBindData(input.arguments,
                                             input.arguments[0]->getDataType());
}

function_set ListDistinctFunction::getFunctionSet() {
  function_set result;
  auto function = std::make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::LIST},
      LogicalTypeID::LIST,
      ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, list_entry_t,
                                                  ListDistinct>);
  function->bindFunc = bindFunc;
  result.push_back(std::move(function));
  return result;
}

}  // namespace function
}  // namespace gs
