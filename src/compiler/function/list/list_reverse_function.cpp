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

#include "neug/compiler/function/list/vector_list_functions.h"
#include "neug/compiler/function/scalar_function.h"

using namespace gs::common;

namespace gs {
namespace function {

struct ListReverse {
  static inline void operation(common::list_entry_t& input,
                               common::list_entry_t& result,
                               common::ValueVector& inputVector,
                               common::ValueVector& resultVector) {
    auto inputDataVector = common::ListVector::getDataVector(&inputVector);
    ListVector::resizeDataVector(&resultVector,
                                 ListVector::getDataVectorSize(&inputVector));
    auto resultDataVector = common::ListVector::getDataVector(&resultVector);
    result = input;  // reverse does not change
    for (auto i = 0u; i < input.size; i++) {
      auto pos = input.offset + i;
      auto reversePos = input.offset + input.size - 1 - i;
      resultDataVector->copyFromVectorData(reversePos, inputDataVector, pos);
    }
  }
};

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  auto scalarFunction = ku_dynamic_cast<ScalarFunction*>(input.definition);
  const auto& resultType = input.arguments[0]->dataType;
  scalarFunction->execFunc =
      ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, list_entry_t,
                                                  ListReverse>;
  return FunctionBindData::getSimpleBindData(input.arguments,
                                             resultType.copy());
}

function_set ListReverseFunction::getFunctionSet() {
  function_set result;
  auto function = std::make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::LIST},
      LogicalTypeID::ANY);
  function->bindFunc = bindFunc;
  result.push_back(std::move(function));
  return result;
}

}  // namespace function
}  // namespace gs
