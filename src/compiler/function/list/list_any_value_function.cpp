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

#include "neug/compiler/common/type_utils.h"
#include "neug/compiler/function/list/vector_list_functions.h"
#include "neug/compiler/function/scalar_function.h"

using namespace gs::common;

namespace gs {
namespace function {

struct ListAnyValue {
  template <typename T>
  static void operation(common::list_entry_t& input, T& result,
                        common::ValueVector& inputVector,
                        common::ValueVector& resultVector) {
    auto inputValues = common::ListVector::getListValues(&inputVector, input);
    auto inputDataVector = common::ListVector::getDataVector(&inputVector);
    auto numBytesPerValue = inputDataVector->getNumBytesPerValue();

    for (auto i = 0u; i < input.size; i++) {
      if (!(inputDataVector->isNull(input.offset + i))) {
        resultVector.copyFromVectorData(reinterpret_cast<uint8_t*>(&result),
                                        inputDataVector, inputValues);
        break;
      }
      inputValues += numBytesPerValue;
    }
  }
};

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  auto scalarFunction = neug_dynamic_cast<ScalarFunction*>(input.definition);
  const auto& resultType = ListType::getChildType(input.arguments[0]->dataType);
  TypeUtils::visit(
      resultType.getPhysicalType(), [&scalarFunction]<typename T>(T) {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, T,
                                                        ListAnyValue>;
      });
  return FunctionBindData::getSimpleBindData(input.arguments, resultType);
}

function_set ListAnyValueFunction::getFunctionSet() {
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
