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

#include "neug/compiler/function/list/functions/list_to_string_function.h"

#include "neug/compiler/common/type_utils.h"
#include "neug/compiler/function/list/vector_list_functions.h"
#include "neug/compiler/function/scalar_function.h"

using namespace gs::common;

namespace gs {
namespace function {

void ListToString::operation(list_entry_t& input, neug_string_t& delim,
                             neug_string_t& result, ValueVector& inputVector,
                             ValueVector& /*delimVector*/,
                             ValueVector& resultVector) {
  std::string resultStr = "";
  auto dataVector = ListVector::getDataVector(&inputVector);
  for (auto i = 0u; i < input.size - 1; i++) {
    resultStr += TypeUtils::entryToString(
        dataVector->dataType,
        ListVector::getListValuesWithOffset(&inputVector, input, i),
        dataVector);
    resultStr += delim.getAsString();
  }
  resultStr += TypeUtils::entryToString(
      dataVector->dataType,
      ListVector::getListValuesWithOffset(&inputVector, input, input.size - 1),
      dataVector);
  StringVector::addString(&resultVector, result, resultStr);
}

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  std::vector<LogicalType> paramTypes;
  paramTypes.push_back(input.arguments[0]->getDataType().copy());
  paramTypes.push_back(LogicalType(input.definition->parameterTypeIDs[1]));
  return std::make_unique<FunctionBindData>(std::move(paramTypes),
                                            LogicalType::STRING());
}

function_set ListToStringFunction::getFunctionSet() {
  function_set result;
  auto function = std::make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::STRING},
      LogicalTypeID::STRING,
      ScalarFunction::BinaryExecListStructFunction<
          list_entry_t, neug_string_t, neug_string_t, ListToString>);
  function->bindFunc = bindFunc;
  result.push_back(std::move(function));
  return result;
}

}  // namespace function
}  // namespace gs
