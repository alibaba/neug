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

#include "neug/compiler/function/list/functions/list_len_function.h"
#include "neug/compiler/function/list/vector_list_functions.h"
#include "neug/compiler/function/scalar_function.h"

using namespace gs::common;

namespace gs {
namespace function {

static std::unique_ptr<FunctionBindData> sizeBindFunc(
    const ScalarBindFuncInput& input) {
  auto scalarFunc = input.definition->constPtrCast<ScalarFunction>();
  auto resultType = LogicalType(scalarFunc->returnTypeID);
  if (input.definition->parameterTypeIDs[0] == common::LogicalTypeID::STRING) {
    std::vector<LogicalType> paramTypes;
    paramTypes.push_back(LogicalType::STRING());
    return std::make_unique<FunctionBindData>(std::move(paramTypes),
                                              resultType.copy());
  } else {
    return FunctionBindData::getSimpleBindData(input.arguments, resultType);
  }
}

function_set SizeFunction::getFunctionSet() {
  function_set result;
  // size(list)
  auto listFunc = std::make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::LIST},
      LogicalTypeID::INT64,
      ScalarFunction::UnaryExecFunction<list_entry_t, int64_t, ListLen>);
  listFunc->bindFunc = sizeBindFunc;
  result.push_back(std::move(listFunc));
  // size(array)
  auto arrayFunc = std::make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{
          LogicalTypeID::ARRAY,
      },
      LogicalTypeID::INT64,
      ScalarFunction::UnaryExecFunction<list_entry_t, int64_t, ListLen>);
  arrayFunc->bindFunc = sizeBindFunc;
  result.push_back(std::move(arrayFunc));
  // size(map)
  auto mapFunc = std::make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::MAP},
      LogicalTypeID::INT64,
      ScalarFunction::UnaryExecFunction<list_entry_t, int64_t, ListLen>);
  mapFunc->bindFunc = sizeBindFunc;
  result.push_back(std::move(mapFunc));
  // size(string)
  auto strFunc = std::make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
      LogicalTypeID::INT64,
      ScalarFunction::UnaryExecFunction<ku_string_t, int64_t, ListLen>);
  strFunc->bindFunc = sizeBindFunc;
  result.push_back(std::move(strFunc));
  return result;
}

}  // namespace function
}  // namespace gs
