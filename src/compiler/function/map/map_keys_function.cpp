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

#include "neug/compiler/function/map/functions/map_keys_function.h"

#include "neug/compiler/function/map/vector_map_functions.h"
#include "neug/compiler/function/scalar_function.h"

using namespace gs::common;

namespace gs {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  auto resultType = LogicalType::LIST(
      MapType::getKeyType(input.arguments[0]->dataType).copy());
  return FunctionBindData::getSimpleBindData(input.arguments, resultType);
}

function_set MapKeysFunctions::getFunctionSet() {
  auto execFunc =
      ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, list_entry_t,
                                                  MapKeys>;
  function_set functionSet;
  auto function = std::make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::MAP}, LogicalTypeID::LIST,
      execFunc);
  function->bindFunc = bindFunc;
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace gs
