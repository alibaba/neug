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

#include "neug/compiler/function/list/functions/list_extract_function.h"

#include "neug/compiler/function/list/vector_list_functions.h"
#include "neug/compiler/function/neug_scalar_function.h"
#include "neug/compiler/function/scalar_function.h"
#include "neug/utils/runtime/rt_any.h"

using namespace gs::common;

namespace gs {
namespace function {

static int checkAndGetIndex(const runtime::RTAny& value) {
  switch (value.type().id()) {
  case gs::DataTypeId::UINTEGER:
    return value.as_uint32();
  case gs::DataTypeId::INTEGER:
    return value.as_int32();
  case gs::DataTypeId::UBIGINT:
    return value.as_uint64();
  case gs::DataTypeId::BIGINT:
    return value.as_int64();
  default:
    THROW_RUNTIME_ERROR(
        "LIST_EXTRACT([], index): the second element should be a integer, "
        "but is " +
        std::to_string(static_cast<int>(value.type().id())));
  }
}

static runtime::RTAny execFunc(runtime::Arena& arena,
                               const std::vector<runtime::RTAny>& args) {
  if (args.size() != 2) {
    THROW_RUNTIME_ERROR(
        "LIST_EXTRACT([], index): expect exactly 2 argument, got " +
        std::to_string(args.size()));
  }
  int index = checkAndGetIndex(args[1]);
  const auto& arg0 = args[0];
  switch (arg0.type().id()) {
  case gs::DataTypeId::STRUCT:
    return arg0.as_tuple().get(index);
  case gs::DataTypeId::LIST:
    return arg0.as_list().get(index);
  default:
    THROW_RUNTIME_ERROR(
        "LIST_EXTRACT([], index): the first element should be a tuple or a "
        "list, "
        "but is " +
        std::to_string(static_cast<int>(arg0.type().id())));
  }
}

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  const auto& resultType = ListType::getChildType(input.arguments[0]->dataType);
  std::vector<LogicalType> paramTypes;
  paramTypes.push_back(input.arguments[0]->getDataType().copy());
  paramTypes.push_back(LogicalType(input.definition->parameterTypeIDs[1]));
  return std::make_unique<FunctionBindData>(std::move(paramTypes),
                                            resultType.copy());
}

function_set ListExtractFunction::getFunctionSet() {
  function_set result;
  std::unique_ptr<ScalarFunction> func = std::make_unique<NeugScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::INT64},
      LogicalTypeID::ANY, std::move(execFunc));
  func->bindFunc = bindFunc;
  result.push_back(std::move(func));
  return result;
}

}  // namespace function
}  // namespace gs
