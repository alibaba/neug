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
#include "neug/utils/exception/exception.h"

using namespace gs::common;

namespace gs {
namespace function {

struct Range {
  // range function:
  // - include end
  // - when start = end: there is only one element in result list
  // - when end - start are of opposite sign of step, the result will be empty
  // - default step = 1
  template <typename T>
  static void operation(T& end, list_entry_t& result, ValueVector& endVector,
                        ValueVector& resultVector) {
    T step = 1;
    T start = 0;
    operation(start, end, step, result, endVector, resultVector);
  }

  template <typename T>
  static void operation(T& start, T& end, list_entry_t& result,
                        ValueVector& leftVector, ValueVector& /*rightVector*/,
                        ValueVector& resultVector) {
    T step = 1;
    operation(start, end, step, result, leftVector, resultVector);
  }

  template <typename T>
  static void operation(T& start, T& end, T& step, list_entry_t& result,
                        ValueVector& /*inputVector*/,
                        ValueVector& resultVector) {
    if (step == 0) {
      THROW_RUNTIME_ERROR("Step of range cannot be 0.");
    }

    // start, start + step, start + 2step, ..., end
    T number = start;
    auto size = ((end - start) * 1.0 / step);
    size < 0 ? size = 0 : size = (int64_t)(size + 1);

    result = ListVector::addList(&resultVector, (int64_t) size);
    auto resultDataVector = ListVector::getDataVector(&resultVector);
    for (auto i = 0u; i < (int64_t) size; i++) {
      resultDataVector->setValue(result.offset + i, number);
      number += step;
    }
  }
};

static scalar_func_exec_t getBinaryExecFunc(const LogicalType& type) {
  scalar_func_exec_t execFunc;
  TypeUtils::visit(
      type,
      [&execFunc]<IntegerTypes T>(T) {
        execFunc =
            ScalarFunction::BinaryExecListStructFunction<T, T, list_entry_t,
                                                         Range>;
      },
      [](auto) { KU_UNREACHABLE; });
  return execFunc;
}

static scalar_func_exec_t getTernaryExecFunc(const LogicalType& type) {
  scalar_func_exec_t execFunc;
  TypeUtils::visit(
      type,
      [&execFunc]<IntegerTypes T>(T) {
        execFunc =
            ScalarFunction::TernaryExecListStructFunction<T, T, T, list_entry_t,
                                                          Range>;
      },
      [](auto) { KU_UNREACHABLE; });
  return execFunc;
}

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  auto type = LogicalType(input.definition->parameterTypeIDs[0]);
  auto resultType = LogicalType::LIST(type.copy());
  auto bindData = std::make_unique<FunctionBindData>(std::move(resultType));
  for (auto& _ : input.arguments) {
    (void) _;
    bindData->paramTypes.push_back(type.copy());
  }
  return bindData;
}

function_set ListRangeFunction::getFunctionSet() {
  function_set result;
  std::unique_ptr<ScalarFunction> func;
  for (auto typeID : LogicalTypeUtils::getIntegerTypeIDs()) {
    // start, end
    func = std::make_unique<ScalarFunction>(
        name, std::vector<LogicalTypeID>{typeID, typeID}, LogicalTypeID::LIST,
        getBinaryExecFunc(LogicalType{typeID}));
    func->bindFunc = bindFunc;
    result.push_back(std::move(func));
    // start, end, step
    func = std::make_unique<ScalarFunction>(
        name, std::vector<LogicalTypeID>{typeID, typeID, typeID},
        LogicalTypeID::LIST, getTernaryExecFunc(LogicalType{typeID}));
    func->bindFunc = bindFunc;
    result.push_back(std::move(func));
  }
  return result;
}

}  // namespace function
}  // namespace gs
