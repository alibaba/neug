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
#include "neug/compiler/function/scalar_function.h"
#include "neug/compiler/function/utility/vector_utility_functions.h"

using namespace gs::common;

namespace gs {
namespace function {

struct CountIf {
  template <class T>
  static inline void operation(T& input, uint8_t& result) {
    if (input != 0) {
      result = 1;
    } else {
      result = 0;
    }
  }
};

function_set CountIfFunction::getFunctionSet() {
  function_set functionSet;
  auto operandTypeIDs = LogicalTypeUtils::getNumericalLogicalTypeIDs();
  operandTypeIDs.push_back(LogicalTypeID::BOOL);
  scalar_func_exec_t execFunc;
  for (auto operandTypeID : operandTypeIDs) {
    TypeUtils::visit(
        LogicalType(operandTypeID),
        [&execFunc]<NumericTypes T>(T) {
          execFunc = ScalarFunction::UnaryExecFunction<T, uint8_t, CountIf>;
        },
        [&execFunc](bool) {
          execFunc = ScalarFunction::UnaryExecFunction<bool, uint8_t, CountIf>;
        },
        [](auto) { KU_UNREACHABLE; });
    functionSet.push_back(std::make_unique<ScalarFunction>(
        name, std::vector<LogicalTypeID>{operandTypeID}, LogicalTypeID::UINT8,
        execFunc));
  }
  return functionSet;
}

}  // namespace function
}  // namespace gs
