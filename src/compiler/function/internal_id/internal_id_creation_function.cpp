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
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/function/internal_id/vector_internal_id_functions.h"
#include "neug/compiler/function/scalar_function.h"

namespace gs {
namespace function {

using namespace common;

struct InternalIDCreation {
  template <typename T>
  static void operation(T& tableID, T& offset, internalID_t& result) {
    result = internalID_t((offset_t) offset, (offset_t) tableID);
  }
};

function_set InternalIDCreationFunction::getFunctionSet() {
  function_set result;
  function::scalar_func_exec_t execFunc;
  for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
    common::TypeUtils::visit(
        common::LogicalType(typeID),
        [&]<common::NumericTypes T>(T) {
          execFunc = ScalarFunction::BinaryExecFunction<T, T, internalID_t,
                                                        InternalIDCreation>;
        },
        [](auto) { KU_UNREACHABLE; });
    result.push_back(std::make_unique<ScalarFunction>(
        name, std::vector<common::LogicalTypeID>{typeID, typeID},
        LogicalTypeID::INTERNAL_ID, execFunc));
  }
  return result;
}

}  // namespace function
}  // namespace gs
