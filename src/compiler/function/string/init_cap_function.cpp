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

#include "neug/compiler/function/string/vector_string_functions.h"

namespace gs {
namespace function {

using namespace gs::common;

struct InitCap {
  static void operation(neug_string_t& operand, neug_string_t& result,
                        ValueVector& resultVector) {
    Lower::operation(operand, result, resultVector);
    int originalSize = 0, newSize = 0;
    BaseLowerUpperFunction::convertCharCase(
        reinterpret_cast<char*>(result.getDataUnsafe()),
        reinterpret_cast<const char*>(result.getData()), 0 /* charPos */,
        true /* toUpper */, originalSize, newSize);
  }
};

function_set InitCapFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
      LogicalTypeID::STRING,
      ScalarFunction::UnaryStringExecFunction<neug_string_t, neug_string_t,
                                              InitCap>));
  return functionSet;
}

}  // namespace function
}  // namespace gs
