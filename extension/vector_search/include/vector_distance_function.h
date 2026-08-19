/** Copyright 2020 Alibaba Group Holding Limited.
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

#pragma once

#include "neug/common/types/value.h"
#include "neug/compiler/function/function.h"

namespace neug::vector_search_ext {

struct VectorDistanceL2Function {
  static constexpr const char* name = "VECTOR_DISTANCE_L2";

  static function::function_set getFunctionSet();
  static Value Exec(const std::vector<Value>& args);
};

struct VectorDistanceCosineFunction {
  static constexpr const char* name = "VECTOR_DISTANCE_COSINE";

  static function::function_set getFunctionSet();
  static Value Exec(const std::vector<Value>& args);
};

struct VectorDistanceIPFunction {
  static constexpr const char* name = "VECTOR_DISTANCE_IP";

  static function::function_set getFunctionSet();
  static Value Exec(const std::vector<Value>& args);
};

}  // namespace neug::vector_search_ext
