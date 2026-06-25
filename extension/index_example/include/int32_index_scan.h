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

#include <string>

#include "neug/compiler/function/function.h"
#include "neug/compiler/function/neug_call_function.h"

namespace neug::extension::index_example {

struct Int32IndexScanFuncInput final : function::CallFuncInputBase {
  std::string uniqueIndexName;
  int32_t targetValue;
  int32_t alias;
};

class Int32IndexScanFunction {
 public:
  static constexpr const char* name = "INT32_INDEX_SCAN";

  static function::function_set getFunctionSet();
};

}  // namespace neug::extension::index_example
