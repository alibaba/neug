/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <algorithm>
#include <string>
#include "neug/compiler/function/scalar_function.h"

namespace neug {
namespace extension {

struct JsonDummyFunction {
  static constexpr const char* name = "JSON_DUMMY";

  static neug::function::function_set getFunctionSet();

  static neug::runtime::Value Exec(const std::vector<neug::runtime::Value>& args);
};

}  // namespace extension
}  // namespace neug
