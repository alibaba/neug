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

#pragma once
#include "neug/compiler/function/function.h"
namespace neug {
namespace function {
struct ProjectGraphFunction {
  static constexpr const char* name = "project_graph";
  static function_set getFunctionSet();
};
struct DropProjectedGraphFunction {
  static constexpr const char* name = "drop_projected_graph";
  static function_set getFunctionSet();
};
struct ShowProjectedGraphsFunction final {
  static constexpr const char* name = "SHOW_PROJECTED_GRAPHS";

  static function_set getFunctionSet();
};

struct ProjectedGraphInfoFunction final {
  static constexpr const char* name = "PROJECTED_GRAPH_INFO";

  static function_set getFunctionSet();
};
}  // namespace function
}  // namespace neug