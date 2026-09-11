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

#include "neug/compiler/function/neug_call_function.h"

namespace neug::function {

struct ShowNodeTablesFunction {
  static constexpr const char* name = "show_node_tables";

  static function_set getFunctionSet();
};

struct ShowRelTablesFunction {
  static constexpr const char* name = "show_rel_tables";

  static function_set getFunctionSet();
};

struct ShowNodeTableInfoFunction {
  static constexpr const char* name = "show_node_table_info";

  static function_set getFunctionSet();
};

struct ShowRelTableInfoFunction {
  static constexpr const char* name = "show_rel_table_info";

  static function_set getFunctionSet();
};

}  // namespace neug::function
