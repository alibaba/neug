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

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "neug/utils/property/types.h"

namespace neug {

/// Native JSON/JSONL reader configuration.
struct JsonReadConfig {
  bool newlines_in_values = false;
  bool json_array_input = false;
  int64_t chunk_size = 4096;

  std::vector<std::string> column_names;
  std::vector<std::string> include_columns;
  std::unordered_map<std::string, DataType> column_types;
};

}  // namespace neug
