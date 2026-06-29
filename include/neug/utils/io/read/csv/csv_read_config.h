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

/// Native CSV reader configuration for csv-parser based IO.
struct CsvReadConfig {
  char delimiter = ',';
  bool quoting = true;
  char quote_char = '"';
  bool double_quote = true;
  bool escaping = false;
  char escape_char = '\\';

  int64_t skip_rows = 0;
  int64_t chunk_size = 4096;

  /// All column names in the physical file (in order).
  std::vector<std::string> column_names;
  /// Selected columns to read, in output order.
  std::vector<std::string> include_columns;
  /// Per-column NeuG types keyed by column name.
  std::unordered_map<std::string, DataType> column_types;

  std::vector<std::string> null_values;
  std::vector<std::string> true_values;
  std::vector<std::string> false_values;
};

}  // namespace neug
