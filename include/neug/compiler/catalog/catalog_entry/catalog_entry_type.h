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

#pragma once

#include <cstdint>
#include <string>

namespace neug {
namespace catalog {

enum class CatalogEntryType : uint8_t {
  // Table entries
  NODE_TABLE_ENTRY = 0,
  REL_TABLE_ENTRY = 1,
  REL_GROUP_ENTRY = 2,
  FOREIGN_TABLE_ENTRY = 4,
  // Macro entries
  SCALAR_MACRO_ENTRY = 10,
  // Function entries
  AGGREGATE_FUNCTION_ENTRY = 20,
  SCALAR_FUNCTION_ENTRY = 21,
  REWRITE_FUNCTION_ENTRY = 22,
  TABLE_FUNCTION_ENTRY = 23,
  COPY_FUNCTION_ENTRY = 25,
  STANDALONE_TABLE_FUNCTION_ENTRY = 26,
  // Sequence entries
  SEQUENCE_ENTRY = 40,
  // UDT entries
  TYPE_ENTRY = 41,
  // Index entries
  INDEX_ENTRY = 42,
  // Dummy entry
  DUMMY_ENTRY = 100,
};

struct CatalogEntryTypeUtils {
  static std::string toString(CatalogEntryType type);
};

struct FunctionEntryTypeUtils {
  static std::string toString(CatalogEntryType type);
};

}  // namespace catalog
}  // namespace neug
