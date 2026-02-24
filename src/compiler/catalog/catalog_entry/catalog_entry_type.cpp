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

#include "neug/compiler/catalog/catalog_entry/catalog_entry_type.h"

#include "neug/compiler/common/assert.h"

namespace neug {
namespace catalog {

std::string CatalogEntryTypeUtils::toString(CatalogEntryType type) {
  switch (type) {
  case CatalogEntryType::NODE_TABLE_ENTRY:
    return "NODE_TABLE_ENTRY";
  case CatalogEntryType::REL_TABLE_ENTRY:
    return "REL_TABLE_ENTRY";
  case CatalogEntryType::REL_GROUP_ENTRY:
    return "REL_GROUP_ENTRY";
  case CatalogEntryType::FOREIGN_TABLE_ENTRY:
    return "FOREIGN_TABLE_ENTRY";
  case CatalogEntryType::SCALAR_MACRO_ENTRY:
    return "SCALAR_MACRO_ENTRY";
  case CatalogEntryType::AGGREGATE_FUNCTION_ENTRY:
    return "AGGREGATE_FUNCTION_ENTRY";
  case CatalogEntryType::SCALAR_FUNCTION_ENTRY:
    return "SCALAR_FUNCTION_ENTRY";
  case CatalogEntryType::REWRITE_FUNCTION_ENTRY:
    return "REWRITE_FUNCTION_ENTRY";
  case CatalogEntryType::TABLE_FUNCTION_ENTRY:
    return "TABLE_FUNCTION_ENTRY";
  case CatalogEntryType::STANDALONE_TABLE_FUNCTION_ENTRY:
    return "STANDALONE_TABLE_FUNCTION_ENTRY";
  case CatalogEntryType::COPY_FUNCTION_ENTRY:
    return "COPY_FUNCTION_ENTRY";
  case CatalogEntryType::DUMMY_ENTRY:
    return "DUMMY_ENTRY";
  case CatalogEntryType::SEQUENCE_ENTRY:
    return "SEQUENCE_ENTRY";
  default:
    NEUG_UNREACHABLE;
  }
}

std::string FunctionEntryTypeUtils::toString(CatalogEntryType type) {
  switch (type) {
  case CatalogEntryType::SCALAR_MACRO_ENTRY:
    return "MACRO FUNCTION";
  case CatalogEntryType::AGGREGATE_FUNCTION_ENTRY:
    return "AGGREGATE FUNCTION";
  case CatalogEntryType::SCALAR_FUNCTION_ENTRY:
    return "SCALAR FUNCTION";
  case CatalogEntryType::REWRITE_FUNCTION_ENTRY:
    return "REWRITE FUNCTION";
  case CatalogEntryType::TABLE_FUNCTION_ENTRY:
    return "TABLE FUNCTION";
  case CatalogEntryType::STANDALONE_TABLE_FUNCTION_ENTRY:
    return "STANDALONE TABLE FUNCTION";
  case CatalogEntryType::COPY_FUNCTION_ENTRY:
    return "COPY FUNCTION";
  default:
    NEUG_UNREACHABLE;
  }
}

}  // namespace catalog
}  // namespace neug
