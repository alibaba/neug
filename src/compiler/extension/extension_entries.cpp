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

#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/extension/extension_manager.h"

namespace neug::extension {

struct EntriesForExtension {
  const char* extensionName;
  std::span<const char* const> entries;
  size_t numEntries;
};

static constexpr std::array ftsExtensionFunctions = {
    "STEM", "QUERY_FTS_INDEX", "CREATE_FTS_INDEX", "DROP_FTS_INDEX"};
static constexpr std::array jsonExtensionFunctions = {
    "TO_JSON",      "JSON_QUOTE",     "ARRAY_TO_JSON",     "ROW_TO_JSON",
    "CAST_TO_JSON", "JSON_ARRAY",     "JSON_OBJECT",       "JSON_MERGE_PATCH",
    "COPY_JSON",    "JSON_EXTRACT",   "JSON_ARRAY_LENGTH", "JSON_CONTAINS",
    "JSON_KEYS",    "JSON_STRUCTURE", "JSON_TYPE",         "JSON_VALID",
    "JSON"};
static constexpr std::array duckdbExtensionFunctions = {
    "CLEAR_ATTACHED_DB_CACHE"};
static constexpr std::array deltaExtensionFunctions = {"DELTA_SCAN"};
static constexpr std::array icebergExtensionFunctions = {
    "ICEBERG_SCAN", "ICEBERG_METADATA", "ICEBERG_SNAPSHOTS"};
static constexpr std::array vectorExtensionFunctions = {
    "QUERY_VECTOR_INDEX", "CREATE_VECTOR_INDEX", "DROP_VECTOR_INDEX"};
static constexpr std::array gdsExtensionFunctions = {
    "K_CORE_DECOMPOSITION", "PAGE_RANK",
    "STRONGLY_CONNECTED_COMPONENTS_KOSARAJU", "STRONGLY_CONNECTED_COMPONENTS",
    "WEAKLY_CONNECTED_COMPONENT"};

static constexpr EntriesForExtension functionsForExtensionsRaw[] = {
    {"FTS", ftsExtensionFunctions, ftsExtensionFunctions.size()},
    {"DUCKDB", duckdbExtensionFunctions, duckdbExtensionFunctions.size()},
    {"DELTA", deltaExtensionFunctions, deltaExtensionFunctions.size()},
    {"ICEBERG", icebergExtensionFunctions, ftsExtensionFunctions.size()},
    {"JSON", jsonExtensionFunctions, jsonExtensionFunctions.size()},
    {"VECTOR", vectorExtensionFunctions, vectorExtensionFunctions.size()},
    {"GDS", gdsExtensionFunctions, gdsExtensionFunctions.size()},
};
static constexpr std::array functionsForExtensions =
    std::to_array(functionsForExtensionsRaw);

static constexpr std::array jsonExtensionTypes = {"JSON"};
static constexpr std::array<EntriesForExtension, 1> typesForExtensions = {
    EntriesForExtension{"JSON", jsonExtensionTypes, jsonExtensionTypes.size()}};

static std::optional<ExtensionEntry> lookupExtensionsByEntryName(
    std::string_view functionName,
    std::span<const EntriesForExtension> entriesForExtensions) {
  std::vector<ExtensionEntry> ret;
  for (const auto extension : entriesForExtensions) {
    for (const auto* entry : extension.entries) {
      if (entry == functionName) {
        return ExtensionEntry{.name = entry,
                              .extensionName = extension.extensionName};
      }
    }
  }
  return {};
}
}  // namespace neug::extension
