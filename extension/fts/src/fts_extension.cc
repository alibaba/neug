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

#include "fts_extension.h"

#include "fts_function.h"
#include "fts_index_scan.h"
#include "neug/compiler/extension/extension_api.h"

extern "C" {

void Init() {
  neug::extension::ExtensionAPI::registerFunction<
      neug::fts_ext::FTSBM25Function>(
      neug::catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY);
  neug::extension::ExtensionAPI::registerFunction<
      neug::fts_ext::FTSIndexScanFunction>(
      neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);
  neug::extension::ExtensionAPI::registerRule<
      neug::fts_ext::FTSIndexScanOptimizer>(
      neug::catalog::CatalogEntryType::RULE_ENTRY);
  neug::extension::ExtensionAPI::registerExtension(
      neug::extension::ExtensionInfo{
          neug::fts_ext::kExtensionCatalogName,
          "Provides FTS indexes and full-text search."});
}

const char* Name() { return neug::fts_ext::kExtensionName; }

}  // extern "C"
