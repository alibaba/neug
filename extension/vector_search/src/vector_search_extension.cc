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

#include "vector_search_extension.h"

#include <glog/logging.h>

#include "hnsw_index.h"
#include "hnsw_index_scan.h"
#include "neug/compiler/extension/extension_api.h"
#include "neug/storages/module/module_factory.h"
#include "vector_distance_function.h"

extern "C" {

void RegisterModules() {
  neug::ModuleFactory::instance()
      .Register<neug::vector_search_ext::HNSWIndex>();
}

void Init() {
  RegisterModules();

  neug::extension::ExtensionAPI::registerFunction<
      neug::vector_search_ext::VectorDistanceL2Function>(
      neug::catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY);
  neug::extension::ExtensionAPI::registerFunction<
      neug::vector_search_ext::VectorDistanceCosineFunction>(
      neug::catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY);
  neug::extension::ExtensionAPI::registerFunction<
      neug::vector_search_ext::VectorDistanceIPFunction>(
      neug::catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY);
  neug::extension::ExtensionAPI::registerFunction<
      neug::vector_search_ext::HNSWIndexScanFunction>(
      neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);
  neug::extension::ExtensionAPI::registerRule<
      neug::vector_search_ext::HNSWIndexScanOptimizer>(
      neug::catalog::CatalogEntryType::RULE_ENTRY);

  neug::extension::ExtensionAPI::registerExtension(
      neug::extension::ExtensionInfo{
          neug::vector_search_ext::kExtensionCatalogName,
          "Provides HNSW vector indexing and vector distance functions."});
  LOG(INFO) << "[vector_search extension] initialized";
}

const char* Name() { return neug::vector_search_ext::kExtensionName; }

}  // extern "C"
