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

#include <string>

#include "int32_index.h"
#include "int32_index_rule.h"
#include "int32_index_scan.h"
#include "neug/compiler/catalog/catalog_entry/catalog_entry_type.h"
#include "neug/compiler/extension/extension_api.h"
#include "neug/storages/module/module_factory.h"
#include "neug/utils/exception/exception.h"

extern "C" {

void Init() {
  try {
    neug::ModuleFactory::instance()
        .Register<neug::extension::index_example::Int32Index>();
    neug::extension::ExtensionAPI::registerFunction<
        neug::extension::index_example::Int32IndexScanFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);
    neug::extension::ExtensionAPI::registerRule<
        neug::extension::index_example::Int32IndexRule>(
        neug::catalog::CatalogEntryType::RULE_ENTRY);
    neug::extension::ExtensionAPI::registerExtension(
        neug::extension::ExtensionInfo{
            "index_example",
            "Provides a simple INT32 index implementation for testing."});
  } catch (const std::exception& e) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "[index_example extension] registration failed: " +
        std::string(e.what()));
  } catch (...) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "[index_example extension] registration failed: unknown exception");
  }
}

const char* Name() { return "INDEX_EXAMPLE"; }

}  // extern "C"
