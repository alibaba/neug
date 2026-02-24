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

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/extension/extension.h"
#include "neug/compiler/gopt/g_catalog.h"
#include "neug/compiler/gopt/g_catalog_holder.h"

namespace neug {
namespace extension {

/**
 * @brief Basic information for an extension.
 *
 * ExtensionInfo describes the metadata of an extension.
 * @field name        Unique name of the extension.
 * @field description Brief description of the extension's functionality.
 */
struct ExtensionInfo {
  std::string name;
  std::string description;
};

class ExtensionAPI {
 public:
  static void setCatalog(neug::catalog::Catalog* catalog);

  template <typename T>
  static void registerFunction(catalog::CatalogEntryType entryType) {
    auto gCatalog = catalog::GCatalogHolder::getGCatalog();
    if (gCatalog->containsFunction(&neug::transaction::DUMMY_TRANSACTION,
                                   T::name, false)) {
      return;
    }
    gCatalog->addFunctionWithSignature(&neug::transaction::DUMMY_TRANSACTION,
                                       entryType, T::name, T::getFunctionSet(),
                                       false);
  }

  template <typename T>
  static void registerFunctionAlias(catalog::CatalogEntryType entryType) {
    auto gCatalog = catalog::GCatalogHolder::getGCatalog();
    gCatalog->addFunctionWithSignature(&neug::transaction::DUMMY_TRANSACTION,
                                       entryType, T::name,
                                       T::alias::getFunctionSet(), false);
  }

  static void registerExtension(const ExtensionInfo& info);
  static const std::unordered_map<std::string, ExtensionInfo>&
  getLoadedExtensions();

 private:
  static std::unordered_map<std::string, ExtensionInfo> loaded_extensions_;
  static std::mutex extensions_mutex_;
};

}  // namespace extension
}  // namespace neug