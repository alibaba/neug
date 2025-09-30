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

#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/catalog/function_signature_registry.h"
#include "neug/compiler/extension/extension.h"
#include "neug/compiler/gopt/g_catalog.h"
#include "neug/compiler/gopt/g_catalog_holder.h"

namespace gs {
namespace extension {

class ExtensionAPI {
 public:
  static void setCatalog(gs::catalog::Catalog* catalog);

  // API for register scalar function
  template <typename T>
  static void registerScalarFunction() {
    auto& catalogSet = s_catalog->functions;
    auto lock = catalogSet->acquireExclusiveLock();

    if (s_catalog->containsFunction(&gs::transaction::DUMMY_TRANSACTION,
                                    T::name, false)) {
      return;
    }
    s_catalog->addFunctionUnlocked(
        &gs::transaction::DUMMY_TRANSACTION,
        gs::catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY, T::name,
        T::getFunctionSet(), false);
    auto* entry = s_catalog->getFunctionEntry(
        &gs::transaction::DUMMY_TRANSACTION, T::name, false);
    auto* funcEntry = static_cast<gs::catalog::FunctionCatalogEntry*>(entry);
    s_catalog->registerFunctionSignatures(funcEntry);
  }

  template <typename T>
  static void registerFunction(catalog::CatalogEntryType entryType) {
    auto gCatalog = catalog::GCatalogHolder::getGCatalog();
    if (gCatalog->containsFunction(&gs::transaction::DUMMY_TRANSACTION, T::name,
                                   false)) {
      return;
    }
    gCatalog->addFunctionWithSignature(&gs::transaction::DUMMY_TRANSACTION,
                                       entryType, std::move(T::name),
                                       std::move(T::getFunctionSet()), false);
  }

 private:
  static gs::catalog::Catalog* s_catalog;
};

inline gs::catalog::Catalog* ExtensionAPI::s_catalog = nullptr;

inline void ExtensionAPI::setCatalog(gs::catalog::Catalog* catalog) {
  s_catalog = catalog;
}

}  // namespace extension
}  // namespace gs