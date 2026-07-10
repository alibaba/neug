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

#include "neug/compiler/gopt/g_catalog.h"

#include <string>
#include "neug/compiler/common/constants.h"
#include "neug/compiler/function/function_collection.h"
#include "neug/compiler/function/function_signature_util.h"
#include "neug/compiler/gopt/g_constants.h"
#include "neug/compiler/transaction/transaction.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace catalog {

GCatalog::GCatalog() : Catalog() { registerBuiltInFunctions(); }

std::unique_ptr<Catalog> GCatalog::clone(const Schema* schema) const {
  auto cloned = std::make_unique<GCatalog>(*this);
  cloned->setSchema(schema);
  return cloned;
}

void GCatalog::registerBuiltInFunctions() {
  auto functionCollection = function::FunctionCollection::getFunctions();

  for (auto i = 0u; functionCollection[i].name != nullptr; ++i) {
    auto& f = functionCollection[i];
    auto functionSet = f.getFunctionSetFunc();
    addFunctionWithSignature(&neug::transaction::DUMMY_TRANSACTION,
                             f.catalogEntryType, f.name, std::move(functionSet),
                             false);
  }
}

void GCatalog::addFunctionWithSignature(transaction::Transaction* transaction,
                                        CatalogEntryType entryType,
                                        std::string name,
                                        function::function_set functionSet,
                                        bool isInternal) {
  for (auto& func : functionSet) {
    func->computeSignature();
  }
  addFunction(transaction, entryType, std::move(name), std::move(functionSet),
              isInternal);
}

function::Function* GCatalog::getFunctionWithSignature(
    const std::string& signatureName) {
  return getFunctionWithSignature(&neug::Constants::DEFAULT_TRANSACTION,
                                  signatureName);
}

function::Function* GCatalog::getFunctionWithSignature(
    transaction::Transaction* transaction, const std::string& signatureName) {
  auto funcName =
      function::FunctionSignatureUtil::getFunctionName(signatureName);
  auto entry = getFunctionEntry(transaction, funcName);
  if (!entry) {
    THROW_CATALOG_EXCEPTION("cannot find function entry with name " + funcName);
  }
  auto funcEntry = entry->ptrCast<catalog::FunctionCatalogEntry>();
  auto& functionSet = funcEntry->getFunctionSet();
  for (auto& func : functionSet) {
    if (func->signatureName == signatureName) {
      return func.get();
    }
  }
  THROW_CATALOG_EXCEPTION("cannot find function with signature name " +
                          signatureName);
}

}  // namespace catalog
}  // namespace neug
