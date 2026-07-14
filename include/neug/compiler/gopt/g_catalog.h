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

namespace neug {
namespace catalog {
class GCatalog : public Catalog {
 public:
  GCatalog();
  ~GCatalog() = default;

  void addFunctionWithSignature(transaction::Transaction* transaction,
                                CatalogEntryType entryType, std::string name,
                                function::function_set functionSet,
                                bool isInternal = false);

  function::Function* getFunctionWithSignature(
      transaction::Transaction* transaction, const std::string& signatureName);

  function::Function* getFunctionWithSignature(
      const std::string& signatureName);

  std::unique_ptr<Catalog> clone(const Schema* schema) const override;

 private:
  void registerBuiltInFunctions();
};
}  // namespace catalog
}  // namespace neug
