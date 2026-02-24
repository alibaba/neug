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

#include "neug/compiler/catalog/catalog_entry/scalar_macro_catalog_entry.h"

namespace neug {
namespace catalog {

ScalarMacroCatalogEntry::ScalarMacroCatalogEntry(
    std::string name,
    std::unique_ptr<function::ScalarMacroFunction> macroFunction)
    : CatalogEntry{CatalogEntryType::SCALAR_MACRO_ENTRY, std::move(name)},
      macroFunction{std::move(macroFunction)} {}

void ScalarMacroCatalogEntry::serialize(common::Serializer& serializer) const {
  CatalogEntry::serialize(serializer);
  macroFunction->serialize(serializer);
}

std::unique_ptr<ScalarMacroCatalogEntry> ScalarMacroCatalogEntry::deserialize(
    common::Deserializer& deserializer) {
  auto scalarMacroCatalogEntry = std::make_unique<ScalarMacroCatalogEntry>();
  scalarMacroCatalogEntry->macroFunction =
      function::ScalarMacroFunction::deserialize(deserializer);
  return scalarMacroCatalogEntry;
}

std::string ScalarMacroCatalogEntry::toCypher(
    const ToCypherInfo& /*info*/) const {
  return macroFunction->toCypher(getName());
}

}  // namespace catalog
}  // namespace neug
