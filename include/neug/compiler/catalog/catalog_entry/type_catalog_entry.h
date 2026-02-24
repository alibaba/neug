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

#pragma once

#include "neug/compiler/catalog/catalog_entry/catalog_entry.h"

namespace neug {
namespace catalog {

class TypeCatalogEntry : public CatalogEntry {
 public:
  //===--------------------------------------------------------------------===//
  // constructors
  //===--------------------------------------------------------------------===//
  TypeCatalogEntry() = default;
  TypeCatalogEntry(std::string name, common::LogicalType type)
      : CatalogEntry{CatalogEntryType::TYPE_ENTRY, std::move(name)},
        type{std::move(type)} {}

  //===--------------------------------------------------------------------===//
  // getter & setter
  //===--------------------------------------------------------------------===//
  const common::LogicalType& getLogicalType() const { return type; }

  //===--------------------------------------------------------------------===//
  // serialization & deserialization
  //===--------------------------------------------------------------------===//
  void serialize(common::Serializer& serializer) const override;
  static std::unique_ptr<TypeCatalogEntry> deserialize(
      common::Deserializer& deserializer);

 private:
  common::LogicalType type;
};

}  // namespace catalog
}  // namespace neug
