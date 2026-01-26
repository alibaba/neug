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

#include "neug/compiler/catalog/catalog_entry/type_catalog_entry.h"

#include "neug/compiler/common/serializer/deserializer.h"

namespace neug {
namespace catalog {

void TypeCatalogEntry::serialize(common::Serializer& serializer) const {
  CatalogEntry::serialize(serializer);
  serializer.writeDebuggingInfo("type");
  type.serialize(serializer);
}

std::unique_ptr<TypeCatalogEntry> TypeCatalogEntry::deserialize(
    common::Deserializer& deserializer) {
  std::string debuggingInfo;
  auto typeCatalogEntry = std::make_unique<TypeCatalogEntry>();
  deserializer.validateDebuggingInfo(debuggingInfo, "type");
  typeCatalogEntry->type = common::LogicalType::deserialize(deserializer);
  return typeCatalogEntry;
}

}  // namespace catalog
}  // namespace neug
