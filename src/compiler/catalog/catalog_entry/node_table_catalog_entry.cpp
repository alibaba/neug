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

#include "neug/compiler/catalog/catalog_entry/node_table_catalog_entry.h"

#include "neug/compiler/binder/ddl/bound_create_table_info.h"
#include "neug/compiler/common/serializer/deserializer.h"

using namespace neug::binder;

namespace neug {
namespace catalog {

void NodeTableCatalogEntry::serialize(common::Serializer& serializer) const {
  TableCatalogEntry::serialize(serializer);
  serializer.writeDebuggingInfo("primaryKeyName");
  serializer.write(primaryKeyName);
}

std::unique_ptr<NodeTableCatalogEntry> NodeTableCatalogEntry::deserialize(
    common::Deserializer& deserializer) {
  std::string debuggingInfo;
  std::string primaryKeyName;
  deserializer.validateDebuggingInfo(debuggingInfo, "primaryKeyName");
  deserializer.deserializeValue(primaryKeyName);
  auto nodeTableEntry = std::make_unique<NodeTableCatalogEntry>();
  nodeTableEntry->primaryKeyName = primaryKeyName;
  return nodeTableEntry;
}

std::string NodeTableCatalogEntry::toCypher(
    const ToCypherInfo& /*info*/) const {
  return common::stringFormat("CREATE NODE TABLE `{}` ({} PRIMARY KEY(`{}`));",
                              getName(), propertyCollection.toCypher(),
                              primaryKeyName);
}

std::unique_ptr<TableCatalogEntry> NodeTableCatalogEntry::copy() const {
  auto other = std::make_unique<NodeTableCatalogEntry>();
  other->primaryKeyName = primaryKeyName;
  other->copyFrom(*this);
  return other;
}

std::unique_ptr<BoundExtraCreateCatalogEntryInfo>
NodeTableCatalogEntry::getBoundExtraCreateInfo(
    transaction::Transaction*) const {
  return std::make_unique<BoundExtraCreateNodeTableInfo>(
      primaryKeyName, copyVector(getProperties()));
}

}  // namespace catalog
}  // namespace neug
