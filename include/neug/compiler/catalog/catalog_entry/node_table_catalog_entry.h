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

#include "table_catalog_entry.h"

namespace neug {
namespace transaction {
class Transaction;
}  // namespace transaction

namespace catalog {

class Catalog;
class NEUG_API NodeTableCatalogEntry final : public TableCatalogEntry {
  static constexpr CatalogEntryType entryType_ =
      CatalogEntryType::NODE_TABLE_ENTRY;

 public:
  NodeTableCatalogEntry() = default;
  NodeTableCatalogEntry(std::string name, std::string primaryKeyName)
      : TableCatalogEntry{entryType_, std::move(name)},
        primaryKeyName{std::move(primaryKeyName)} {}

  bool isParent(common::table_id_t /*tableID*/) override { return false; }
  common::TableType getTableType() const override {
    return common::TableType::NODE;
  }

  std::string getPrimaryKeyName() const { return primaryKeyName; }
  common::property_id_t getPrimaryKeyID() const {
    return propertyCollection.getPropertyID(primaryKeyName);
  }
  const binder::PropertyDefinition& getPrimaryKeyDefinition() const {
    return getProperty(primaryKeyName);
  }

  void serialize(common::Serializer& serializer) const override;
  static std::unique_ptr<NodeTableCatalogEntry> deserialize(
      common::Deserializer& deserializer);

  std::unique_ptr<TableCatalogEntry> copy() const override;
  std::string toCypher(const ToCypherInfo& info) const override;

 private:
  std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo>
  getBoundExtraCreateInfo(transaction::Transaction* transaction) const override;

 private:
  std::string primaryKeyName;
};

}  // namespace catalog
}  // namespace neug
