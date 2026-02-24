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

#include "neug/compiler/common/enums/extend_direction.h"
#include "neug/compiler/common/enums/rel_direction.h"
#include "neug/compiler/common/enums/rel_multiplicity.h"
#include "table_catalog_entry.h"

namespace neug {
namespace catalog {

struct RelTableToCypherInfo : public ToCypherInfo {
  const main::ClientContext* context;

  explicit RelTableToCypherInfo(const main::ClientContext* context)
      : context{context} {}
};

class RelGroupCatalogEntry;
class NEUG_API RelTableCatalogEntry : public TableCatalogEntry {
  static constexpr auto entryType_ = CatalogEntryType::REL_TABLE_ENTRY;

 public:
  RelTableCatalogEntry()
      : srcMultiplicity{},
        dstMultiplicity{},
        storageDirection{},
        srcTableID{common::INVALID_TABLE_ID},
        dstTableID{common::INVALID_TABLE_ID} {};
  RelTableCatalogEntry(std::string name,
                       common::RelMultiplicity srcMultiplicity,
                       common::RelMultiplicity dstMultiplicity,
                       common::table_id_t srcTableID,
                       common::table_id_t dstTableID,
                       common::ExtendDirection storageDirection)
      : TableCatalogEntry{entryType_, std::move(name)},
        srcMultiplicity{srcMultiplicity},
        dstMultiplicity{dstMultiplicity},
        storageDirection(storageDirection),
        srcTableID{srcTableID},
        dstTableID{dstTableID} {
    propertyCollection = PropertyDefinitionCollection{
        1};  // Skip NBR_NODE_ID column as the first one.
  }

  bool isParent(common::table_id_t tableID) override;
  bool hasParentRelGroup(const Catalog* catalog,
                         const transaction::Transaction* transaction) const;
  RelGroupCatalogEntry* getParentRelGroup(
      const Catalog* catalog,
      const transaction::Transaction* transaction) const;

  common::TableType getTableType() const override {
    return common::TableType::REL;
  }
  common::table_id_t getSrcTableID() const { return srcTableID; }
  common::table_id_t getDstTableID() const { return dstTableID; }
  bool isSingleMultiplicity(common::RelDataDirection direction) const;
  common::RelMultiplicity getMultiplicity(
      common::RelDataDirection direction) const;

  std::vector<common::RelDataDirection> getRelDataDirections() const;
  common::ExtendDirection getStorageDirection() const;

  common::table_id_t getBoundTableID(
      common::RelDataDirection relDirection) const;
  common::table_id_t getNbrTableID(common::RelDataDirection relDirection) const;

  void serialize(common::Serializer& serializer) const override;
  static std::unique_ptr<RelTableCatalogEntry> deserialize(
      common::Deserializer& deserializer);

  std::unique_ptr<TableCatalogEntry> copy() const override;

  std::string getMultiplicityStr() const;
  std::string toCypher(const ToCypherInfo& info) const override;

 private:
  std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo>
  getBoundExtraCreateInfo(transaction::Transaction* transaction) const override;

 private:
  common::RelMultiplicity srcMultiplicity;
  common::RelMultiplicity dstMultiplicity;
  common::ExtendDirection storageDirection;
  common::table_id_t srcTableID;
  common::table_id_t dstTableID;
};

}  // namespace catalog
}  // namespace neug
