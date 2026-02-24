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
namespace catalog {

struct RelGroupToCypherInfo : public ToCypherInfo {
  const main::ClientContext* context;

  explicit RelGroupToCypherInfo(const main::ClientContext* context)
      : context{context} {}
};

class Catalog;
class RelGroupCatalogEntry final : public CatalogEntry {
  static constexpr CatalogEntryType type_ = CatalogEntryType::REL_GROUP_ENTRY;

 public:
  RelGroupCatalogEntry() = default;
  RelGroupCatalogEntry(std::string tableName,
                       std::vector<common::table_id_t> relTableIDs)
      : CatalogEntry{type_, std::move(tableName)},
        relTableIDs{std::move(relTableIDs)} {}

  common::idx_t getNumRelTables() const { return relTableIDs.size(); }
  const std::vector<common::table_id_t>& getRelTableIDs() const {
    return relTableIDs;
  }

  std::unique_ptr<RelGroupCatalogEntry> alter(
      common::transaction_t timestamp,
      const binder::BoundAlterInfo& alterInfo) const;

  bool isParent(common::table_id_t tableID) const;

  //===--------------------------------------------------------------------===//
  // serialization & deserialization
  //===--------------------------------------------------------------------===//
  void serialize(common::Serializer& serializer) const override;
  static std::unique_ptr<RelGroupCatalogEntry> deserialize(
      common::Deserializer& deserializer);
  std::string toCypher(const ToCypherInfo& info) const override;

  binder::BoundCreateTableInfo getBoundCreateTableInfo(
      transaction::Transaction* transaction, const Catalog* catalog,
      bool isInternal) const;

  static std::string getChildTableName(const std::string& groupName,
                                       const std::string& srcName,
                                       const std::string& dstName) {
    return groupName + "_" + srcName + "_" + dstName;
  }

  void setComment(std::string newComment) { comment = std::move(newComment); }
  std::string getComment() const { return comment; }

  std::unique_ptr<RelGroupCatalogEntry> copy() const {
    return std::make_unique<RelGroupCatalogEntry>(getName(), relTableIDs);
  }

 private:
  std::vector<common::table_id_t> relTableIDs;
  std::string comment;
};

}  // namespace catalog
}  // namespace neug
