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

#include "neug/compiler/catalog/catalog_entry/catalog_entry_type.h"
#include "neug/compiler/common/enums/conflict_action.h"
#include "neug/compiler/common/enums/extend_direction.h"
#include "neug/compiler/common/enums/rel_multiplicity.h"
#include "neug/compiler/common/types/types.h"
#include "property_definition.h"

namespace neug {
namespace common {
enum class RelMultiplicity : uint8_t;
}
namespace binder {
struct BoundExtraCreateCatalogEntryInfo {
  virtual ~BoundExtraCreateCatalogEntryInfo() = default;

  template <class TARGET>
  const TARGET* constPtrCast() const {
    return common::neug_dynamic_cast<const TARGET*>(this);
  }

  template <class TARGET>
  TARGET* ptrCast() {
    return common::neug_dynamic_cast<TARGET*>(this);
  }

  virtual inline std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy()
      const = 0;
};

struct BoundCreateTableInfo {
  catalog::CatalogEntryType type = catalog::CatalogEntryType::DUMMY_ENTRY;
  std::string tableName;
  common::ConflictAction onConflict = common::ConflictAction::INVALID;
  std::unique_ptr<BoundExtraCreateCatalogEntryInfo> extraInfo;
  bool isInternal = false;
  bool hasParent = false;

  BoundCreateTableInfo() = default;
  BoundCreateTableInfo(
      catalog::CatalogEntryType type, std::string tableName,
      common::ConflictAction onConflict,
      std::unique_ptr<BoundExtraCreateCatalogEntryInfo> extraInfo,
      bool isInternal, bool hasParent = false)
      : type{type},
        tableName{std::move(tableName)},
        onConflict{onConflict},
        extraInfo{std::move(extraInfo)},
        isInternal{isInternal},
        hasParent{hasParent} {}
  EXPLICIT_COPY_DEFAULT_MOVE(BoundCreateTableInfo);

  std::string toString() const;

 private:
  BoundCreateTableInfo(const BoundCreateTableInfo& other)
      : type{other.type},
        tableName{other.tableName},
        onConflict{other.onConflict},
        extraInfo{other.extraInfo->copy()},
        isInternal{other.isInternal},
        hasParent{other.hasParent} {}
};

struct NEUG_API BoundExtraCreateTableInfo
    : public BoundExtraCreateCatalogEntryInfo {
  std::vector<PropertyDefinition> propertyDefinitions;

  explicit BoundExtraCreateTableInfo(
      std::vector<PropertyDefinition> propertyDefinitions)
      : propertyDefinitions{std::move(propertyDefinitions)} {}

  BoundExtraCreateTableInfo(const BoundExtraCreateTableInfo& other)
      : BoundExtraCreateTableInfo{copyVector(other.propertyDefinitions)} {}
  BoundExtraCreateTableInfo& operator=(const BoundExtraCreateTableInfo&) =
      delete;

  std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const override {
    return std::make_unique<BoundExtraCreateTableInfo>(*this);
  }
};

struct BoundExtraCreateNodeTableInfo final : BoundExtraCreateTableInfo {
  std::string primaryKeyName;

  BoundExtraCreateNodeTableInfo(std::string primaryKeyName,
                                std::vector<PropertyDefinition> definitions)
      : BoundExtraCreateTableInfo{std::move(definitions)},
        primaryKeyName{std::move(primaryKeyName)} {}
  BoundExtraCreateNodeTableInfo(const BoundExtraCreateNodeTableInfo& other)
      : BoundExtraCreateTableInfo{copyVector(other.propertyDefinitions)},
        primaryKeyName{other.primaryKeyName} {}

  std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const override {
    return std::make_unique<BoundExtraCreateNodeTableInfo>(*this);
  }
};

struct BoundExtraCreateRelTableInfo final : BoundExtraCreateTableInfo {
  common::RelMultiplicity srcMultiplicity;
  common::RelMultiplicity dstMultiplicity;
  common::ExtendDirection storageDirection;
  common::table_id_t srcTableID;
  common::table_id_t dstTableID;
  std::string srcLabelName;
  std::string dstTableName;

  BoundExtraCreateRelTableInfo(common::table_id_t srcTableID,
                               common::table_id_t dstTableID,
                               std::vector<PropertyDefinition> definitions);
  BoundExtraCreateRelTableInfo(common::RelMultiplicity srcMultiplicity,
                               common::RelMultiplicity dstMultiplicity,
                               common::ExtendDirection storageDirection,
                               common::table_id_t srcTableID,
                               common::table_id_t dstTableID,
                               std::vector<PropertyDefinition> definitions);

  BoundExtraCreateRelTableInfo(common::RelMultiplicity srcMultiplicity,
                               common::RelMultiplicity dstMultiplicity,
                               common::ExtendDirection storageDirection,
                               const std::string& srcLabelName,
                               const std::string& dstLabelName,
                               std::vector<PropertyDefinition> definitions);

  BoundExtraCreateRelTableInfo(const BoundExtraCreateRelTableInfo& other);

  std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const override {
    return std::make_unique<BoundExtraCreateRelTableInfo>(*this);
  }
};

struct BoundExtraCreateRelTableGroupInfo final
    : BoundExtraCreateCatalogEntryInfo {
  std::vector<BoundCreateTableInfo> infos;

  explicit BoundExtraCreateRelTableGroupInfo(
      std::vector<BoundCreateTableInfo> infos)
      : infos{std::move(infos)} {}
  BoundExtraCreateRelTableGroupInfo(
      const BoundExtraCreateRelTableGroupInfo& other)
      : infos{copyVector(other.infos)} {}

  std::unique_ptr<BoundExtraCreateCatalogEntryInfo> copy() const override {
    return std::make_unique<BoundExtraCreateRelTableGroupInfo>(*this);
  }
};

}  // namespace binder
}  // namespace neug
