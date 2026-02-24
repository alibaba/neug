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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "neug/compiler/catalog/catalog_entry/catalog_entry_type.h"
#include "neug/compiler/common/enums/conflict_action.h"
#include "parsed_property_definition.h"

namespace neug {
namespace parser {

struct ExtraCreateTableInfo {
  virtual ~ExtraCreateTableInfo() = default;

  template <class TARGET>
  const TARGET& constCast() const {
    return common::neug_dynamic_cast<const TARGET&>(*this);
  }
};

struct CreateTableInfo {
  catalog::CatalogEntryType type;
  std::string tableName;
  std::vector<ParsedPropertyDefinition> propertyDefinitions;
  std::unique_ptr<ExtraCreateTableInfo> extraInfo;
  common::ConflictAction onConflict;

  CreateTableInfo(catalog::CatalogEntryType type, std::string tableName,
                  common::ConflictAction onConflict)
      : type{type},
        tableName{std::move(tableName)},
        extraInfo{nullptr},
        onConflict{onConflict} {}
  DELETE_COPY_DEFAULT_MOVE(CreateTableInfo);
};

struct ExtraCreateNodeTableInfo : public ExtraCreateTableInfo {
  std::string pKName;

  explicit ExtraCreateNodeTableInfo(std::string pKName)
      : pKName{std::move(pKName)} {}
};

struct ExtraCreateRelTableInfo : public ExtraCreateTableInfo {
  std::string relMultiplicity;
  std::string srcTableName;
  std::string dstTableName;
  options_t options;

  ExtraCreateRelTableInfo(std::string relMultiplicity, std::string srcTableName,
                          std::string dstTableName, options_t options)
      : relMultiplicity{std::move(relMultiplicity)},
        srcTableName{std::move(srcTableName)},
        dstTableName{std::move(dstTableName)},
        options{std::move(options)} {}
};

struct ExtraCreateRelTableGroupInfo : public ExtraCreateTableInfo {
  std::string relMultiplicity;
  std::vector<std::pair<std::string, std::string>> srcDstTablePairs;
  options_t options;

  ExtraCreateRelTableGroupInfo(
      std::string relMultiplicity,
      std::vector<std::pair<std::string, std::string>> srcDstTablePairs,
      options_t options)
      : relMultiplicity{std::move(relMultiplicity)},
        srcDstTablePairs{std::move(srcDstTablePairs)},
        options{std::move(options)} {}
};

}  // namespace parser
}  // namespace neug
