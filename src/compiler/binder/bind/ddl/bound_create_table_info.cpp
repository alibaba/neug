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

#include "neug/compiler/binder/ddl/bound_create_table_info.h"

#include "neug/compiler/common/system_config.h"

using namespace neug::parser;
using namespace neug::common;
using namespace neug::catalog;

namespace neug {
namespace binder {

std::string BoundCreateTableInfo::toString() const {
  std::string result = "";
  switch (type) {
  case CatalogEntryType::NODE_TABLE_ENTRY: {
    result += "Create Node Table: ";
    result += tableName;
    result += ",Properties: ";
    auto nodeInfo = extraInfo->ptrCast<BoundExtraCreateNodeTableInfo>();
    for (auto& definition : nodeInfo->propertyDefinitions) {
      result += definition.getName();
      result += ", ";
    }
    break;
  }
  case CatalogEntryType::REL_TABLE_ENTRY: {
    result += "Create Relationship Table: ";
    result += tableName;
    result += ",Multiplicity: ";
    auto* relInfo = extraInfo->ptrCast<BoundExtraCreateRelTableInfo>();
    if (relInfo->srcMultiplicity == RelMultiplicity::ONE) {
      result += "ONE";
    } else {
      result += "MANY";
    }
    result += "_";
    if (relInfo->dstMultiplicity == RelMultiplicity::ONE) {
      result += "ONE";
    } else {
      result += "MANY";
    }
    result += ",Properties: ";
    for (auto& definition : relInfo->propertyDefinitions) {
      result += definition.getName();
      result += ", ";
    }
    break;
  }
  case CatalogEntryType::REL_GROUP_ENTRY: {
    result += "Create Relationship Group Table: ";
    result += tableName;
    auto* relGroupInfo =
        extraInfo->ptrCast<BoundExtraCreateRelTableGroupInfo>();
    result += ",Tables: ";
    for (auto& createInfo : relGroupInfo->infos) {
      result += createInfo.tableName;
      result += ", ";
    }
    auto* groupTableInfo =
        relGroupInfo->infos[0].extraInfo->ptrCast<BoundExtraCreateTableInfo>();
    result += "Properties: ";
    for (auto& definition : groupTableInfo->propertyDefinitions) {
      result += definition.getName();
      result += ", ";
    }
    break;
  }
  default:
    break;
  }
  return result;
}

BoundExtraCreateRelTableInfo::BoundExtraCreateRelTableInfo(
    common::table_id_t srcTableID, common::table_id_t dstTableID,
    std::vector<PropertyDefinition> definitions)
    : BoundExtraCreateRelTableInfo{common::RelMultiplicity::MANY,
                                   common::RelMultiplicity::MANY,
                                   common::DEFAULT_EXTEND_DIRECTION,
                                   srcTableID,
                                   dstTableID,
                                   std::move(definitions)} {}

BoundExtraCreateRelTableInfo::BoundExtraCreateRelTableInfo(
    common::RelMultiplicity srcMultiplicity,
    common::RelMultiplicity dstMultiplicity,
    common::ExtendDirection storageDirection, common::table_id_t srcTableID,
    common::table_id_t dstTableID, std::vector<PropertyDefinition> definitions)
    : BoundExtraCreateTableInfo{std::move(definitions)},
      srcMultiplicity{srcMultiplicity},
      dstMultiplicity{dstMultiplicity},
      storageDirection(storageDirection),
      srcTableID{srcTableID},
      dstTableID{dstTableID} {}

BoundExtraCreateRelTableInfo::BoundExtraCreateRelTableInfo(
    common::RelMultiplicity srcMultiplicity,
    common::RelMultiplicity dstMultiplicity,
    common::ExtendDirection storageDirection, const std::string& srcLabelName,
    const std::string& dstLabelName,
    std::vector<PropertyDefinition> definitions)
    : BoundExtraCreateTableInfo{std::move(definitions)},
      srcMultiplicity{srcMultiplicity},
      dstMultiplicity{dstMultiplicity},
      storageDirection(storageDirection),
      srcLabelName{srcLabelName},
      dstTableName{dstLabelName} {}

BoundExtraCreateRelTableInfo::BoundExtraCreateRelTableInfo(
    const BoundExtraCreateRelTableInfo& other)
    : BoundExtraCreateTableInfo{copyVector(other.propertyDefinitions)},
      srcMultiplicity{other.srcMultiplicity},
      dstMultiplicity{other.dstMultiplicity},
      storageDirection{other.storageDirection},
      srcTableID{other.srcTableID},
      dstTableID{other.dstTableID} {}

}  // namespace binder
}  // namespace neug
