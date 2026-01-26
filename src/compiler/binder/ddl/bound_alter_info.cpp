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

#include "neug/compiler/binder/ddl/bound_alter_info.h"

namespace neug {
namespace binder {

std::string BoundAlterInfo::toString() const {
  std::string result = "Operation: ";
  switch (alterType) {
  case common::AlterType::RENAME: {
    auto renameInfo =
        common::neug_dynamic_cast<BoundExtraRenameTableInfo*>(extraInfo.get());
    result += "Rename Table " + tableName + " to " + renameInfo->newName;
    break;
  }
  case common::AlterType::ADD_PROPERTY: {
    auto addPropInfo =
        common::neug_dynamic_cast<BoundExtraAddPropertyInfo*>(extraInfo.get());
    result += "Add Property " + addPropInfo->propertyDefinition.getName() +
              " to Table " + tableName;
    break;
  }
  case common::AlterType::DROP_PROPERTY: {
    auto dropPropInfo =
        common::neug_dynamic_cast<BoundExtraDropPropertyInfo*>(extraInfo.get());
    result += "Drop Property " + dropPropInfo->propertyName + " from Table " +
              tableName;
    break;
  }
  case common::AlterType::RENAME_PROPERTY: {
    auto renamePropInfo =
        common::neug_dynamic_cast<BoundExtraRenamePropertyInfo*>(
            extraInfo.get());
    result += "Rename Property " + renamePropInfo->oldName + " to " +
              renamePropInfo->newName + " in Table " + tableName;
    break;
  }
  case common::AlterType::COMMENT: {
    result += "Comment on Table " + tableName;
    break;
  }
  default:
    break;
  }
  return result;
}

}  // namespace binder
}  // namespace neug
