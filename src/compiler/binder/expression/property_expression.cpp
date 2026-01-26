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

#include "neug/compiler/binder/expression/property_expression.h"

#include "neug/compiler/binder/expression/node_rel_expression.h"
#include "neug/compiler/catalog/catalog_entry/table_catalog_entry.h"

using namespace neug::common;
using namespace neug::catalog;

namespace neug {
namespace binder {

std::unique_ptr<PropertyExpression> PropertyExpression::construct(
    LogicalType type, const std::string& propertyName,
    const Expression& child) {
  NEUG_ASSERT(child.expressionType == ExpressionType::PATTERN);
  auto& patternExpr = child.constCast<NodeOrRelExpression>();
  auto variableName = patternExpr.getVariableName();
  auto uniqueName = patternExpr.getUniqueName();
  // Assign an invalid property id for virtual property.
  common::table_id_map_t<SingleLabelPropertyInfo> infos;
  for (auto& entry : patternExpr.getEntries()) {
    infos.insert({entry->getTableID(),
                  SingleLabelPropertyInfo(false /* exists */,
                                          false /* isPrimaryKey */)});
  }
  return std::make_unique<PropertyExpression>(std::move(type), propertyName,
                                              uniqueName, variableName,
                                              std::move(infos));
}

bool PropertyExpression::isPrimaryKey() const {
  for (auto& [id, info] : infos) {
    if (!info.isPrimaryKey) {
      return false;
    }
  }
  return true;
}

bool PropertyExpression::isPrimaryKey(common::table_id_t tableID) const {
  if (!infos.contains(tableID)) {
    return false;
  }
  return infos.at(tableID).isPrimaryKey;
}

bool PropertyExpression::hasProperty(common::table_id_t tableID) const {
  NEUG_ASSERT(infos.contains(tableID));
  return infos.at(tableID).exists;
}

column_id_t PropertyExpression::getColumnID(
    const TableCatalogEntry& entry) const {
  if (!hasProperty(entry.getTableID())) {
    return INVALID_COLUMN_ID;
  }
  return entry.getColumnID(propertyName);
}

}  // namespace binder
}  // namespace neug
