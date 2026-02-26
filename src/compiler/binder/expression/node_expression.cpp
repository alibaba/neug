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

#include "neug/compiler/binder/expression/node_expression.h"

#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/gopt/g_graph_type.h"

namespace neug {
namespace binder {

NodeExpression::~NodeExpression() = default;

void NodeExpression::setEntries(
    std::vector<catalog::TableCatalogEntry*> entries_) {
  entries = std::move(entries_);
  auto extraTypeInfo = getDataType().getExtraTypeInfoRef();
  auto nodeTypeInfo = dynamic_cast<common::GNodeTypeInfo*>(extraTypeInfo);
  if (!nodeTypeInfo) {
    return;
  }
  // update node labels using new entries
  std::vector<catalog::NodeTableCatalogEntry*> nodeEntries;
  for (auto entry : entries) {
    nodeEntries.emplace_back(entry->ptrCast<catalog::NodeTableCatalogEntry>());
  }
  auto nodeType = nodeTypeInfo->getNodeType();
  if (nodeType) {
    nodeType->setNodeTables(std::move(nodeEntries));
  }
}

std::shared_ptr<Expression> NodeExpression::getPrimaryKey(
    common::table_id_t tableID) const {
  for (auto& e : propertyExprs) {
    if (e->constCast<PropertyExpression>().isPrimaryKey(tableID)) {
      return e->copy();
    }
  }
  NEUG_UNREACHABLE;
}

}  // namespace binder
}  // namespace neug
