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

#include "neug/compiler/binder/expression/node_rel_expression.h"

#include "neug/compiler/catalog/catalog_entry/table_catalog_entry.h"
#include "neug/utils/exception/exception.h"

using namespace neug::catalog;
using namespace neug::common;

namespace neug {
namespace binder {

table_id_vector_t NodeOrRelExpression::getTableIDs() const {
  table_id_vector_t result;
  for (auto& entry : entries) {
    result.push_back(entry->getTableID());
  }
  return result;
}

table_id_set_t NodeOrRelExpression::getTableIDsSet() const {
  table_id_set_t result;
  for (auto& entry : entries) {
    result.insert(entry->getTableID());
  }
  return result;
}

void NodeOrRelExpression::addEntries(
    const std::vector<TableCatalogEntry*>& entries_) {
  auto tableIDsSet = getTableIDsSet();
  for (auto& entry : entries_) {
    if (!tableIDsSet.contains(entry->getTableID())) {
      entries.push_back(entry);
    }
  }
}

TableCatalogEntry* NodeOrRelExpression::getSingleEntry() const {
  // LCOV_EXCL_START
  if (entries.empty()) {
    THROW_RUNTIME_ERROR(
        "Trying to access table id in an empty node. This should never happen");
  }
  // LCOV_EXCL_STOP
  return entries[0];
}

void NodeOrRelExpression::addPropertyExpression(
    const std::string& propertyName, std::unique_ptr<Expression> property) {
  if (propertyNameToIdx.contains(propertyName)) {
    THROW_EXCEPTION_WITH_FILE_LINE("Property name : " + propertyName +
                                   " already exists");
  }
  propertyNameToIdx.insert({propertyName, propertyExprs.size()});
  propertyExprs.push_back(std::move(property));
}

std::shared_ptr<Expression> NodeOrRelExpression::getPropertyExpression(
    const std::string& propertyName) const {
  NEUG_ASSERT(propertyNameToIdx.contains(propertyName));
  return propertyExprs[propertyNameToIdx.at(propertyName)]->copy();
}

expression_vector NodeOrRelExpression::getPropertyExprs() const {
  expression_vector result;
  for (auto& expr : propertyExprs) {
    result.push_back(expr->copy());
  }
  return result;
}

}  // namespace binder
}  // namespace neug
