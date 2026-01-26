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

#include "neug/compiler/graph/graph_entry.h"

#include "neug/utils/exception/exception.h"

using namespace neug::planner;
using namespace neug::binder;
using namespace neug::common;
using namespace neug::catalog;

namespace neug {
namespace graph {

std::string GraphEntryTableInfo::toString() const {
  auto result = common::stringFormat("{'table': '{}'", tableName);
  if (predicate != "") {
    result += common::stringFormat(",'predicate': '{}'", predicate);
  }
  result += "}";
  return result;
}

GraphEntry::GraphEntry(std::vector<TableCatalogEntry*> nodeEntries,
                       std::vector<TableCatalogEntry*> relEntries) {
  for (auto& entry : nodeEntries) {
    nodeInfos.emplace_back(entry);
  }
  for (auto& entry : relEntries) {
    relInfos.emplace_back(entry);
  }
}

std::vector<table_id_t> GraphEntry::getNodeTableIDs() const {
  std::vector<table_id_t> result;
  for (auto& info : nodeInfos) {
    result.push_back(info.entry->getTableID());
  }
  return result;
}

std::vector<table_id_t> GraphEntry::getRelTableIDs() const {
  std::vector<table_id_t> result;
  for (auto& info : relInfos) {
    result.push_back(info.entry->getTableID());
  }
  return result;
}

std::vector<TableCatalogEntry*> GraphEntry::getNodeEntries() const {
  std::vector<TableCatalogEntry*> result;
  for (auto& info : nodeInfos) {
    result.push_back(info.entry);
  }
  return result;
}

std::vector<TableCatalogEntry*> GraphEntry::getRelEntries() const {
  std::vector<TableCatalogEntry*> result;
  for (auto& info : relInfos) {
    result.push_back(info.entry);
  }
  return result;
}

const BoundGraphEntryTableInfo& GraphEntry::getRelInfo(
    table_id_t tableID) const {
  for (auto& info : relInfos) {
    if (info.entry->getTableID() == tableID) {
      return info;
    }
  }
  // LCOV_EXCL_START
  THROW_RUNTIME_ERROR(
      stringFormat("Cannot find rel table with id {}", tableID));
  // LCOV_EXCL_STOP
}

void GraphEntry::setRelPredicate(std::shared_ptr<Expression> predicate) {
  for (auto& info : relInfos) {
    NEUG_ASSERT(info.predicate == nullptr);
    info.predicate = predicate;
  }
}

}  // namespace graph
}  // namespace neug
