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
#include <string>
#include <string_view>
#include <vector>

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/expression_visitor.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/catalog/catalog_entry/catalog_entry_type.h"
#include "neug/compiler/catalog/catalog_entry/rel_group_catalog_entry.h"
#include "neug/compiler/catalog/catalog_entry/rel_table_catalog_entry.h"
#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/gopt/g_alias_manager.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/parser.h"
#include "neug/utils/exception/exception.h"

using namespace neug::planner;
using namespace neug::binder;
using namespace neug::common;
using namespace neug::catalog;

namespace neug {
namespace graph {

std::string ParsedGraphEntryTableInfo::toString() const {
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

void GraphEntrySet::validateGraphNotExist(const std::string& name) const {
  if (hasGraph(name)) {
    THROW_BINDER_EXCEPTION(
        stringFormat("Projected graph '{}' already exists.", name));
  }
}

void GraphEntrySet::validateGraphExist(const std::string& name) const {
  if (!hasGraph(name)) {
    THROW_BINDER_EXCEPTION(
        stringFormat("Projected graph '{}' does not exist.", name));
  }
}

static expression_vector getResultColumns(const std::string& cypher,
                                          main::ClientContext* context) {
  auto parsedStatements = parser::Parser::parseQuery(cypher);
  NEUG_ASSERT(parsedStatements.size() == 1);
  auto binder = binder::Binder(context);
  auto boundStatement = binder.bind(*parsedStatements[0]);
  return boundStatement->getStatementResult()->getColumns();
}

BoundGraphEntryTableInfo GDSFunction::bindNodeEntry(
    main::ClientContext& context, const std::string& tableName,
    const std::string& predicate) {
  auto catalog = context.getCatalog();
  auto transaction = context.getTransaction();
  auto nodeEntry = catalog->getTableCatalogEntry(transaction, tableName);
  if (nodeEntry->getType() != CatalogEntryType::NODE_TABLE_ENTRY) {
    THROW_BINDER_EXCEPTION(stringFormat("{} is not a NODE table.", tableName));
  }
  if (!predicate.empty()) {
    auto cypher = stringFormat("MATCH (n:`{}`) RETURN n, {}",
                               nodeEntry->getName(), predicate);
    auto columns = getResultColumns(cypher, &context);
    NEUG_ASSERT(columns.size() == 2);
    return {nodeEntry, columns[0], columns[1]};
  } else {
    auto cypher = stringFormat("MATCH (n:`{}`) RETURN n", nodeEntry->getName());
    auto columns = getResultColumns(cypher, &context);
    NEUG_ASSERT(columns.size() == 1);
    return {nodeEntry, columns[0], nullptr /* empty predicate */};
  }
}

BoundGraphEntryTableInfo GDSFunction::bindRelEntry(
    main::ClientContext& context, const std::vector<std::string>& triplets,
    const std::string& predicate) {
  auto* catalog = context.getCatalog();
  auto* transaction = context.getTransaction();
  const auto& srcLabel = triplets[0];
  const auto& edgeLabel = triplets[1];
  const auto& dstLabel = triplets[2];
  std::string tableName = edgeLabel;
  if (catalog->containsRelGroup(transaction, edgeLabel)) {
    tableName =
        RelGroupCatalogEntry::getChildTableName(edgeLabel, srcLabel, dstLabel);
  }
  auto* relEntry = catalog->getTableCatalogEntry(transaction, tableName);
  if (!relEntry || relEntry->getType() != CatalogEntryType::REL_TABLE_ENTRY) {
    THROW_BINDER_EXCEPTION(stringFormat("{} is not a REL table.", tableName));
  }
  if (!predicate.empty()) {
    auto cypher = stringFormat("MATCH ()-[r:`{}`]->() RETURN r, {}",
                               relEntry->getName(), predicate);
    auto columns = getResultColumns(cypher, &context);
    NEUG_ASSERT(columns.size() == 2);
    return {relEntry, columns[0], columns[1]};
  } else {
    auto cypher =
        stringFormat("MATCH ()-[r:`{}`]->() RETURN r", relEntry->getName());
    auto columns = getResultColumns(cypher, &context);
    NEUG_ASSERT(columns.size() == 1);
    return {relEntry, columns[0], nullptr /* empty predicate */};
  }
}

static void validateNodeProjected(const table_id_set_t& connectedNodeTableIDSet,
                                  const table_id_set_t& projectedNodeIDSet,
                                  const std::string& relName, Catalog* catalog,
                                  transaction::Transaction* transaction) {
  for (auto id : connectedNodeTableIDSet) {
    if (!projectedNodeIDSet.contains(id)) {
      auto entryName =
          catalog->getTableCatalogEntry(transaction, id)->getName();
      THROW_BINDER_EXCEPTION(stringFormat(
          "{} is connected to {} but not projected.", entryName, relName));
    }
  }
}

static void validateRelSrcDstNodeAreProjected(
    const TableCatalogEntry& entry, const table_id_set_t& projectedNodeIDSet,
    Catalog* catalog, transaction::Transaction* transaction) {
  if (entry.getType() != CatalogEntryType::REL_TABLE_ENTRY) {
    THROW_BINDER_EXCEPTION(
        stringFormat("{} is not a rel table entry.", entry.getName()));
  }
  const auto& relEntry = entry.constCast<RelTableCatalogEntry>();
  validateNodeProjected({relEntry.getSrcTableID()}, projectedNodeIDSet,
                        relEntry.getName(), catalog, transaction);
  validateNodeProjected({relEntry.getDstTableID()}, projectedNodeIDSet,
                        relEntry.getName(), catalog, transaction);
}

// parse edgeTableName in format '[src, edge, dst]' into [src, edge, dst]
// triplets pay attention to the whitespace in the string, i.e, [ src,  edge,
// dst  ]
static std::vector<std::string> parseTriplets(
    const std::string& edgeTableName) {
  auto trimmed =
      common::StringUtils::rtrim(common::StringUtils::ltrim(edgeTableName));
  if (trimmed.size() < 2u || trimmed.front() != '[' || trimmed.back() != ']') {
    THROW_BINDER_EXCEPTION(stringFormat(
        "Invalid edge triplet format '{}', expected '[src, edge, dst]'.",
        edgeTableName));
  }
  std::string_view inner(trimmed.data() + 1, trimmed.size() - 2u);
  inner = common::StringUtils::rtrim(common::StringUtils::ltrim(inner));

  std::vector<std::string> parts;
  parts.reserve(3);
  size_t start = 0;
  while (start <= inner.size()) {
    const auto comma = inner.find(',', start);
    const auto segment = comma == std::string_view::npos
                             ? inner.substr(start)
                             : inner.substr(start, comma - start);
    auto piece =
        common::StringUtils::rtrim(common::StringUtils::ltrim(segment));
    parts.emplace_back(std::string(piece));
    if (comma == std::string_view::npos) {
      break;
    }
    start = comma + 1;
  }

  if (parts.size() != 3u) {
    THROW_BINDER_EXCEPTION(stringFormat(
        "Invalid edge triplet '{}': expected exactly 3 comma-separated names "
        "inside [...], got {}.",
        edgeTableName, parts.size()));
  }
  for (const auto& name : parts) {
    if (name.empty()) {
      THROW_BINDER_EXCEPTION(stringFormat(
          "Invalid edge triplet '{}': empty src, edge, or dst name.",
          edgeTableName));
    }
  }
  return parts;
}

GraphEntry GDSFunction::bindGraphEntry(main::ClientContext& context,
                                       const ParsedGraphEntry& entry) {
  auto* catalog = context.getCatalog();
  auto* transaction = context.getTransaction();
  GraphEntry result;
  table_id_set_t projectedNodeTableIDSet;
  for (auto& nodeInfo : entry.nodeInfos) {
    auto boundInfo =
        bindNodeEntry(context, nodeInfo.tableName, nodeInfo.predicate);
    if (boundInfo.predicate) {
      binder::RenameDependentVar renameVar(gopt::DEFAULT_ALIAS_NAME);
      renameVar.visit(boundInfo.predicate);
    }
    projectedNodeTableIDSet.insert(boundInfo.entry->getTableID());
    result.nodeInfos.push_back(std::move(boundInfo));
  }
  for (auto& relInfo : entry.relInfos) {
    const auto& triplets = parseTriplets(relInfo.tableName);
    auto boundInfo = bindRelEntry(context, triplets, relInfo.predicate);
    if (boundInfo.predicate) {
      binder::RenameDependentVar renameVar(gopt::DEFAULT_ALIAS_NAME);
      renameVar.visit(boundInfo.predicate);
    }
    validateRelSrcDstNodeAreProjected(*boundInfo.entry, projectedNodeTableIDSet,
                                      catalog, transaction);
    result.relInfos.push_back(std::move(boundInfo));
  }
  return result;
}

}  // namespace graph
}  // namespace neug
