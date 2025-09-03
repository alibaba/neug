/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/compiler/gopt/g_storage_manager.h"
#include <fstream>
#include <sstream>

#include "neug/compiler/catalog/catalog_entry/rel_group_catalog_entry.h"
#include "neug/compiler/catalog/catalog_entry/rel_table_catalog_entry.h"
#include "neug/compiler/catalog/catalog_entry/table_catalog_entry.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/gopt/g_constants.h"
#include "neug/compiler/gopt/g_node_table.h"
#include "neug/compiler/gopt/g_rel_table.h"
#include "neug/compiler/main/database.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace storage {
GStorageManager::GStorageManager(const std::string& statsData,
                                 main::Database* database,
                                 MemoryManager& memoryManager,
                                 gs::storage::WAL& wal)
    : StorageManager(memoryManager), wal(wal), database(database) {
  std::unordered_map<std::string, common::row_idx_t> countMap;
  getCardMap(statsData, countMap);
  if (!database || !database->getCatalog()) {
    THROW_EXCEPTION_WITH_FILE_LINE("Database or catalog is not initialized");
  }
  loadStats(*database->getCatalog(), countMap);
}

bool GStorageManager::checkTableConsistency(
    Table* oldTable, catalog::TableCatalogEntry* curEntry) {
  if (oldTable->getTableType() != curEntry->getTableType() ||
      oldTable->getTableName() != curEntry->getName()) {
    return false;
  }
  if (oldTable->getTableType() == common::TableType::REL) {
    auto catalog = database->getCatalog();
    auto& transaction = gs::Constants::DEFAULT_TRANSACTION;
    // check if the src and dst table ids are consistent
    auto oldRelTable = oldTable->ptrCast<GRelTable>();
    if (!catalog->containsTable(&transaction, oldRelTable->getSrcTableId()) ||
        !catalog->containsTable(&transaction, oldRelTable->getDstTableId())) {
      return false;
    }
    auto oldSrcEntry = catalog->getTableCatalogEntry(
        &transaction, oldRelTable->getSrcTableId());
    auto oldDstEntry = catalog->getTableCatalogEntry(
        &transaction, oldRelTable->getDstTableId());
    auto curRelEntry = curEntry->ptrCast<catalog::RelTableCatalogEntry>();
    auto curSrcEntry = catalog->getTableCatalogEntry(
        &transaction, curRelEntry->getSrcTableID());
    auto curDstEntry = catalog->getTableCatalogEntry(
        &transaction, curRelEntry->getDstTableID());
    if (curSrcEntry->getTableType() != oldSrcEntry->getTableType() ||
        curDstEntry->getTableType() != oldDstEntry->getTableType() ||
        curSrcEntry->getName() != oldSrcEntry->getName() ||
        curDstEntry->getName() != oldDstEntry->getName()) {
      return false;
    }
  }
  return true;
}

// check if cached table data is consistent with the schema, return false if
// not, otherwise return true
Table* GStorageManager::getTableByName(common::table_id_t tableID,
                                       catalog::TableCatalogEntry* curEntry) {
  if (tables.contains(tableID)) {
    auto oldTable = tables.at(tableID).get();
    if (checkTableConsistency(oldTable, curEntry)) {
      return oldTable;
    }
  }
  for (auto& [_, table] : tables) {
    if (checkTableConsistency(table.get(), curEntry)) {
      return table.get();
    }
  }
  return nullptr;
}

Table* GStorageManager::getTable(common::table_id_t tableID) {
  auto& transaction = gs::Constants::DEFAULT_TRANSACTION;
  auto catalog = database->getCatalog();
  if (!catalog) {
    THROW_EXCEPTION_WITH_FILE_LINE("Catalog is not initialized");
  }
  KU_ASSERT(catalog->containsTable(&transaction, tableID));
  auto curEntry = catalog->getTableCatalogEntry(&transaction, tableID);
  Table* oldTable = getTableByName(tableID, curEntry);
  if (oldTable) {
    return oldTable;
  }
  switch (curEntry->getTableType()) {
  case common::TableType::NODE: {
    auto defaultNode = std::make_unique<GNodeTable>(
        curEntry->ptrCast<catalog::NodeTableCatalogEntry>(), this,
        &memoryManager, 1);
    return defaultNode.release();
  }
  case common::TableType::REL:
  default: {
    auto defaultRel = std::make_unique<GRelTable>(
        1, curEntry->ptrCast<catalog::RelTableCatalogEntry>(), this);
    return defaultRel.release();
  }
  }
}

GStorageManager::GStorageManager(const std::filesystem::path& statsPath,
                                 main::Database* database,
                                 MemoryManager& memoryManager,
                                 gs::storage::WAL& wal)
    : StorageManager(memoryManager), wal(wal), database(database) {
  std::ifstream file(statsPath);
  if (!file.is_open()) {
    THROW_EXCEPTION_WITH_FILE_LINE("Statistics file " + statsPath.string() +
                                   " not found");
  }

  std::stringstream buffer;
  buffer << file.rdbuf();
  std::string statsJson = buffer.str();

  std::unordered_map<std::string, common::row_idx_t> countMap;
  getCardMap(statsJson, countMap);
  if (!database || !database->getCatalog()) {
    THROW_EXCEPTION_WITH_FILE_LINE("Database or catalog is not initialized");
  }
  loadStats(*database->getCatalog(), countMap);
}

void GStorageManager::getCardMap(
    const std::string& json,
    std::unordered_map<std::string, common::row_idx_t>& countMap) {
  try {
    if (json.empty()) {
      // If JSON is empty, just return an empty countMap
      return;
    }

    rapidjson::Document jsonData;

    if (jsonData.Parse(json.c_str()).HasParseError()) {
      THROW_EXCEPTION_WITH_FILE_LINE("Invalid JSON format: " +
                                     std::to_string(jsonData.GetParseError()));
    }
    // Process node statistics if valid
    if (jsonData.HasMember("vertex_type_statistics") &&
        jsonData["vertex_type_statistics"].IsArray()) {
      for (const auto& nodeStat :
           jsonData["vertex_type_statistics"].GetArray()) {
        if (nodeStat.HasMember("type_name") && nodeStat.HasMember("count") &&
            nodeStat["type_name"].IsString() && nodeStat["count"].IsInt64()) {
          auto nodeName = nodeStat["type_name"].GetString();
          countMap[nodeName] = nodeStat["count"].GetInt64();
        }
      }
    }

    if (jsonData.HasMember("edge_type_statistics") &&
        jsonData["edge_type_statistics"].IsArray()) {
      for (const auto& relStat : jsonData["edge_type_statistics"].GetArray()) {
        if (relStat.HasMember("type_name") &&
            relStat.HasMember("vertex_type_pair_statistics") &&
            relStat["type_name"].IsString() &&
            relStat["vertex_type_pair_statistics"].IsArray()) {
          auto relName = relStat["type_name"].GetString();
          common::row_idx_t totalCount = 0;

          for (const auto& srcDst :
               relStat["vertex_type_pair_statistics"].GetArray()) {
            if (srcDst.HasMember("source_vertex") &&
                srcDst.HasMember("destination_vertex") &&
                srcDst.HasMember("count") &&
                srcDst["source_vertex"].IsString() &&
                srcDst["destination_vertex"].IsString() &&
                srcDst["count"].IsInt64()) {
              auto srcName = srcDst["source_vertex"].GetString();
              auto dstName = srcDst["destination_vertex"].GetString();
              auto childName =
                  gs::catalog::RelGroupCatalogEntry::getChildTableName(
                      relName, srcName, dstName);
              auto count = srcDst["count"].GetInt64();
              countMap[childName] = count;
              totalCount += count;
            }
          }
          countMap[relName] = totalCount;
        }
      }
    }
  } catch (const std::exception& e) {
    // If any error occurs during JSON processing, just continue with empty
    // countMap All counts will default to 0
  }
}

void GStorageManager::loadStats(
    const catalog::Catalog& catalog,
    const std::unordered_map<std::string, common::row_idx_t>& countMap) {
  auto& transaction = gs::Constants::DEFAULT_TRANSACTION;

  // Process all node tables from catalog
  for (auto& tableEntry : catalog.getTableEntries(&transaction)) {
    if (tableEntry->getType() == catalog::CatalogEntryType::NODE_TABLE_ENTRY) {
      auto* nodeTableEntry =
          dynamic_cast<catalog::NodeTableCatalogEntry*>(tableEntry);
      auto nodeName = nodeTableEntry->getName();
      auto count = countMap.count(nodeName) ? countMap.at(nodeName) : 1;
      tables[nodeTableEntry->getTableID()] = std::make_unique<GNodeTable>(
          nodeTableEntry, this, &memoryManager, count);
    } else if (tableEntry->getType() ==
               catalog::CatalogEntryType::REL_TABLE_ENTRY) {
      auto* relTableEntry =
          dynamic_cast<catalog::RelTableCatalogEntry*>(tableEntry);
      auto relName = relTableEntry->getName();
      auto count = countMap.count(relName) ? countMap.at(relName) : 1;
      tables[relTableEntry->getTableID()] =
          std::make_unique<gs::storage::GRelTable>(count, relTableEntry, this);
    }
  }
}
}  // namespace storage
}  // namespace gs