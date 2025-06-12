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

#include "gopt/g_storage_manager.h"
#include <fstream>

#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "common/exception/exception.h"
#include "gopt/g_constants.h"
#include "gopt/g_node_table.h"
#include "gopt/g_rel_table.h"
#include "json.hpp"

namespace gs {
namespace storage {
GStorageManager::GStorageManager(const std::string& statsData,
                                 const catalog::Catalog& catalog,
                                 MemoryManager& memoryManager,
                                 gs::storage::WAL& wal)
    : StorageManager(memoryManager), wal(wal) {
  std::unordered_map<std::string, common::row_idx_t> countMap;
  getCardMap(statsData, countMap);
  loadStats(catalog, countMap);
}

GStorageManager::GStorageManager(const std::filesystem::path& statsPath,
                                 const catalog::Catalog& catalog,
                                 MemoryManager& memoryManager,
                                 gs::storage::WAL& wal)
    : StorageManager(memoryManager), wal(wal) {
  std::ifstream file(statsPath);
  if (!file.is_open()) {
    throw common::Exception("Statistics file " + statsPath.string() +
                            " not found");
  }

  std::stringstream buffer;
  buffer << file.rdbuf();
  std::string statsJson = buffer.str();

  std::unordered_map<std::string, common::row_idx_t> countMap;
  getCardMap(statsJson, countMap);
  loadStats(catalog, countMap);
}

void GStorageManager::getCardMap(
    const std::string& json,
    std::unordered_map<std::string, common::row_idx_t>& countMap) {
  try {
    if (json.empty()) {
      // If JSON is empty, just return an empty countMap
      return;
    }
    nlohmann::json jsonData = nlohmann::json::parse(json);
    // Process node statistics if valid
    if (jsonData.contains("vertex_type_statistics") &&
        jsonData["vertex_type_statistics"].is_array()) {
      for (const auto& nodeStat : jsonData["vertex_type_statistics"]) {
        if (nodeStat.contains("type_name") && nodeStat.contains("count") &&
            nodeStat["type_name"].is_string() &&
            nodeStat["count"].is_number()) {
          auto nodeName = nodeStat["type_name"].get<std::string>();
          countMap[nodeName] = nodeStat["count"].get<common::row_idx_t>();
        }
      }
    }

    // Process relationship statistics if valid
    if (jsonData.contains("edge_type_statistics") &&
        jsonData["edge_type_statistics"].is_array()) {
      for (const auto& relStat : jsonData["edge_type_statistics"]) {
        if (relStat.contains("type_name") &&
            relStat.contains("vertex_type_pair_statistics") &&
            relStat["type_name"].is_string() &&
            relStat["vertex_type_pair_statistics"].is_array()) {
          auto relName = relStat["type_name"].get<std::string>();
          common::row_idx_t totalCount = 0;

          for (const auto& srcDst : relStat["vertex_type_pair_statistics"]) {
            if (srcDst.contains("source_vertex") &&
                srcDst.contains("destination_vertex") &&
                srcDst.contains("count") &&
                srcDst["source_vertex"].is_string() &&
                srcDst["destination_vertex"].is_string() &&
                srcDst["count"].is_number()) {
              auto srcName = srcDst["source_vertex"].get<std::string>();
              auto dstName = srcDst["destination_vertex"].get<std::string>();
              auto childName =
                  gs::catalog::RelGroupCatalogEntry::getChildTableName(
                      relName, srcName, dstName);
              auto count = srcDst["count"].get<common::row_idx_t>();
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