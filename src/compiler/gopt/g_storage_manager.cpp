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

#include "src/include/gopt/g_storage_manager.h"
#include <fstream>
#include <sstream>

#include "src/include/catalog/catalog_entry/rel_group_catalog_entry.h"
#include "src/include/common/exception/exception.h"
#include "src/include/gopt/g_constants.h"
#include "src/include/gopt/g_node_table.h"
#include "src/include/gopt/g_rel_table.h"

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

    rapidjson::Document jsonData;

    if (jsonData.Parse(json.c_str()).HasParseError()) {
      throw common::Exception("Invalid JSON format: " +
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