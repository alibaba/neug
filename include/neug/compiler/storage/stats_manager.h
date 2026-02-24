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

#pragma once

#include <rapidjson/document.h>
#include <filesystem>
#include <unordered_map>
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/catalog/catalog_entry/table_catalog_entry.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/main/metadata_manager.h"
#include "neug/compiler/storage/buffer_manager/memory_manager.h"
#include "neug/compiler/storage/store/table.h"

namespace neug {
namespace main {
class MetadataManager;
}

namespace catalog {
class CatalogEntry;
}

namespace storage {
class Table;
class DiskArrayCollection;

class NEUG_API StatsManager {
 public:
  StatsManager(MemoryManager& memoryManager) : memoryManager(memoryManager) {}

  StatsManager(const std::filesystem::path& statsPath,
               main::MetadataManager* database, MemoryManager& memoryManager);

  StatsManager(const std::string& statsData, main::MetadataManager* catalog,
               MemoryManager& memoryManager);

  ~StatsManager() = default;

  Table* getTable(common::table_id_t tableID);

  void loadTables(const catalog::Catalog& catalog,
                  common::VirtualFileSystem* vfs,
                  main::ClientContext* context) {}

 private:
  main::MetadataManager* database;
  std::unordered_map<common::table_id_t, std::unique_ptr<Table>> tables;
  MemoryManager& memoryManager;
  std::mutex mtx;

 private:
  void loadStats(
      const catalog::Catalog& catalog,
      const std::unordered_map<std::string, common::row_idx_t>& countMap);
  void getCardMap(const std::string& jsonData,
                  std::unordered_map<std::string, common::row_idx_t>& countMap);
  Table* getTableByName(common::table_id_t tableID,
                        catalog::TableCatalogEntry* curEntry);
  bool checkTableConsistency(Table* oldTable,
                             catalog::TableCatalogEntry* curEntry);
};

}  // namespace storage
}  // namespace neug
