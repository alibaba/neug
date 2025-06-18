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

#pragma once

#include <rapidjson/document.h>
#include <filesystem>
#include <unordered_map>
#include "storage/storage_manager.h"

namespace gs {
namespace storage {
class GStorageManager : public StorageManager {
 private:
  gs::storage::WAL& wal;

 public:
  GStorageManager(const std::filesystem::path& statsPath,
                  const catalog::Catalog& catalog, MemoryManager& memoryManager,
                  gs::storage::WAL& wal);

  GStorageManager(const std::string& statsData, const catalog::Catalog& catalog,
                  MemoryManager& memoryManager, gs::storage::WAL& wal);

  ~GStorageManager() override = default;

  WAL& getWAL() const override { return wal; }

  void loadTables(const catalog::Catalog& catalog,
                  common::VirtualFileSystem* vfs,
                  main::ClientContext* context) override {}

 private:
  void loadStats(
      const catalog::Catalog& catalog,
      const std::unordered_map<std::string, common::row_idx_t>& countMap);
  void getCardMap(const std::string& jsonData,
                  std::unordered_map<std::string, common::row_idx_t>& countMap);
};
}  // namespace storage
}  // namespace gs