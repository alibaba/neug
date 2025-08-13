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

#include <mutex>

#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/storage/buffer_manager/memory_manager.h"
#include "neug/compiler/storage/store/table.h"
#include "neug/compiler/storage/wal/wal.h"

namespace gs {
namespace main {
class Database;
}

namespace catalog {
class CatalogEntry;
}

namespace storage {
class Table;
class DiskArrayCollection;

class KUZU_API StorageManager {
 public:
  StorageManager(MemoryManager& memoryManager) : memoryManager(memoryManager) {}

  StorageManager(const std::string& databasePath, bool readOnly,
                 const catalog::Catalog& catalog, MemoryManager& memoryManager,
                 bool enableCompression, common::VirtualFileSystem* vfs,
                 main::ClientContext* context)
      : memoryManager(memoryManager) {}

  virtual ~StorageManager() = default;

  static void recover(main::ClientContext& clientContext);

  void createTable(catalog::CatalogEntry* entry, main::ClientContext* context);

  void checkpoint(main::ClientContext& clientContext);
  void finalizeCheckpoint(main::ClientContext& clientContext);
  void rollbackCheckpoint(main::ClientContext& clientContext);

  virtual Table* getTable(common::table_id_t tableID) {
    std::lock_guard lck{mtx};
    KU_ASSERT(tables.contains(tableID));
    return tables.at(tableID).get();
  }

  virtual WAL& getWAL() const = 0;
  std::string getDatabasePath() const { return databasePath; }
  bool isReadOnly() const { return readOnly; }
  bool compressionEnabled() const { return enableCompression; }

  virtual void loadTables(const catalog::Catalog& catalog,
                          common::VirtualFileSystem* vfs,
                          main::ClientContext* context) = 0;

 private:
 protected:
  std::unordered_map<common::table_id_t, std::unique_ptr<Table>> tables;
  MemoryManager& memoryManager;

 protected:
  std::mutex mtx;
  std::string databasePath;
  bool readOnly;
  bool enableCompression;
};

}  // namespace storage
}  // namespace gs
