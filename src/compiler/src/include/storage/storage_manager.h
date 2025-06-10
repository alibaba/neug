#pragma once

#include <mutex>

#include "catalog/catalog.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/store/table.h"
#include "storage/wal/wal.h"

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

  Table* getTable(common::table_id_t tableID) {
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

 private:
  std::mutex mtx;
  std::string databasePath;
  bool readOnly;
  bool enableCompression;
};

}  // namespace storage
}  // namespace gs
