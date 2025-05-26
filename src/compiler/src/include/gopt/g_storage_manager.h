#pragma once

#include "storage/storage_manager.h"

namespace kuzu {
namespace storage {
class GStorageManager : public StorageManager {
 private:
  std::string statsPath;
  kuzu::storage::WAL& wal;

 public:
  GStorageManager(const std::string& statsPath, const catalog::Catalog& catalog,
                  MemoryManager& memoryManager, kuzu::storage::WAL& wal)
      : StorageManager(memoryManager),
        statsPath(std::move(statsPath)),
        wal(wal) {
    loadTables(catalog, nullptr, nullptr);
  }

  ~GStorageManager() override = default;

  void loadTables(const catalog::Catalog& catalog,
                  common::VirtualFileSystem* vfs,
                  main::ClientContext* context) override;

  WAL& getWAL() const override { return wal; }
};
}  // namespace storage
}  // namespace kuzu