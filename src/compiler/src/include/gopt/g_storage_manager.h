#pragma once

#include <filesystem>
#include <unordered_map>
#include "storage/storage_manager.h"
#include "third_party/nlohmann_json/json.hpp"

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