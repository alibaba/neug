#pragma once

#include <yaml-cpp/node/node.h>
#include <filesystem>
#include "gopt/g_catalog.h"
#include "gopt/g_constants.h"
#include "gopt/g_storage_manager.h"
#include "gopt/g_transaction_manager.h"
#include "main/database.h"

namespace kuzu {
namespace main {
class GDatabase : public Database {
 private:
  std::unique_ptr<kuzu::storage::WAL> wal;

 public:
  GDatabase(const kuzu::main::SystemConfig& sysConfig) : Database(sysConfig) {
    this->memoryManager = std::make_unique<kuzu::storage::MemoryManager>();
    this->wal = std::make_unique<kuzu::storage::WAL>();
    this->transactionManager =
        std::make_unique<kuzu::transaction::GTransactionManager>(*this->wal);
  };

  void updateSchema(const std::filesystem::path& schemaPath,
                    const std::filesystem::path& statsPath) {
    this->catalog = std::make_unique<kuzu::catalog::GCatalog>(schemaPath);
    this->storageManager = std::make_unique<kuzu::storage::GStorageManager>(
        statsPath, *this->catalog, *this->memoryManager, *this->wal);
  }

  void updateSchema(const std::string& schema, const std::string& stats) {
    this->catalog = std::make_unique<kuzu::catalog::GCatalog>(schema);
    this->storageManager = std::make_unique<kuzu::storage::GStorageManager>(
        stats, *this->catalog, *this->memoryManager, *this->wal);
  }
};
}  // namespace main
}  // namespace kuzu