#pragma once

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
    GDatabase(const std::string& schemaPath, const std::string& statsPath,
        const kuzu::main::SystemConfig& sysConfig)
        : Database(sysConfig) {
        this->catalog = std::make_unique<kuzu::catalog::GCatalog>(schemaPath);
        this->memoryManager = std::make_unique<kuzu::storage::MemoryManager>();
        this->storageManager = std::make_unique<kuzu::storage::GStorageManager>(statsPath,
            *this->catalog, *this->memoryManager, *this->wal);
        this->wal = std::make_unique<kuzu::storage::WAL>();
        this->transactionManager =
            std::make_unique<kuzu::transaction::GTransactionManager>(*this->wal);
    };
};
} // namespace main
} // namespace kuzu