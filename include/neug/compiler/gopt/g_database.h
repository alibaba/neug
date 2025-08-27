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

#include <yaml-cpp/node/node.h>
#include <filesystem>
#include "neug/compiler/gopt/g_catalog.h"
#include "neug/compiler/gopt/g_constants.h"
#include "neug/compiler/gopt/g_storage_manager.h"
#include "neug/compiler/gopt/g_transaction_manager.h"
#include "neug/compiler/main/database.h"

namespace gs {
namespace main {
class GDatabase : public Database {
 private:
  std::unique_ptr<gs::storage::WAL> wal;

 public:
  GDatabase(const gs::main::SystemConfig& sysConfig) : Database(sysConfig) {
    this->memoryManager = std::make_unique<gs::storage::MemoryManager>();
    this->wal = std::make_unique<gs::storage::WAL>();
    this->transactionManager =
        std::make_unique<gs::transaction::GTransactionManager>(*this->wal);
  };

  ~GDatabase() = default;

  void updateSchema(const std::filesystem::path& schemaPath) {
    this->catalog = std::make_unique<gs::catalog::GCatalog>(schemaPath);
  }

  void updateSchema(const std::string& schema) {
    this->catalog = std::make_unique<gs::catalog::GCatalog>(schema);
  }

  void updateSchema(const YAML::Node& schema) {
    this->catalog = std::make_unique<gs::catalog::GCatalog>(schema);
  }

  void updateStats(const std::filesystem::path& statsPath) {
    this->storageManager = std::make_unique<gs::storage::GStorageManager>(
        statsPath, this, *this->memoryManager, *this->wal);
  }

  void updateStats(const std::string& stats) {
    this->storageManager = std::make_unique<gs::storage::GStorageManager>(
        stats, this, *this->memoryManager, *this->wal);
  }
};
}  // namespace main
}  // namespace gs