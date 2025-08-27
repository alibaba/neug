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

#include <memory>
#include <string>

#include "neug/compiler/extension/catalog_extension.h"
#include "neug/compiler/transaction/transaction_manager.h"

namespace duckdb {
class MaterializedQueryResult;
}

namespace gs {
namespace storage {
class StorageManager;
}  // namespace storage

namespace main {

class AttachedDatabase {
 public:
  AttachedDatabase(std::string dbName, std::string dbType,
                   std::unique_ptr<extension::CatalogExtension> catalog)
      : dbName{std::move(dbName)},
        dbType{std::move(dbType)},
        catalog{std::move(catalog)} {}

  virtual ~AttachedDatabase() = default;

  std::string getDBName() const { return dbName; }

  std::string getDBType() const { return dbType; }

  catalog::Catalog* getCatalog() { return catalog.get(); }

  std::unique_ptr<duckdb::MaterializedQueryResult> executeQuery(
      const std::string& query);

  void invalidateCache();

  template <class TARGET>
  const TARGET& constCast() const {
    return common::ku_dynamic_cast<const TARGET&>(*this);
  }

 protected:
  std::string dbName;
  std::string dbType;
  std::unique_ptr<catalog::Catalog> catalog;
};

class AttachedKuzuDatabase : public AttachedDatabase {
 public:
  AttachedKuzuDatabase(std::string dbPath, std::string dbName,
                       std::string dbType, ClientContext* clientContext);

  storage::StorageManager* getStorageManager() { return storageManager.get(); }

  transaction::TransactionManager* getTransactionManager() {
    return transactionManager.get();
  }

 private:
  void initCatalog(const std::string& path, ClientContext* context);

 private:
  std::unique_ptr<storage::StorageManager> storageManager;
  std::unique_ptr<transaction::TransactionManager> transactionManager;
};

}  // namespace main
}  // namespace gs
