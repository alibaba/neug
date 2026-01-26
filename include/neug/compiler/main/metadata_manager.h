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

#include <yaml-cpp/node/node.h>
#include <filesystem>
#include <memory>

#include "kuzu_fwd.h"
#include "neug/compiler/main/option_config.h"
#include "neug/compiler/storage/buffer_manager/memory_manager.h"
#include "neug/utils/api.h"

namespace neug {
namespace common {
class FileSystem;
enum class LogicalTypeID : uint8_t;
}  // namespace common

namespace catalog {
class CatalogEntry;
}

namespace function {
struct Function;
}

namespace extension {
struct ExtensionUtils;
class ExtensionManager;
}  // namespace extension

namespace storage {
class StorageExtension;
}

namespace main {
struct ExtensionOption;
class DatabaseManager;
class ClientContext;

/**
 * @brief Database class is the main class of Kuzu. It manages all database
 * components.
 */
class MetadataManager {
  friend class EmbeddedShell;
  friend class ClientContext;
  friend class Connection;
  friend class StorageDriver;
  friend class testing::BaseGraphTest;
  friend class testing::PrivateGraphTest;
  friend class transaction::TransactionContext;
  friend struct extension::ExtensionUtils;

 public:
  MetadataManager();
  /**
   * @brief Destructs the database object.
   */
  NEUG_API virtual ~MetadataManager();

  NEUG_API catalog::Catalog* getCatalog() { return catalog.get(); }

  void updateSchema(const std::filesystem::path& schemaPath);

  void updateSchema(const std::string& schema);

  void updateSchema(const YAML::Node& schema);

  void updateStats(const std::filesystem::path& statsPath);

  void updateStats(const std::string& stats);

 private:
  std::unique_ptr<catalog::Catalog> catalog;
  std::unique_ptr<storage::StatsManager> storageManager;
  std::unique_ptr<storage::MemoryManager> memoryManager;
  std::unique_ptr<common::VirtualFileSystem> vfs;
  std::unique_ptr<extension::ExtensionManager> extensionManager;
};

}  // namespace main
}  // namespace neug
