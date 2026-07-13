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
#include "neug/compiler/graph/graph_entry.h"
#include "neug/compiler/main/option_config.h"
#include "neug/compiler/storage/buffer_manager/memory_manager.h"
#include "neug/storages/graph/graph_stats.h"
#include "neug/utils/api.h"
#include "neug/utils/io/vfs/file_system.h"

namespace neug {
namespace common {
class FileSystem;
}  // namespace common

namespace catalog {
class CatalogEntry;
}

class GraphStats;
class Schema;

namespace function {
struct Function;
}

namespace extension {
struct ExtensionUtils;
class ExtensionManager;
}  // namespace extension

namespace storage {
class StorageExtension;
}  // namespace storage

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

  NEUG_API neug::fsys::FileSystemRegistry* getVFS() const { return vfs.get(); }

  std::unique_ptr<MetadataManager> clone(const Schema* schema,
                                         const GraphStats& stats) const;

  std::shared_ptr<GraphStats> getGraphStats() const;

  graph::GraphEntrySet& getGraphEntrySetUnsafe();

  const graph::GraphEntrySet& getGraphEntrySet() const;

 private:
  MetadataManager(std::unique_ptr<catalog::Catalog> catalog,
                  GraphStats statsManager,
                  std::shared_ptr<storage::MemoryManager> memoryManager,
                  std::shared_ptr<neug::fsys::FileSystemRegistry> vfs,
                  std::shared_ptr<extension::ExtensionManager> extensionManager,
                  std::shared_ptr<graph::GraphEntrySet> graphEntrySet);

  std::unique_ptr<catalog::Catalog> catalog;
  GraphStats statsManager;
  std::shared_ptr<storage::MemoryManager> memoryManager;
  std::shared_ptr<neug::fsys::FileSystemRegistry> vfs;
  std::shared_ptr<extension::ExtensionManager> extensionManager;
  std::shared_ptr<graph::GraphEntrySet> graphEntrySet;
};

}  // namespace main
}  // namespace neug
