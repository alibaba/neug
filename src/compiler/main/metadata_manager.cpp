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

#include "neug/compiler/main/metadata_manager.h"

#include "neug/compiler/extension/extension_manager.h"
#include "neug/compiler/gopt/g_catalog.h"
#include "neug/compiler/main/client_context.h"
#include "neug/utils/yaml_utils.h"

#if defined(_WIN32)
#include <windows.h>
#else
#include <unistd.h>
#endif

#include "neug/compiler/common/file_system/virtual_file_system.h"
#include "neug/compiler/gopt/g_catalog_holder.h"
#include "neug/compiler/storage/stats_manager.h"

using namespace gs::catalog;
using namespace gs::common;
using namespace gs::storage;
using namespace gs::transaction;

namespace gs {
namespace main {

MetadataManager::MetadataManager() {
  this->vfs = std::make_unique<VirtualFileSystem>();
  this->extensionManager = std::make_unique<extension::ExtensionManager>();
  this->memoryManager = std::make_unique<gs::storage::MemoryManager>();
  // the catalog is initialized only once and is empty before data loading
  this->catalog = std::make_unique<gs::catalog::GCatalog>();
  catalog::GCatalogHolder::setGCatalog(
      this->catalog->ptrCast<catalog::GCatalog>());
  std::string emptyStats = "";
  this->storageManager = std::make_unique<gs::storage::StatsManager>(
      emptyStats, this, *this->memoryManager);
}

MetadataManager::~MetadataManager() = default;

void MetadataManager::updateSchema(const std::filesystem::path& schemaPath) {
  if (!this->catalog) {
    THROW_CATALOG_EXCEPTION("Catalog is not set");
  }
  this->catalog->ptrCast<gs::catalog::GCatalog>()->updateSchema(schemaPath);
}

void MetadataManager::updateSchema(const std::string& schema) {
  if (!this->catalog) {
    THROW_CATALOG_EXCEPTION("Catalog is not set");
  }
  this->catalog->ptrCast<gs::catalog::GCatalog>()->updateSchema(schema);
}

void MetadataManager::updateSchema(const YAML::Node& schema) {
  if (!this->catalog) {
    THROW_CATALOG_EXCEPTION("Catalog is not set");
  }
  this->catalog->ptrCast<gs::catalog::GCatalog>()->updateSchema(schema);
}

void MetadataManager::updateStats(const std::filesystem::path& statsPath) {
  this->storageManager = std::make_unique<gs::storage::StatsManager>(
      statsPath, this, *this->memoryManager);
}

void MetadataManager::updateStats(const std::string& stats) {
  this->storageManager = std::make_unique<gs::storage::StatsManager>(
      stats, this, *this->memoryManager);
}

}  // namespace main
}  // namespace gs
