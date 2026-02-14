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
#include "neug/compiler/gopt/g_vfs_holder.h"
#include "neug/compiler/storage/stats_manager.h"

using namespace neug::catalog;
using namespace neug::common;
using namespace neug::storage;
using namespace neug::transaction;

namespace neug {
namespace main {

MetadataManager::MetadataManager() {
  this->vfs = std::make_unique<VirtualFileSystem>();
  common::VFSHolder::setVFS(this->vfs.get());
  this->extensionManager = std::make_unique<extension::ExtensionManager>();
  this->memoryManager = std::make_unique<neug::storage::MemoryManager>();
  // the catalog is initialized only once and is empty before data loading
  this->catalog = std::make_unique<neug::catalog::GCatalog>();
  catalog::GCatalogHolder::setGCatalog(
      this->catalog->ptrCast<catalog::GCatalog>());
  std::string emptyStats = "";
  auto statsManager = std::make_shared<neug::storage::StatsManager>(
      emptyStats, this, *this->memoryManager);
  this->statsManager = std::move(statsManager);
}

MetadataManager::~MetadataManager() = default;

std::shared_ptr<storage::StatsManager> MetadataManager::getStatsManager()
    const {
  // spin until we own the lock
  while (statsManagerLock.test_and_set(std::memory_order_acquire)) {}
  std::shared_ptr<storage::StatsManager> copy = statsManager;
  statsManagerLock.clear(std::memory_order_release);
  return copy;
}

void MetadataManager::updateSchema(const std::filesystem::path& schemaPath) {
  if (!this->catalog) {
    THROW_CATALOG_EXCEPTION("Catalog is not set");
  }
  this->catalog->ptrCast<neug::catalog::GCatalog>()->updateSchema(schemaPath);
}

void MetadataManager::updateSchema(const std::string& schema) {
  if (!this->catalog) {
    THROW_CATALOG_EXCEPTION("Catalog is not set");
  }
  this->catalog->ptrCast<neug::catalog::GCatalog>()->updateSchema(schema);
}

void MetadataManager::updateSchema(const YAML::Node& schema) {
  if (!this->catalog) {
    THROW_CATALOG_EXCEPTION("Catalog is not set");
  }
  this->catalog->ptrCast<neug::catalog::GCatalog>()->updateSchema(schema);
}

void MetadataManager::updateStats(const std::filesystem::path& statsPath) {
  auto newManager = std::make_shared<neug::storage::StatsManager>(
      statsPath, this, *this->memoryManager);
  while (this->statsManagerLock.test_and_set(std::memory_order_acquire)) {}
  this->statsManager = std::move(newManager);
  this->statsManagerLock.clear(std::memory_order_release);
}

void MetadataManager::updateStats(const std::string& stats) {
  auto newManager = std::make_shared<neug::storage::StatsManager>(
      stats, this, *this->memoryManager);
  while (this->statsManagerLock.test_and_set(std::memory_order_acquire)) {}
  this->statsManager = std::move(newManager);
  this->statsManagerLock.clear(std::memory_order_release);
}

}  // namespace main
}  // namespace neug
