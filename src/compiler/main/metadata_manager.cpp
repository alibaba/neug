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

#include "neug/compiler/gopt/g_catalog.h"
#include "neug/compiler/main/client_context.h"

#if defined(_WIN32)
#include <windows.h>
#else
#include <unistd.h>
#endif

#include "neug/storages/graph/graph_stats.h"

using namespace neug::catalog;
using namespace neug::common;
using namespace neug::storage;
using namespace neug::transaction;

namespace neug {
namespace main {

MetadataManager::MetadataManager() {
  this->vfs = std::make_shared<neug::fsys::FileSystemRegistry>();
  this->memoryManager = std::make_shared<neug::storage::MemoryManager>();
  // the catalog is initialized only once and is empty before data loading
  this->catalog = std::make_unique<neug::catalog::GCatalog>();
  this->statsManager = neug::GraphStats();
}

MetadataManager::~MetadataManager() = default;

MetadataManager::MetadataManager(
    std::unique_ptr<catalog::Catalog> catalog, GraphStats statsManager,
    std::shared_ptr<storage::MemoryManager> memoryManager,
    std::shared_ptr<neug::fsys::FileSystemRegistry> vfs)
    : catalog{std::move(catalog)},
      statsManager{std::move(statsManager)},
      memoryManager{std::move(memoryManager)},
      vfs{std::move(vfs)} {}

std::unique_ptr<MetadataManager> MetadataManager::clone(
    const Schema* schema, const GraphStats& stats) const {
  if (!catalog) {
    THROW_CATALOG_EXCEPTION("Catalog is not set");
  }
  return std::unique_ptr<MetadataManager>(
      new MetadataManager(catalog->clone(schema), stats, memoryManager, vfs));
}

std::shared_ptr<GraphStats> MetadataManager::getGraphStats() const {
  return std::make_shared<GraphStats>(statsManager);
}

}  // namespace main
}  // namespace neug
