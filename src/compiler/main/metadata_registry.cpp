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

#include "neug/compiler/main/metadata_registry.h"

#include "neug/compiler/gopt/g_catalog.h"
#include "neug/compiler/main/metadata_manager.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace main {

MetadataManager* MetadataRegistry::metadataManager = nullptr;

void MetadataRegistry::registerMetadata(
    main::MetadataManager* metadataManager) {
  if (!metadataManager) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Metadata manager is not set");
  }
  MetadataRegistry::metadataManager = metadataManager;
}

MetadataManager* MetadataRegistry::getMetadata() {
  if (!metadataManager) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Metadata manager is not set");
  }
  return metadataManager;
}

catalog::GCatalog* MetadataRegistry::getCatalog() {
  auto metadataManager = MetadataRegistry::getMetadata();
  auto catalog =
      dynamic_cast<catalog::GCatalog*>(metadataManager->getCatalog());
  if (!catalog) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Catalog is not set");
  }
  return catalog;
}

neug::fsys::FileSystemRegistry* MetadataRegistry::getVFS() {
  auto metadataManager = MetadataRegistry::getMetadata();
  auto vfs = metadataManager->getVFS();
  if (!vfs) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Virtual file system is not set");
  }
  return vfs;
}
}  // namespace main
}  // namespace neug
