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

#include "neug/compiler/gopt/g_catalog.h"
#include "neug/compiler/main/metadata_manager.h"

namespace neug {
namespace main {

class MetadataRegistry {
 private:
  // MetadataManger is a single instance, there will be only one instance of
  // MetadataManager in the lifetime of the database.
  static MetadataManager* metadataManager;

 public:
  MetadataRegistry() = delete;

  static void registerMetadata(main::MetadataManager* metadataManager);

  static MetadataManager* getMetadata();

  static catalog::GCatalog* getCatalog();

  static neug::fsys::FileSystemRegistry* getVFS();
};
}  // namespace main
}  // namespace neug
