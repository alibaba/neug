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

#include <mutex>
#include <string>
#include <unordered_map>
#include "neug/compiler/gopt/g_catalog.h"
#include "neug/compiler/main/metadata_manager.h"
#include "neug/compiler/main/metadata_registry.h"

namespace neug {
namespace extension {

/**
 * @brief Basic information for an extension.
 *
 * ExtensionInfo describes the metadata of an extension.
 * @field name        Unique name of the extension.
 * @field description Brief description of the extension's functionality.
 */
struct ExtensionInfo {
  std::string name;
  std::string description;
};

class ExtensionAPI {
 public:
  template <typename T>
  static void registerFunction(catalog::CatalogEntryType entryType) {
    auto gCatalog = neug::main::MetadataRegistry::getCatalog();
    if (gCatalog->containsFunction(&neug::transaction::DUMMY_TRANSACTION,
                                   T::name, false)) {
      return;
    }
    gCatalog->addFunctionWithSignature(&neug::transaction::DUMMY_TRANSACTION,
                                       entryType, T::name, T::getFunctionSet(),
                                       false);
  }

  template <typename T>
  static void registerFunctionAlias(catalog::CatalogEntryType entryType) {
    auto gCatalog = neug::main::MetadataRegistry::getCatalog();
    gCatalog->addFunctionWithSignature(&neug::transaction::DUMMY_TRANSACTION,
                                       entryType, T::name,
                                       T::alias::getFunctionSet(), false);
  }

  // Register file system factory for specific protocol.
  // For example, register "file" protocol file system factory:
  // registerFileSystem("file", [](const reader::FileSchema& schema) {
  //   return std::make_unique<LocalFileSystem>(schema);
  // });
  static void registerFileSystem(const std::string& protocol,
                                 neug::fsys::FileSystemFactory factory) {
    auto vfs = neug::main::MetadataRegistry::getVFS();
    vfs->Register(protocol, std::move(factory));
  }

  static void registerExtension(const ExtensionInfo& info);
  static const std::unordered_map<std::string, ExtensionInfo>&
  getLoadedExtensions();

 private:
  static std::unordered_map<std::string, ExtensionInfo> loaded_extensions_;
  static std::mutex extensions_mutex_;
};

}  // namespace extension
}  // namespace neug