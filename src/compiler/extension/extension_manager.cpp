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

#include "neug/compiler/extension/extension_manager.h"

#include "generated_extension_loader.h"
#include "neug/compiler/common/file_system/virtual_file_system.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/extension/extension.h"

namespace gs {
namespace extension {

static void executeExtensionLoader(main::ClientContext* context,
                                   const std::string& extensionName) {
  auto loaderPath =
      ExtensionUtils::getLocalPathForExtensionLoader(context, extensionName);
  if (context->getVFSUnsafe()->fileOrPathExists(loaderPath)) {
    auto libLoader = ExtensionLibLoader(extensionName, loaderPath);
    auto load = libLoader.getLoadFunc();
    (*load)(context);
  }
}

void ExtensionManager::loadExtension(const std::string& path,
                                     main::ClientContext* context) {
  auto fullPath = path;
  bool isOfficial = extension::ExtensionUtils::isOfficialExtension(path);
  if (isOfficial) {
    auto localPathForSharedLib =
        ExtensionUtils::getLocalPathForSharedLib(context);
    if (!context->getVFSUnsafe()->fileOrPathExists(localPathForSharedLib)) {
      context->getVFSUnsafe()->createDir(localPathForSharedLib);
    }
    executeExtensionLoader(context, path);
    fullPath = ExtensionUtils::getLocalPathForExtensionLib(context, path);
  }

  auto libLoader = ExtensionLibLoader(path, fullPath);
  auto init = libLoader.getInitFunc();
  (*init)(context);
  auto name = libLoader.getNameFunc();
  auto extensionName = (*name)();
  loadedExtensions.push_back(LoadedExtension(
      extensionName, fullPath,
      isOfficial ? ExtensionSource::OFFICIAL : ExtensionSource::USER));
}

std::string ExtensionManager::toCypher() {
  std::string cypher;
  for (auto& extension : loadedExtensions) {
    cypher += extension.toCypher();
  }
  return cypher;
}

void ExtensionManager::addExtensionOption(std::string name,
                                          common::LogicalTypeID type,
                                          common::Value defaultValue,
                                          bool isConfidential) {
  if (getExtensionOption(name) != nullptr) {
    return;
  }
  common::StringUtils::toLower(name);
  extensionOptions.emplace(
      name, main::ExtensionOption{name, type, std::move(defaultValue),
                                  isConfidential});
}

const main::ExtensionOption* ExtensionManager::getExtensionOption(
    std::string name) const {
  common::StringUtils::toLower(name);
  return extensionOptions.contains(name) ? &extensionOptions.at(name) : nullptr;
}

void ExtensionManager::autoLoadLinkedExtensions(main::ClientContext* context) {
  loadLinkedExtensions(context);
}

}  // namespace extension
}  // namespace gs
