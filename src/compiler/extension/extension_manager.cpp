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
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/extension/extension.h"

namespace neug {
namespace extension {

std::mutex ExtensionManager::loaded_extensions_mutex_;
std::unordered_map<std::string, ExtensionManager::LoadedExtension>
    ExtensionManager::loaded_extensions_;

std::string ExtensionManager::NormalizeExtensionName(std::string name) {
  common::StringUtils::toLower(name);
  return name;
}

const main::ExtensionOption* ExtensionManager::getExtensionOption(
    std::string name) const {
  common::StringUtils::toLower(name);
  return extensionOptions.contains(name) ? &extensionOptions.at(name) : nullptr;
}

void ExtensionManager::RegisterLoadedExtension(const std::string& name,
                                               void* handle, InitFunc init) {
  if (!handle || !init) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Cannot register an extension with a null handle or init function");
  }
  std::lock_guard<std::mutex> lock(loaded_extensions_mutex_);
  loaded_extensions_[NormalizeExtensionName(name)] = {handle, init};
}

void* ExtensionManager::GetLoadedExtensionHandle(const std::string& name) {
  std::lock_guard<std::mutex> lock(loaded_extensions_mutex_);
  auto iter = loaded_extensions_.find(NormalizeExtensionName(name));
  return iter == loaded_extensions_.end() ? nullptr : iter->second.handle;
}

void ExtensionManager::InitLoadedExtensions() {
  std::vector<InitFunc> init_functions;
  {
    std::lock_guard<std::mutex> lock(loaded_extensions_mutex_);
    init_functions.reserve(loaded_extensions_.size());
    for (const auto& [_, extension] : loaded_extensions_) {
      init_functions.push_back(extension.init);
    }
  }
  for (auto init : init_functions) {
    init();
  }
}

}  // namespace extension
}  // namespace neug
