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

#include <mutex>
#include <unordered_map>

#include "neug/compiler/main/client_context.h"
#include "neug/compiler/main/option_config.h"

namespace neug {
namespace extension {

struct ExtensionEntry {
  const char* name;
  const char* extensionName;
};

class ExtensionManager {
 public:
  using InitFunc = void (*)();

  const main::ExtensionOption* getExtensionOption(std::string name) const;

  static void RegisterLoadedExtension(const std::string& name, void* handle,
                                      InitFunc init);
  static void* GetLoadedExtensionHandle(const std::string& name);
  static void InitLoadedExtensions();

 private:
  struct LoadedExtension {
    void* handle;
    InitFunc init;
  };

  static std::string NormalizeExtensionName(std::string name);

  static std::mutex loaded_extensions_mutex_;
  static std::unordered_map<std::string, LoadedExtension> loaded_extensions_;

  std::unordered_map<std::string, main::ExtensionOption> extensionOptions;
};

}  // namespace extension
}  // namespace neug
