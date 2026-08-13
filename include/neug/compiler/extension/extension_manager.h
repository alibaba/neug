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

#include <atomic>
#include <memory>
#include <string>
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
 private:
  struct LoadedExtension;

 public:
  using InitFunc = void (*)();

  enum class LoadState { LOADING, LOADED, FAILED };

  struct LoadTicket {
    bool owns_load;

   private:
    friend class ExtensionManager;
    std::shared_ptr<LoadedExtension> extension;

    LoadTicket(bool owns_load, std::shared_ptr<LoadedExtension> extension)
        : owns_load(owns_load), extension(std::move(extension)) {}
  };

  const main::ExtensionOption* getExtensionOption(std::string name) const;

  static LoadTicket AcquireLoad(const std::string& name);
  static void CompleteLoad(const LoadTicket& ticket, void* handle,
                           InitFunc init);
  static void FailLoad(const LoadTicket& ticket);
  static void ReplayLoadedExtensions();

 private:
  struct LoadedExtension {
    std::atomic<LoadState> state{LoadState::LOADING};
    void* handle = nullptr;
    InitFunc init = nullptr;
  };

  using LoadedExtensionMap =
      std::unordered_map<std::string, std::shared_ptr<LoadedExtension>>;

  static std::string NormalizeExtensionName(std::string name);

  static std::atomic<const LoadedExtensionMap*> loaded_extensions_;

  std::unordered_map<std::string, main::ExtensionOption> extensionOptions;
};

}  // namespace extension
}  // namespace neug
