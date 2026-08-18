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

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "neug/utils/result.h"

namespace neug {

struct ExtensionEntry {
  const char* name;
  const char* extensionName;
};

class ExtensionManager {
 private:
  struct LoadedExtension;

 public:
  using InitFunc = void (*)();

  struct LoadResult {
    std::string canonical_name;
    bool newly_loaded;
  };

  Status InstallExtension(const std::string& name,
                          const std::string& repository = {});
  result<LoadResult> LoadExtension(const std::string& name);
  Status UninstallExtension(const std::string& name);
  bool IsLoaded(const std::string& name) const;

 private:
  struct LoadedExtension {
    std::string library_path;
    void* handle = nullptr;
    InitFunc init = nullptr;
  };

  static std::string NormalizeExtensionName(std::string name);

  mutable std::mutex mutex_;
  std::unordered_map<std::string, LoadedExtension> loaded_extensions_;
};

}  // namespace neug
