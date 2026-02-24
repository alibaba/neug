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

#include <memory>                                 // for shared_ptr
#include <string>                                 // for string
#include <unordered_map>                          // for unordered_map
#include "neug/storages/loader/loading_config.h"  // for LoadingConfig

namespace neug {
class IFragmentLoader;
class Schema;

/**
 * @brief LoaderFactory is a factory class to create IFragmentLoader.
 * Support Using dynamically built library as plugin.
 */
class LoaderFactory {
 public:
  using loader_initializer_t = std::shared_ptr<IFragmentLoader> (*)(
      const std::string& work_dir, const Schema& schema,
      const LoadingConfig& loading_config);

  static void Init();

  static void Finalize();

  static std::shared_ptr<IFragmentLoader> CreateFragmentLoader(
      const std::string& work_dir, const Schema& schema,
      const LoadingConfig& loading_config);

  static bool Register(const std::string& scheme_type,
                       const std::string& format_type,
                       loader_initializer_t initializer);

 private:
  static std::unordered_map<std::string, loader_initializer_t>&
  getKnownLoaders();
};
}  // namespace neug
