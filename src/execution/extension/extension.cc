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

#include "neug/execution/extension/extension.h"
#include "neug/compiler/extension/extension_manager.h"

namespace neug {

Status ExtensionManager::InstallExtension(const std::string& name,
                                          const std::string& repository) {
  std::lock_guard<std::mutex> lock(mutex_);
  return extension::install_extension(NormalizeExtensionName(name), repository);
}

Status ExtensionManager::UninstallExtension(const std::string& name) {
  std::lock_guard<std::mutex> lock(mutex_);
  return extension::uninstall_extension(NormalizeExtensionName(name));
}

}  // namespace neug
