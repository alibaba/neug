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

#include "neug/compiler/extension/extension_api.h"
#include <iostream>

namespace neug {
namespace extension {

std::unordered_map<std::string, ExtensionInfo> ExtensionAPI::loaded_extensions_;
std::mutex ExtensionAPI::extensions_mutex_;

void ExtensionAPI::registerExtension(const ExtensionInfo& info) {
  std::lock_guard<std::mutex> lock(extensions_mutex_);
  loaded_extensions_[info.name] = info;
}

const std::unordered_map<std::string, ExtensionInfo>& ExtensionAPI::getLoadedExtensions() {
  std::lock_guard<std::mutex> lock(extensions_mutex_);
  return loaded_extensions_;
}

}  // namespace extension
}  // namespace neug