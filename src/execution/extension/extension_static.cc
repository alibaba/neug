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

#include "neug/compiler/extension/extension_manager.h"
#include "neug/execution/extension/extension.h"

namespace neug {
namespace extension {

Status install_extension(const std::string&, const std::string&) {
  return Status(
      StatusCode::ERR_NOT_SUPPORTED,
      "INSTALL is unavailable in static extension mode. "
      "Extensions must be linked into the application at build time.");
}

Status uninstall_extension(const std::string&) {
  return Status(StatusCode::ERR_NOT_SUPPORTED,
                "UNINSTALL is unavailable in static extension mode. "
                "Extensions must be removed at build time.");
}

}  // namespace extension

result<ExtensionManager::LoadResult> ExtensionManager::LoadExtension(
    const std::string&) {
  RETURN_ERROR(Status(
      StatusCode::ERR_NOT_SUPPORTED,
      "LOAD is unavailable in static extension mode. "
      "Extensions must be linked and initialized at build/startup time."));
}

}  // namespace neug
