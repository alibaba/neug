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

#include <string>
#include "neug/compiler/extension/extension.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/utils/result.h"

namespace neug {
namespace extension {

Status install_extension(const std::string& extension_name);

Status load_extension(const std::string& extension_name);

Status uninstall_extension(const std::string& extension_name);

Status downloadExtensionFile(const ExtensionRepoInfo& repoInfo,
                             const std::string& localFilePath);

result<std::string> computeFileSHA256(const std::string& path);

Status verifyExtensionChecksum(const ExtensionRepoInfo& libRepoInfo,
                               const std::string& localLibPath,
                               bool& checksumChecked);

}  // namespace extension
}  // namespace neug
