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

#include "neug/compiler/extension/extension_installer.h"

#include <fstream>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <algorithm>
#include <openssl/sha.h>
#include "httplib.h"
#include "neug/compiler/common/file_system/virtual_file_system.h"
#include "neug/compiler/main/client_context.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/result.h"
#include "neug/compiler/common/string_utils.h"

namespace gs {
namespace extension {

void ExtensionInstaller::tryDownloadExtensionFile(
    const ExtensionRepoInfo& repoInfo, const std::string& localFilePath) {
  httplib::Client cli(repoInfo.hostURL.c_str());
  httplib::Headers headers = {
      {"User-Agent", common::stringFormat("gs/v{}", NEUG_EXTENSION_VERSION)}};
  auto res = cli.Get(repoInfo.hostPath.c_str(), headers);
  if (!res || res->status != 200) {
    if (res.error() == httplib::Error::Success) {
      // LCOV_EXCL_START
      THROW_IO_EXCEPTION(common::stringFormat(
          "HTTP Returns: {}, Failed to download extension: \"{}\" from {}.",
          res.value().status, info.name, repoInfo.repoURL));
      // LCOC_EXCL_STOP
    } else {
      THROW_IO_EXCEPTION(common::stringFormat(
          "Failed to download extension: {} at URL {} (ERROR: {})", info.name,
          repoInfo.repoURL, to_string(res.error())));
    }
  }

  auto vfs = context.getVFSUnsafe();
  auto fileInfo = vfs->openFile(
      localFilePath,
      common::FileOpenFlags(common::FileFlags::WRITE |
                            common::FileFlags::READ_ONLY |
                            common::FileFlags::CREATE_AND_TRUNCATE_IF_EXISTS));
  fileInfo->writeFile(reinterpret_cast<const uint8_t*>(res->body.c_str()),
                      res->body.size(), 0 /* offset */);
  fileInfo->syncFile();
}

void ExtensionInstaller::install() {
  installExtension();
  installDependencies();
}

void ExtensionInstaller::installExtension() {
  auto vfs = context.getVFSUnsafe();
  auto localExtensionDir = context.getExtensionDir();
  if (!vfs->fileOrPathExists(localExtensionDir, &context)) {
    vfs->createDir(localExtensionDir);
  }
  auto localDirForExtension =
      extension::ExtensionUtils::getLocalExtensionDir(&context, info.name);
  if (!vfs->fileOrPathExists(localDirForExtension)) {
    vfs->createDir(localDirForExtension);
  }
  auto localDirForSharedLib =
      extension::ExtensionUtils::getLocalPathForSharedLib(&context);
  if (!vfs->fileOrPathExists(localDirForSharedLib)) {
    vfs->createDir(localDirForSharedLib);
  }
  auto libFileRepoInfo =
      extension::ExtensionUtils::getExtensionLibRepoInfo(info.name, info.repo);
  auto localLibFilePath =
      extension::ExtensionUtils::getLocalPathForExtensionLib(&context,
                                                             info.name);
  tryDownloadExtensionFile(libFileRepoInfo, localLibFilePath);
}

void ExtensionInstaller::installDependencies() {
  auto extensionRepoInfo =
      ExtensionUtils::getExtensionInstallerRepoInfo(info.name, info.repo);
  httplib::Client cli(extensionRepoInfo.hostURL.c_str());
  httplib::Headers headers = {
      {"User-Agent", common::stringFormat("gs/v{}", NEUG_EXTENSION_VERSION)}};
  auto res = cli.Get(extensionRepoInfo.hostPath.c_str(), headers);
  if (!res || res->status != 200) {
    // The extension doesn't have an installer.
    return;
  }
  auto extensionInstallerPath =
      ExtensionUtils::getLocalPathForExtensionInstaller(&context, info.name);
  tryDownloadExtensionFile(extensionRepoInfo, extensionInstallerPath);
  auto libLoader =
      ExtensionLibLoader(info.name, extensionInstallerPath.c_str());
  auto install = libLoader.getInstallFunc();
  (*install)(info.repo, context);
}

}  // namespace extension
}  // namespace gs
