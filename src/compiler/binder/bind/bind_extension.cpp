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

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/bound_extension_statement.h"
#include "neug/compiler/common/file_system/local_file_system.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/extension/extension.h"
#include "neug/compiler/parser/extension_statement.h"
#include "neug/utils/exception/exception.h"

using namespace neug::parser;

namespace neug {
namespace binder {

static void bindInstallExtension(const ExtensionAuxInfo& auxInfo) {
  if (!ExtensionUtils::isOfficialExtension(auxInfo.path)) {
    THROW_BINDER_EXCEPTION(common::stringFormat(
        "{} is not an official extension.\nNon-official extensions "
        "can be installed directly by: `LOAD EXTENSION [EXTENSION_PATH]`.",
        auxInfo.path));
  }
}

static void bindLoadExtension(const ExtensionAuxInfo& auxInfo) {
  if (ExtensionUtils::isOfficialExtension(auxInfo.path)) {
    return;
  }
  auto localFileSystem = common::LocalFileSystem("");
  if (!localFileSystem.fileOrPathExists(auxInfo.path,
                                        nullptr /* clientContext */)) {
    THROW_BINDER_EXCEPTION(common::stringFormat(
        "The extension {} is neither an official extension, nor does "
        "the extension path: '{}' exists.",
        auxInfo.path, auxInfo.path));
  }
}

std::unique_ptr<BoundStatement> Binder::bindExtension(
    const Statement& statement) {
  auto extensionStatement = statement.constPtrCast<ExtensionStatement>();
  auto auxInfo = extensionStatement->getAuxInfo();
  switch (auxInfo->action) {
  case ExtensionAction::INSTALL:
    bindInstallExtension(*auxInfo);
    break;
  case ExtensionAction::LOAD:
    bindLoadExtension(*auxInfo);
    break;
  case ExtensionAction::UNINSTALL:
    break;
  default:
    NEUG_UNREACHABLE;
  }
  if (ExtensionUtils::isOfficialExtension(auxInfo->path)) {
    common::StringUtils::toLower(auxInfo->path);
  }
  return std::make_unique<BoundExtensionStatement>(std::move(auxInfo));
}

}  // namespace binder
}  // namespace neug
