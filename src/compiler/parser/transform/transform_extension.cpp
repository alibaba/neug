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

#include "neug/compiler/extension/extension.h"
#include "neug/compiler/parser/extension_statement.h"
#include "neug/compiler/parser/transformer.h"

using namespace neug::common;
using namespace neug::extension;

namespace neug {
namespace parser {

std::unique_ptr<Statement> Transformer::transformExtension(
    CypherParser::NEUG_ExtensionContext& ctx) {
  if (ctx.nEUG_InstallExtension()) {
    auto extensionRepo =
        ctx.nEUG_InstallExtension()->StringLiteral()
            ? transformStringLiteral(
                  *ctx.nEUG_InstallExtension()->StringLiteral())
            : ExtensionUtils::OFFICIAL_EXTENSION_REPO;
    auto installExtensionAuxInfo = std::make_unique<InstallExtensionAuxInfo>(
        std::move(extensionRepo),
        transformVariable(*ctx.nEUG_InstallExtension()->oC_Variable()));
    return std::make_unique<ExtensionStatement>(
        std::move(installExtensionAuxInfo));
  } else if (ctx.nEUG_LoadExtension()) {
    auto path =
        ctx.nEUG_LoadExtension()->StringLiteral()
            ? transformStringLiteral(*ctx.nEUG_LoadExtension()->StringLiteral())
            : transformVariable(*ctx.nEUG_LoadExtension()->oC_Variable());
    auto auxInfo = std::make_unique<ExtensionAuxInfo>(ExtensionAction::LOAD,
                                                      std::move(path));
    return std::make_unique<ExtensionStatement>(std::move(auxInfo));
  } else if (ctx.nEUG_UninstallExtension()) {
    auto name =
        ctx.nEUG_UninstallExtension()->StringLiteral()
            ? transformStringLiteral(
                  *ctx.nEUG_UninstallExtension()->StringLiteral())
            : transformVariable(*ctx.nEUG_UninstallExtension()->oC_Variable());
    auto auxInfo = std::make_unique<ExtensionAuxInfo>(
        ExtensionAction::UNINSTALL, std::move(name));
    return std::make_unique<ExtensionStatement>(std::move(auxInfo));
  }
  THROW_PARSER_EXCEPTION("Unsupported EXTENSION statement");
}

}  // namespace parser
}  // namespace neug
