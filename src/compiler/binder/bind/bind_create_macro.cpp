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
#include "neug/compiler/binder/bound_create_macro.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/common/string_format.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/create_macro.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;
using namespace neug::parser;

namespace neug {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCreateMacro(
    const Statement& statement) const {
  auto& createMacro = neug_dynamic_cast<const CreateMacro&>(statement);
  auto macroName = createMacro.getMacroName();
  StringUtils::toUpper(macroName);
  if (clientContext->getCatalog()->containsMacro(
          clientContext->getTransaction(), macroName)) {
    THROW_BINDER_EXCEPTION(stringFormat("Macro {} already exists.", macroName));
  }
  parser::default_macro_args defaultArgs;
  for (auto& defaultArg : createMacro.getDefaultArgs()) {
    defaultArgs.emplace_back(defaultArg.first, defaultArg.second->copy());
  }
  auto scalarMacro = std::make_unique<function::ScalarMacroFunction>(
      createMacro.getMacroExpression()->copy(), createMacro.getPositionalArgs(),
      std::move(defaultArgs));
  return std::make_unique<BoundCreateMacro>(std::move(macroName),
                                            std::move(scalarMacro));
}

}  // namespace binder
}  // namespace neug
