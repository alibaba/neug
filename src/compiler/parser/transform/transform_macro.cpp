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

#include "neug/compiler/parser/create_macro.h"
#include "neug/compiler/parser/transformer.h"

namespace neug {
namespace parser {

std::vector<std::string> Transformer::transformPositionalArgs(
    CypherParser::NEUG_PositionalArgsContext& ctx) {
  std::vector<std::string> positionalArgs;
  for (auto& positionalArg : ctx.oC_SymbolicName()) {
    positionalArgs.push_back(transformSymbolicName(*positionalArg));
  }
  return positionalArgs;
}

std::unique_ptr<Statement> Transformer::transformCreateMacro(
    CypherParser::NEUG_CreateMacroContext& ctx) {
  auto macroName = transformFunctionName(*ctx.oC_FunctionName());
  auto macroExpression = transformExpression(*ctx.oC_Expression());
  std::vector<std::string> positionalArgs;
  if (ctx.nEUG_PositionalArgs()) {
    positionalArgs = transformPositionalArgs(*ctx.nEUG_PositionalArgs());
  }
  default_macro_args defaultArgs;
  for (auto& defaultArg : ctx.nEUG_DefaultArg()) {
    defaultArgs.emplace_back(
        transformSymbolicName(*defaultArg->oC_SymbolicName()),
        transformLiteral(*defaultArg->oC_Literal()));
  }
  return std::make_unique<CreateMacro>(
      std::move(macroName), std::move(macroExpression),
      std::move(positionalArgs), std::move(defaultArgs));
}

}  // namespace parser
}  // namespace neug
