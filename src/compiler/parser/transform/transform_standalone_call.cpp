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

#include "neug/compiler/parser/standalone_call.h"
#include "neug/compiler/parser/standalone_call_function.h"
#include "neug/compiler/parser/transformer.h"

namespace neug {
namespace parser {

std::unique_ptr<Statement> Transformer::transformStandaloneCall(
    CypherParser::NEUG_StandaloneCallContext& ctx) {
  if (ctx.oC_FunctionInvocation()) {
    return std::make_unique<StandaloneCallFunction>(
        transformFunctionInvocation(*ctx.oC_FunctionInvocation()));
  } else {
    auto optionName = transformSymbolicName(*ctx.oC_SymbolicName());
    auto parameter = transformExpression(*ctx.oC_Expression());
    return std::make_unique<StandaloneCall>(std::move(optionName),
                                            std::move(parameter));
  }
}

}  // namespace parser
}  // namespace neug
