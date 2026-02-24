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
#include "neug/compiler/binder/bound_standalone_call_function.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/expression/parsed_function_expression.h"
#include "neug/compiler/parser/standalone_call_function.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;

namespace neug {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindStandaloneCallFunction(
    const parser::Statement& statement) {
  auto& callStatement = statement.constCast<parser::StandaloneCallFunction>();
  auto& funcExpr = callStatement.getFunctionExpression()
                       ->constCast<parser::ParsedFunctionExpression>();
  auto funcName = funcExpr.getFunctionName();
  auto entry = clientContext->getCatalog()->getFunctionEntry(
      clientContext->getTransaction(), funcName,
      clientContext->useInternalCatalogEntry());
  NEUG_ASSERT(entry);
  auto boundTableFunction =
      bindTableFunc(funcName, funcExpr, {} /* yieldVariables */);
  return std::make_unique<BoundStandaloneCallFunction>(
      std::move(boundTableFunction));
}

}  // namespace binder
}  // namespace neug
