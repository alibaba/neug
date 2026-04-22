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

#include "neug/compiler/parser/visitor/standalone_call_rewriter.h"

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/bound_standalone_call_function.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/expression/parsed_function_expression.h"
#include "neug/compiler/parser/standalone_call_function.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace parser {

std::string StandaloneCallRewriter::getRewriteQuery(
    const Statement& statement) {
  visit(statement);
  return rewriteQuery;
}

void StandaloneCallRewriter::visitStandaloneCallFunction(
    const Statement& statement) {
  auto& standaloneCallFunc = statement.constCast<StandaloneCallFunction>();
  auto funcName = standaloneCallFunc.getFunctionExpression()
                      ->constPtrCast<parser::ParsedFunctionExpression>()
                      ->getFunctionName();
  if (!context->getCatalog()->containsFunction(context->getTransaction(),
                                               funcName) &&
      !singleStatement) {
    THROW_PARSER_EXCEPTION(funcName +
                           " must be called in a query which "
                           "doesn't have other statements.");
  }
  binder::Binder binder{context};
  const auto boundStatement = binder.bind(standaloneCallFunc);
  auto& boundStandaloneCall =
      boundStatement->constCast<binder::BoundStandaloneCallFunction>();
  const auto func = boundStandaloneCall.getTableFunction()
                        .constPtrCast<function::TableFunction>();
  if (func->rewriteFunc) {
    rewriteQuery =
        func->rewriteFunc(*context, *boundStandaloneCall.getBindData());
  }
}

}  // namespace parser
}  // namespace neug
