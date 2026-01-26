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
#include "neug/compiler/binder/query/reading_clause/bound_table_function_call.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/expression/parsed_function_expression.h"
#include "neug/compiler/parser/query/reading_clause/in_query_call_clause.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;
using namespace neug::catalog;
using namespace neug::parser;
using namespace neug::function;
using namespace neug::catalog;

namespace neug {
namespace binder {

std::unique_ptr<BoundReadingClause> Binder::bindInQueryCall(
    const ReadingClause& readingClause) {
  auto& call = readingClause.constCast<InQueryCallClause>();
  auto expr = call.getFunctionExpression();
  auto functionExpr = expr->constPtrCast<ParsedFunctionExpression>();
  auto functionName = functionExpr->getFunctionName();
  std::unique_ptr<BoundReadingClause> boundReadingClause;
  auto entry = clientContext->getCatalog()->getFunctionEntry(
      clientContext->getTransaction(), functionName);
  switch (entry->getType()) {
  case CatalogEntryType::TABLE_FUNCTION_ENTRY: {
    auto boundTableFunction =
        bindTableFunc(functionName, *functionExpr, call.getYieldVariables());
    boundReadingClause =
        std::make_unique<BoundTableFunctionCall>(std::move(boundTableFunction));
  } break;
  default:
    THROW_BINDER_EXCEPTION(
        stringFormat("{} is not a table or algorithm function.", functionName));
  }
  if (call.hasWherePredicate()) {
    auto wherePredicate = bindWhereExpression(*call.getWherePredicate());
    boundReadingClause->setPredicate(std::move(wherePredicate));
  }
  return boundReadingClause;
}

}  // namespace binder
}  // namespace neug
