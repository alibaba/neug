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

#include "neug/compiler/parser/visitor/statement_read_write_analyzer.h"

#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/catalog/catalog_entry/function_catalog_entry.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/expression/parsed_expression_visitor.h"
#include "neug/compiler/parser/expression/parsed_function_expression.h"
#include "neug/compiler/parser/query/reading_clause/in_query_call_clause.h"
#include "neug/compiler/parser/query/reading_clause/reading_clause.h"
#include "neug/compiler/parser/query/return_with_clause/with_clause.h"
#include "neug/compiler/parser/standalone_call_function.h"

namespace neug {
namespace parser {

namespace {

bool isCallFunctionReadOnly(main::ClientContext* context,
                            const ParsedExpression* functionExpression) {
  if (context == nullptr || functionExpression == nullptr) {
    return false;
  }
  if (functionExpression->getExpressionType() !=
      common::ExpressionType::FUNCTION) {
    return false;
  }
  auto funcName = functionExpression->constCast<ParsedFunctionExpression>()
                      .getFunctionName();
  auto* catalog = context->getCatalog();
  auto* transaction = context->getTransaction();
  if (transaction == nullptr ||
      !catalog->containsFunction(transaction, funcName)) {
    // Unknown CALL — treat as non-read-only to be safe.
    return false;
  }
  auto* entry = catalog->getFunctionEntry(transaction, funcName);
  if (entry->getType() != catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY &&
      entry->getType() !=
          catalog::CatalogEntryType::STANDALONE_TABLE_FUNCTION_ENTRY) {
    // Non-table callables are not classified here; be conservative.
    return false;
  }
  auto& funcSet =
      entry->constPtrCast<catalog::FunctionCatalogEntry>()->getFunctionSet();
  if (funcSet.empty()) {
    return false;
  }
  // Only treat as read-only when every overload is read-only.
  for (const auto& func : funcSet) {
    if (!func->isReadOnly) {
      return false;
    }
  }
  return true;
}

}  // namespace

void StatementReadWriteAnalyzer::visitStandaloneCallFunction(
    const Statement& statement) {
  auto& call = statement.constCast<StandaloneCallFunction>();
  if (!isCallFunctionReadOnly(context, call.getFunctionExpression())) {
    readOnly = false;
  }
}

void StatementReadWriteAnalyzer::visitInQueryCall(
    const ReadingClause* readingClause) {
  auto& call = readingClause->constCast<InQueryCallClause>();
  if (!isCallFunctionReadOnly(context, call.getFunctionExpression())) {
    readOnly = false;
  }
  if (readingClause->hasWherePredicate() &&
      !isExprReadOnly(readingClause->getWherePredicate())) {
    readOnly = false;
  }
}

void StatementReadWriteAnalyzer::visitReadingClause(
    const ReadingClause* readingClause) {
  // Dispatch in-query CALL so we can consult Function::isReadOnly.
  if (readingClause->getClauseType() == common::ClauseType::IN_QUERY_CALL) {
    visitInQueryCall(readingClause);
    return;
  }
  if (readingClause->hasWherePredicate()) {
    if (!isExprReadOnly(readingClause->getWherePredicate())) {
      readOnly = false;
    }
  }
}

void StatementReadWriteAnalyzer::visitWithClause(const WithClause* withClause) {
  for (auto& expr :
       withClause->getProjectionBody()->getProjectionExpressions()) {
    if (!isExprReadOnly(expr.get())) {
      readOnly = false;
      return;
    }
  }
}

void StatementReadWriteAnalyzer::visitReturnClause(
    const ReturnClause* returnClause) {
  for (auto& expr :
       returnClause->getProjectionBody()->getProjectionExpressions()) {
    if (!isExprReadOnly(expr.get())) {
      readOnly = false;
      return;
    }
  }
}

bool StatementReadWriteAnalyzer::isExprReadOnly(const ParsedExpression* expr) {
  auto analyzer = ReadWriteExprAnalyzer(context);
  analyzer.visit(expr);
  return analyzer.isReadOnly();
}

}  // namespace parser
}  // namespace neug
