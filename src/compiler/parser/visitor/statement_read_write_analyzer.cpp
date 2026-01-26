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

#include "neug/compiler/parser/expression/parsed_expression_visitor.h"
#include "neug/compiler/parser/query/reading_clause/reading_clause.h"
#include "neug/compiler/parser/query/return_with_clause/with_clause.h"

namespace neug {
namespace parser {

void StatementReadWriteAnalyzer::visitReadingClause(
    const ReadingClause* readingClause) {
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
