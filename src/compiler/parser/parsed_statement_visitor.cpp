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

#include "neug/compiler/parser/parsed_statement_visitor.h"

#include "neug/compiler/common/cast.h"
#include "neug/compiler/parser/explain_statement.h"
#include "neug/compiler/parser/query/regular_query.h"

using namespace neug::common;

namespace neug {
namespace parser {

void StatementVisitor::visit(const Statement& statement) {
  switch (statement.getStatementType()) {
  case StatementType::QUERY: {
    visitQuery(statement);
  } break;
  case StatementType::CREATE_SEQUENCE: {
    visitCreateSequence(statement);
  } break;
  case StatementType::DROP: {
    visitDrop(statement);
  } break;
  case StatementType::CREATE_TABLE: {
    visitCreateTable(statement);
  } break;
  case StatementType::CREATE_TYPE: {
    visitCreateType(statement);
  } break;
  case StatementType::ALTER: {
    visitAlter(statement);
  } break;
  case StatementType::COPY_FROM: {
    visitCopyFrom(statement);
  } break;
  case StatementType::COPY_TO: {
    visitCopyTo(statement);
  } break;
  case StatementType::STANDALONE_CALL: {
    visitStandaloneCall(statement);
  } break;
  case StatementType::EXPLAIN: {
    visitExplain(statement);
  } break;
  case StatementType::CREATE_MACRO: {
    visitCreateMacro(statement);
  } break;
  case StatementType::TRANSACTION: {
    visitTransaction(statement);
  } break;
  case StatementType::EXTENSION: {
    visitExtension(statement);
  } break;
  case StatementType::EXPORT_DATABASE: {
    visitExportDatabase(statement);
  } break;
  case StatementType::IMPORT_DATABASE: {
    visitImportDatabase(statement);
  } break;
  case StatementType::ATTACH_DATABASE: {
    visitAttachDatabase(statement);
  } break;
  case StatementType::DETACH_DATABASE: {
    visitDetachDatabase(statement);
  } break;
  case StatementType::USE_DATABASE: {
    visitUseDatabase(statement);
  } break;
  case StatementType::STANDALONE_CALL_FUNCTION: {
    visitStandaloneCallFunction(statement);
  } break;
  default:
    NEUG_UNREACHABLE;
  }
}

void StatementVisitor::visitExplain(const Statement& statement) {
  auto& explainStatement =
      neug_dynamic_cast<const ExplainStatement&>(statement);
  visit(*explainStatement.getStatementToExplain());
}

void StatementVisitor::visitQuery(const Statement& statement) {
  auto& regularQuery = neug_dynamic_cast<const RegularQuery&>(statement);
  for (auto i = 0u; i < regularQuery.getNumSingleQueries(); ++i) {
    visitSingleQuery(regularQuery.getSingleQuery(i));
  }
}

void StatementVisitor::visitSingleQuery(const SingleQuery* singleQuery) {
  for (auto i = 0u; i < singleQuery->getNumQueryParts(); ++i) {
    visitQueryPart(singleQuery->getQueryPart(i));
  }
  for (auto i = 0u; i < singleQuery->getNumReadingClauses(); ++i) {
    visitReadingClause(singleQuery->getReadingClause(i));
  }
  for (auto i = 0u; i < singleQuery->getNumUpdatingClauses(); ++i) {
    visitUpdatingClause(singleQuery->getUpdatingClause(i));
  }
  if (singleQuery->hasReturnClause()) {
    visitReturnClause(singleQuery->getReturnClause());
  }
}

void StatementVisitor::visitQueryPart(const QueryPart* queryPart) {
  for (auto i = 0u; i < queryPart->getNumReadingClauses(); ++i) {
    visitReadingClause(queryPart->getReadingClause(i));
  }
  for (auto i = 0u; i < queryPart->getNumUpdatingClauses(); ++i) {
    visitUpdatingClause(queryPart->getUpdatingClause(i));
  }
  visitWithClause(queryPart->getWithClause());
}

void StatementVisitor::visitReadingClause(const ReadingClause* readingClause) {
  switch (readingClause->getClauseType()) {
  case ClauseType::MATCH: {
    visitMatch(readingClause);
  } break;
  case ClauseType::UNWIND: {
    visitUnwind(readingClause);
  } break;
  case ClauseType::IN_QUERY_CALL: {
    visitInQueryCall(readingClause);
  } break;
  case ClauseType::LOAD_FROM: {
    visitLoadFrom(readingClause);
  } break;
  default:
    NEUG_UNREACHABLE;
  }
}

void StatementVisitor::visitUpdatingClause(
    const UpdatingClause* updatingClause) {
  switch (updatingClause->getClauseType()) {
  case ClauseType::SET: {
    visitSet(updatingClause);
  } break;
  case ClauseType::DELETE_: {
    visitDelete(updatingClause);
  } break;
  case ClauseType::INSERT: {
    visitInsert(updatingClause);
  } break;
  case ClauseType::MERGE: {
    visitMerge(updatingClause);
  } break;
  default:
    NEUG_UNREACHABLE;
  }
}

}  // namespace parser
}  // namespace neug
