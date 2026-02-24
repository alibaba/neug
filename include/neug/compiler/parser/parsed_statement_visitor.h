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

#pragma once

#include "statement.h"

namespace neug {
namespace main {
class ClientContext;
}
namespace parser {

class SingleQuery;
class QueryPart;
class ReadingClause;
class UpdatingClause;
class WithClause;
class ReturnClause;

class StatementVisitor {
 public:
  StatementVisitor() = default;
  virtual ~StatementVisitor() = default;

  void visit(const Statement& statement);

 private:
  // LCOV_EXCL_START
  virtual void visitQuery(const Statement& statement);
  virtual void visitSingleQuery(const SingleQuery* singleQuery);
  virtual void visitQueryPart(const QueryPart* queryPart);
  virtual void visitReadingClause(const ReadingClause* readingClause);
  virtual void visitMatch(const ReadingClause* /*readingClause*/) {}
  virtual void visitUnwind(const ReadingClause* /*readingClause*/) {}
  virtual void visitInQueryCall(const ReadingClause* /*readingClause*/) {}
  virtual void visitLoadFrom(const ReadingClause* /*readingClause*/) {}
  virtual void visitUpdatingClause(const UpdatingClause* /*updatingClause*/);
  virtual void visitSet(const UpdatingClause* /*updatingClause*/) {}
  virtual void visitDelete(const UpdatingClause* /*updatingClause*/) {}
  virtual void visitInsert(const UpdatingClause* /*updatingClause*/) {}
  virtual void visitMerge(const UpdatingClause* /*updatingClause*/) {}
  virtual void visitWithClause(const WithClause* /*withClause*/) {}
  virtual void visitReturnClause(const ReturnClause* /*returnClause*/) {}

  virtual void visitCreateSequence(const Statement& /*statement*/) {}
  virtual void visitDrop(const Statement& /*statement*/) {}
  virtual void visitCreateTable(const Statement& /*statement*/) {}
  virtual void visitCreateType(const Statement& /*statement*/) {}
  virtual void visitAlter(const Statement& /*statement*/) {}
  virtual void visitCopyFrom(const Statement& /*statement*/) {}
  virtual void visitCopyTo(const Statement& /*statement*/) {}
  virtual void visitStandaloneCall(const Statement& /*statement*/) {}
  virtual void visitExplain(const Statement& /*statement*/);
  virtual void visitCreateMacro(const Statement& /*statement*/) {}
  virtual void visitTransaction(const Statement& /*statement*/) {}
  virtual void visitExtension(const Statement& /*statement*/) {}
  virtual void visitExportDatabase(const Statement& /*statement*/) {}
  virtual void visitImportDatabase(const Statement& /*statement*/) {}
  virtual void visitAttachDatabase(const Statement& /*statement*/) {}
  virtual void visitDetachDatabase(const Statement& /*statement*/) {}
  virtual void visitUseDatabase(const Statement& /*statement*/) {}
  virtual void visitStandaloneCallFunction(const Statement& /*statement*/) {}
  // LCOV_EXCL_STOP
};

}  // namespace parser
}  // namespace neug
