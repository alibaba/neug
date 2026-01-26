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

#include "bound_statement.h"
#include "neug/compiler/binder/query/normalized_single_query.h"

namespace neug {
namespace binder {

class BoundStatementVisitor {
 public:
  BoundStatementVisitor() = default;
  virtual ~BoundStatementVisitor() = default;

  void visit(const BoundStatement& statement);
  // Unsafe visitors are implemented on-demand. We may reuse safe visitor inside
  // unsafe visitor if no other class need to overwrite an unsafe visitor.
  void visitUnsafe(BoundStatement& statement);

  virtual void visitSingleQuery(const NormalizedSingleQuery& singleQuery);
  virtual void visitQueryPart(const NormalizedQueryPart& queryPart);

 protected:
  virtual void visitCreateSequence(const BoundStatement&) {}
  virtual void visitCreateTable(const BoundStatement&) {}
  virtual void visitDrop(const BoundStatement&) {}
  virtual void visitCreateType(const BoundStatement&) {}
  virtual void visitAlter(const BoundStatement&) {}
  virtual void visitCopyFrom(const BoundStatement&);
  virtual void visitCopyTo(const BoundStatement&);
  virtual void visitExportDatabase(const BoundStatement&) {}
  virtual void visitImportDatabase(const BoundStatement&) {}
  virtual void visitStandaloneCall(const BoundStatement&) {}
  virtual void visitExplain(const BoundStatement&);
  virtual void visitCreateMacro(const BoundStatement&) {}
  virtual void visitTransaction(const BoundStatement&) {}
  virtual void visitExtension(const BoundStatement&) {}

  virtual void visitRegularQuery(const BoundStatement& statement);
  virtual void visitRegularQueryUnsafe(BoundStatement& statement);
  virtual void visitSingleQueryUnsafe(NormalizedSingleQuery& singleQuery);
  virtual void visitQueryPartUnsafe(NormalizedQueryPart& queryPart);
  void visitReadingClause(const BoundReadingClause& readingClause);
  void visitReadingClauseUnsafe(BoundReadingClause& readingClause);
  virtual void visitMatch(const BoundReadingClause&) {}
  virtual void visitMatchUnsafe(BoundReadingClause&) {}
  virtual void visitUnwind(const BoundReadingClause& /*readingClause*/) {}
  virtual void visitTableFunctionCall(const BoundReadingClause&) {}
  virtual void visitLoadFrom(const BoundReadingClause& /*statement*/) {}
  void visitUpdatingClause(const BoundUpdatingClause& updatingClause);
  virtual void visitSet(const BoundUpdatingClause& /*updatingClause*/) {}
  virtual void visitDelete(const BoundUpdatingClause& /* updatingClause*/) {}
  virtual void visitInsert(const BoundUpdatingClause& /* updatingClause*/) {}
  virtual void visitMerge(const BoundUpdatingClause& /* updatingClause*/) {}

  virtual void visitProjectionBody(
      const BoundProjectionBody& /* projectionBody*/) {}
  virtual void visitProjectionBodyPredicate(
      const std::shared_ptr<Expression>& /* predicate*/) {}
  virtual void visitAttachDatabase(const BoundStatement&) {}
  virtual void visitDetachDatabase(const BoundStatement&) {}
  virtual void visitUseDatabase(const BoundStatement&) {}
  virtual void visitStandaloneCallFunction(const BoundStatement&) {}
};

}  // namespace binder
}  // namespace neug
