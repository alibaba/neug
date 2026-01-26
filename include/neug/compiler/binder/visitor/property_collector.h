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

#include "neug/compiler/binder/bound_statement_visitor.h"

namespace neug {
namespace binder {

// Collect all property expressions for a given statement.
class PropertyCollector final : public BoundStatementVisitor {
 public:
  expression_vector getProperties() const;

  // Skip collecting node/rel properties if they are in WITH projection list.
  // See with_clause_projection_rewriter for more details.
  void visitSingleQuerySkipNodeRel(const NormalizedSingleQuery& singleQuery);

 private:
  void visitQueryPartSkipNodeRel(const NormalizedQueryPart& queryPart);

  void visitMatch(const BoundReadingClause& readingClause) override;
  void visitUnwind(const BoundReadingClause& readingClause) override;
  void visitLoadFrom(const BoundReadingClause& readingClause) override;
  void visitTableFunctionCall(const BoundReadingClause&) override;

  void visitSet(const BoundUpdatingClause& updatingClause) override;
  void visitDelete(const BoundUpdatingClause& updatingClause) override;
  void visitInsert(const BoundUpdatingClause& updatingClause) override;
  void visitMerge(const BoundUpdatingClause& updatingClause) override;

  void visitProjectionBodySkipNodeRel(
      const BoundProjectionBody& projectionBody);
  void visitProjectionBody(const BoundProjectionBody& projectionBody) override;
  void visitProjectionBodyPredicate(
      const std::shared_ptr<Expression>& predicate) override;

  void collectProperties(const std::shared_ptr<Expression>& expression);
  void collectPropertiesSkipNodeRel(
      const std::shared_ptr<Expression>& expression);

 private:
  expression_set properties;
};

}  // namespace binder
}  // namespace neug
