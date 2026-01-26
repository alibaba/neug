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

#include "neug/compiler/common/enums/delete_type.h"
#include "neug/compiler/parser/expression/parsed_expression.h"
#include "updating_clause.h"

namespace neug {
namespace parser {

class DeleteClause final : public UpdatingClause {
 public:
  explicit DeleteClause(common::DeleteNodeType deleteType)
      : UpdatingClause{common::ClauseType::DELETE_}, deleteType{deleteType} {};

  void addExpression(std::unique_ptr<ParsedExpression> expression) {
    expressions.push_back(std::move(expression));
  }
  common::DeleteNodeType getDeleteClauseType() const { return deleteType; }
  uint32_t getNumExpressions() const { return expressions.size(); }
  ParsedExpression* getExpression(uint32_t idx) const {
    return expressions[idx].get();
  }

 private:
  common::DeleteNodeType deleteType;
  parsed_expr_vector expressions;
};

}  // namespace parser
}  // namespace neug
