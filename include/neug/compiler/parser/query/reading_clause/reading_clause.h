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

#include "neug/compiler/common/cast.h"
#include "neug/compiler/common/enums/clause_type.h"
#include "neug/compiler/parser/expression/parsed_expression.h"

namespace neug {
namespace parser {

class ReadingClause {
 public:
  explicit ReadingClause(common::ClauseType clauseType)
      : clauseType{clauseType} {};
  virtual ~ReadingClause() = default;

  common::ClauseType getClauseType() const { return clauseType; }

  void setWherePredicate(std::unique_ptr<ParsedExpression> expression) {
    wherePredicate = std::move(expression);
  }
  bool hasWherePredicate() const { return wherePredicate != nullptr; }
  const ParsedExpression* getWherePredicate() const {
    return wherePredicate.get();
  }

  template <class TARGET>
  const TARGET& constCast() const {
    return common::neug_dynamic_cast<const TARGET&>(*this);
  }

 private:
  common::ClauseType clauseType;
  std::unique_ptr<ParsedExpression> wherePredicate;
};
}  // namespace parser
}  // namespace neug
