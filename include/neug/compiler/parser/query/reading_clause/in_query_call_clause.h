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

#include "neug/compiler/parser/query/reading_clause/reading_clause.h"
#include "yield_variable.h"

namespace neug {
namespace parser {

class InQueryCallClause final : public ReadingClause {
  static constexpr common::ClauseType clauseType_ =
      common::ClauseType::IN_QUERY_CALL;

 public:
  InQueryCallClause(std::unique_ptr<ParsedExpression> functionExpression,
                    std::vector<YieldVariable> yieldClause)
      : ReadingClause{clauseType_},
        functionExpression{std::move(functionExpression)},
        yieldVariables{std::move(yieldClause)} {}

  const ParsedExpression* getFunctionExpression() const {
    return functionExpression.get();
  }

  const std::vector<YieldVariable>& getYieldVariables() const {
    return yieldVariables;
  }

 private:
  std::unique_ptr<ParsedExpression> functionExpression;
  std::vector<YieldVariable> yieldVariables;
};

}  // namespace parser
}  // namespace neug
