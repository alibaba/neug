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

#include "expression.h"

namespace neug {
namespace binder {

struct CaseAlternative {
  std::shared_ptr<Expression> whenExpression;
  std::shared_ptr<Expression> thenExpression;

  CaseAlternative(std::shared_ptr<Expression> whenExpression,
                  std::shared_ptr<Expression> thenExpression)
      : whenExpression{std::move(whenExpression)},
        thenExpression{std::move(thenExpression)} {}
};

class CaseExpression final : public Expression {
  static constexpr common::ExpressionType expressionType_ =
      common::ExpressionType::CASE_ELSE;

 public:
  CaseExpression(common::LogicalType dataType,
                 std::shared_ptr<Expression> elseExpression,
                 const std::string& name)
      : Expression{expressionType_, std::move(dataType), name},
        elseExpression{std::move(elseExpression)} {}

  void addCaseAlternative(std::shared_ptr<Expression> when,
                          std::shared_ptr<Expression> then) {
    caseAlternatives.push_back(
        make_unique<CaseAlternative>(std::move(when), std::move(then)));
  }
  common::idx_t getNumCaseAlternatives() const {
    return caseAlternatives.size();
  }
  CaseAlternative* getCaseAlternative(common::idx_t idx) const {
    return caseAlternatives[idx].get();
  }

  std::shared_ptr<Expression> getElseExpression() const {
    return elseExpression;
  }

  std::string toStringInternal() const override;

 private:
  std::vector<std::unique_ptr<CaseAlternative>> caseAlternatives;
  std::shared_ptr<Expression> elseExpression;
};

}  // namespace binder
}  // namespace neug
