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

#include "neug/compiler/common/copy_constructors.h"
#include "parsed_expression.h"

namespace neug {
namespace parser {

struct ParsedCaseAlternative {
  std::unique_ptr<ParsedExpression> whenExpression;
  std::unique_ptr<ParsedExpression> thenExpression;

  ParsedCaseAlternative() = default;
  ParsedCaseAlternative(std::unique_ptr<ParsedExpression> whenExpression,
                        std::unique_ptr<ParsedExpression> thenExpression)
      : whenExpression{std::move(whenExpression)},
        thenExpression{std::move(thenExpression)} {}
  ParsedCaseAlternative(const ParsedCaseAlternative& other)
      : whenExpression{other.whenExpression->copy()},
        thenExpression{other.thenExpression->copy()} {}
  DEFAULT_BOTH_MOVE(ParsedCaseAlternative);

  void serialize(common::Serializer& serializer) const;
  static ParsedCaseAlternative deserialize(common::Deserializer& deserializer);
};

// Cypher supports 2 types of CaseExpression
// 1. CASE a.age
//    WHEN 20 THEN ...
// 2. CASE
//    WHEN a.age = 20 THEN ...
class ParsedCaseExpression final : public ParsedExpression {
  friend class ParsedExpressionChildrenVisitor;

 public:
  explicit ParsedCaseExpression(std::string raw)
      : ParsedExpression{common::ExpressionType::CASE_ELSE, std::move(raw)} {};

  ParsedCaseExpression(std::string alias, std::string rawName,
                       parsed_expr_vector children,
                       std::unique_ptr<ParsedExpression> caseExpression,
                       std::vector<ParsedCaseAlternative> caseAlternatives,
                       std::unique_ptr<ParsedExpression> elseExpression)
      : ParsedExpression{common::ExpressionType::CASE_ELSE, std::move(alias),
                         std::move(rawName), std::move(children)},
        caseExpression{std::move(caseExpression)},
        caseAlternatives{std::move(caseAlternatives)},
        elseExpression{std::move(elseExpression)} {}

  ParsedCaseExpression(std::unique_ptr<ParsedExpression> caseExpression,
                       std::vector<ParsedCaseAlternative> caseAlternatives,
                       std::unique_ptr<ParsedExpression> elseExpression)
      : ParsedExpression{common::ExpressionType::CASE_ELSE},
        caseExpression{std::move(caseExpression)},
        caseAlternatives{std::move(caseAlternatives)},
        elseExpression{std::move(elseExpression)} {}

  inline void setCaseExpression(std::unique_ptr<ParsedExpression> expression) {
    caseExpression = std::move(expression);
  }
  inline bool hasCaseExpression() const { return caseExpression != nullptr; }
  inline ParsedExpression* getCaseExpression() const {
    return caseExpression.get();
  }

  inline void addCaseAlternative(ParsedCaseAlternative caseAlternative) {
    caseAlternatives.push_back(std::move(caseAlternative));
  }
  inline uint32_t getNumCaseAlternative() const {
    return caseAlternatives.size();
  }
  inline ParsedCaseAlternative* getCaseAlternativeUnsafe(uint32_t idx) {
    return &caseAlternatives[idx];
  }
  inline const ParsedCaseAlternative* getCaseAlternative(uint32_t idx) const {
    return &caseAlternatives[idx];
  }

  inline void setElseExpression(std::unique_ptr<ParsedExpression> expression) {
    elseExpression = std::move(expression);
  }
  inline bool hasElseExpression() const { return elseExpression != nullptr; }
  inline ParsedExpression* getElseExpression() const {
    return elseExpression.get();
  }

  static std::unique_ptr<ParsedCaseExpression> deserialize(
      common::Deserializer& deserializer);

  std::unique_ptr<ParsedExpression> copy() const override;

 private:
  void serializeInternal(common::Serializer& serializer) const override;

 private:
  // Optional. If not specified, directly check next whenExpression
  std::unique_ptr<ParsedExpression> caseExpression;
  std::vector<ParsedCaseAlternative> caseAlternatives;
  // Optional. If not specified, evaluate as null
  std::unique_ptr<ParsedExpression> elseExpression;
};

}  // namespace parser
}  // namespace neug
