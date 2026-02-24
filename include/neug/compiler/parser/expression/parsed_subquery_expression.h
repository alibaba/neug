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

#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/enums/subquery_type.h"
#include "neug/compiler/parser/query/graph_pattern/pattern_element.h"
#include "neug/compiler/parser/query/reading_clause/join_hint.h"
#include "parsed_expression.h"

namespace neug {
namespace parser {

class ParsedSubqueryExpression : public ParsedExpression {
  static constexpr common::ExpressionType type_ =
      common::ExpressionType::SUBQUERY;

 public:
  ParsedSubqueryExpression(common::SubqueryType subqueryType,
                           std::string rawName)
      : ParsedExpression{type_, std::move(rawName)},
        subqueryType{subqueryType} {}

  common::SubqueryType getSubqueryType() const { return subqueryType; }

  void addPatternElement(PatternElement element) {
    patternElements.push_back(std::move(element));
  }
  void setPatternElements(std::vector<PatternElement> elements) {
    patternElements = std::move(elements);
  }
  const std::vector<PatternElement>& getPatternElements() const {
    return patternElements;
  }

  void setWhereClause(std::unique_ptr<ParsedExpression> expression) {
    whereClause = std::move(expression);
  }
  bool hasWhereClause() const { return whereClause != nullptr; }
  const ParsedExpression* getWhereClause() const { return whereClause.get(); }

  void setHint(std::shared_ptr<JoinHintNode> root) {
    hintRoot = std::move(root);
  }
  bool hasHint() const { return hintRoot != nullptr; }
  std::shared_ptr<JoinHintNode> getHint() const { return hintRoot; }

  static std::unique_ptr<ParsedSubqueryExpression> deserialize(
      common::Deserializer&) {
    NEUG_UNREACHABLE;
  }

  std::unique_ptr<ParsedExpression> copy() const override { NEUG_UNREACHABLE; }

 private:
  void serializeInternal(common::Serializer&) const override {
    NEUG_UNREACHABLE;
  }

 private:
  common::SubqueryType subqueryType;
  std::vector<PatternElement> patternElements;
  std::unique_ptr<ParsedExpression> whereClause;
  std::shared_ptr<JoinHintNode> hintRoot = nullptr;
};

}  // namespace parser
}  // namespace neug
