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

#include "neug/compiler/common/types/value/value.h"
#include "parsed_expression.h"

namespace neug {
namespace parser {

class ParsedLiteralExpression : public ParsedExpression {
  static constexpr common::ExpressionType expressionType =
      common::ExpressionType::LITERAL;

 public:
  ParsedLiteralExpression(common::Value value, std::string raw)
      : ParsedExpression{expressionType, std::move(raw)},
        value{std::move(value)} {}

  ParsedLiteralExpression(std::string alias, std::string rawName,
                          parsed_expr_vector children, common::Value value)
      : ParsedExpression{expressionType, std::move(alias), std::move(rawName),
                         std::move(children)},
        value{std::move(value)} {}

  explicit ParsedLiteralExpression(common::Value value)
      : ParsedExpression{expressionType}, value{std::move(value)} {}

  common::Value getValue() const { return value; }

  static std::unique_ptr<ParsedLiteralExpression> deserialize(
      common::Deserializer& deserializer) {
    return std::make_unique<ParsedLiteralExpression>(
        *common::Value::deserialize(deserializer));
  }

  std::unique_ptr<ParsedExpression> copy() const override {
    return std::make_unique<ParsedLiteralExpression>(
        alias, rawName, copyVector(children), value);
  }

 private:
  void serializeInternal(common::Serializer& serializer) const override {
    value.serialize(serializer);
  }

 private:
  common::Value value;
};

}  // namespace parser
}  // namespace neug
