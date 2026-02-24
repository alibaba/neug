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

#include "neug/compiler/common/constants.h"
#include "neug/compiler/common/serializer/serializer.h"
#include "parsed_expression.h"

namespace neug {
namespace parser {

class ParsedPropertyExpression : public ParsedExpression {
  static constexpr common::ExpressionType expressionType_ =
      common::ExpressionType::PROPERTY;

 public:
  ParsedPropertyExpression(std::string propertyName,
                           std::unique_ptr<ParsedExpression> child,
                           std::string raw)
      : ParsedExpression{expressionType_, std::move(child), std::move(raw)},
        propertyName{std::move(propertyName)} {}

  ParsedPropertyExpression(std::string alias, std::string rawName,
                           parsed_expr_vector children,
                           std::string propertyName)
      : ParsedExpression{expressionType_, std::move(alias), std::move(rawName),
                         std::move(children)},
        propertyName{std::move(propertyName)} {}

  explicit ParsedPropertyExpression(std::string propertyName)
      : ParsedExpression{expressionType_},
        propertyName{std::move(propertyName)} {}

  std::string getPropertyName() const { return propertyName; }
  bool isStar() const { return propertyName == common::InternalKeyword::STAR; }

  static std::unique_ptr<ParsedPropertyExpression> deserialize(
      common::Deserializer& deserializer);

  std::unique_ptr<ParsedExpression> copy() const override {
    return std::make_unique<ParsedPropertyExpression>(
        alias, rawName, copyVector(children), propertyName);
  }

 private:
  void serializeInternal(common::Serializer& serializer) const override {
    serializer.serializeValue(propertyName);
  }

 private:
  std::string propertyName;
};

}  // namespace parser
}  // namespace neug
