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

#include "neug/compiler/common/serializer/deserializer.h"
#include "neug/compiler/common/serializer/serializer.h"
#include "parsed_expression.h"

namespace neug {
namespace parser {

class ParsedVariableExpression : public ParsedExpression {
 public:
  ParsedVariableExpression(std::string variableName, std::string raw)
      : ParsedExpression{common::ExpressionType::VARIABLE, std::move(raw)},
        variableName{std::move(variableName)} {}

  ParsedVariableExpression(std::string alias, std::string rawName,
                           parsed_expr_vector children,
                           std::string variableName)
      : ParsedExpression{common::ExpressionType::VARIABLE, std::move(alias),
                         std::move(rawName), std::move(children)},
        variableName{std::move(variableName)} {}

  explicit ParsedVariableExpression(std::string variableName)
      : ParsedExpression{common::ExpressionType::VARIABLE},
        variableName{std::move(variableName)} {}

  inline std::string getVariableName() const { return variableName; }

  static std::unique_ptr<ParsedVariableExpression> deserialize(
      common::Deserializer& deserializer);

  inline std::unique_ptr<ParsedExpression> copy() const override {
    return std::make_unique<ParsedVariableExpression>(
        alias, rawName, copyVector(children), variableName);
  }

 private:
  inline void serializeInternal(common::Serializer& serializer) const override {
    serializer.serializeValue(variableName);
  }

 private:
  std::string variableName;
};

}  // namespace parser
}  // namespace neug
