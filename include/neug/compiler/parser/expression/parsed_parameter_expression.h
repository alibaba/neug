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
#include "parsed_expression.h"

namespace neug {
namespace parser {

class ParsedParameterExpression : public ParsedExpression {
 public:
  explicit ParsedParameterExpression(std::string parameterName, std::string raw)
      : ParsedExpression{common::ExpressionType::PARAMETER, std::move(raw)},
        parameterName{std::move(parameterName)} {}

  inline std::string getParameterName() const { return parameterName; }

  static std::unique_ptr<ParsedParameterExpression> deserialize(
      common::Deserializer&) {
    NEUG_UNREACHABLE;
  }

  inline std::unique_ptr<ParsedExpression> copy() const override {
    NEUG_UNREACHABLE;
  }

 private:
  void serializeInternal(common::Serializer&) const override {
    NEUG_UNREACHABLE;
  }

 private:
  std::string parameterName;
};

}  // namespace parser
}  // namespace neug
