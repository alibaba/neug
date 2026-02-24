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

class VariableExpression final : public Expression {
  static constexpr common::ExpressionType expressionType_ =
      common::ExpressionType::VARIABLE;

 public:
  VariableExpression(common::LogicalType dataType, std::string uniqueName,
                     std::string variableName)
      : Expression{expressionType_, std::move(dataType), std::move(uniqueName)},
        variableName{std::move(variableName)},
        useName{false} {}

  std::string getVariableName() const { return variableName; }

  void cast(const common::LogicalType& type) override;

  std::string toStringInternal() const override { return variableName; }

  std::unique_ptr<Expression> copy() const override {
    return std::make_unique<VariableExpression>(dataType.copy(), uniqueName,
                                                variableName);
  }

  void setUseName(bool useName) { this->useName = useName; }

  bool getUseName() const { return useName; }

 private:
  std::string variableName;
  bool useName;
};

}  // namespace binder
}  // namespace neug
