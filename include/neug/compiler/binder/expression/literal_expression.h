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
#include "neug/compiler/common/types/value/value.h"

namespace neug {
namespace binder {

class NEUG_API LiteralExpression final : public Expression {
 public:
  LiteralExpression(common::Value value, const std::string& uniqueName)
      : Expression{common::ExpressionType::LITERAL, value.getDataType().copy(),
                   uniqueName},
        value{std::move(value)} {}

  bool isNull() const { return value.isNull(); }

  void cast(const common::LogicalType& type) override;

  common::Value getValue() const { return value; }

  std::string toStringInternal() const override { return value.toString(); }

  std::unique_ptr<Expression> copy() const override {
    return std::make_unique<LiteralExpression>(value, uniqueName);
  }

 public:
  common::Value value;
};

}  // namespace binder
}  // namespace neug
