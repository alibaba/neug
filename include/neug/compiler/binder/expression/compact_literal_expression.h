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

#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "expression.h"
#include "neug/compiler/common/types/value/value.h"

namespace neug {
namespace binder {

struct CompactLiteralSegment {
  compiler_impl::Value value;
  uint64_t repeatCount;

  CompactLiteralSegment(compiler_impl::Value value, uint64_t repeatCount)
      : value{std::move(value)}, repeatCount{repeatCount} {}
};

class CompactLiteralExpression final : public Expression {
 public:
  CompactLiteralExpression(common::DataType dataType,
                           std::vector<CompactLiteralSegment> segments,
                           std::string uniqueName)
      : Expression{common::ExpressionType::LITERAL, std::move(dataType),
                   std::move(uniqueName)},
        segments_{std::move(segments)} {}

  void cast(const common::DataType& type) override;

  const std::vector<CompactLiteralSegment>& getSegments() const {
    return segments_;
  }

  uint64_t getElementCount() const;

  compiler_impl::Value materialize() const;

  std::string toStringInternal() const override;

  std::unique_ptr<Expression> copy() const override {
    return std::make_unique<CompactLiteralExpression>(dataType.copy(),
                                                      segments_, uniqueName);
  }

 private:
  std::vector<CompactLiteralSegment> segments_;
};

}  // namespace binder
}  // namespace neug
