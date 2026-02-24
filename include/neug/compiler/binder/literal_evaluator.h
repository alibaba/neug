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

#include "neug/compiler/binder/expression_evaluator.h"
#include "neug/compiler/common/types/value/value.h"

namespace neug {
namespace evaluator {

class LiteralExpressionEvaluator : public ExpressionEvaluator {
  static constexpr EvaluatorType type_ = EvaluatorType::LITERAL;

 public:
  LiteralExpressionEvaluator(std::shared_ptr<binder::Expression> expression,
                             common::Value value)
      : ExpressionEvaluator{type_, std::move(expression),
                            true /* isResultFlat */},
        value{std::move(value)} {}

  void evaluate() override;

  void evaluate(common::sel_t count) override;

  bool selectInternal(common::SelectionVector& selVector) override;

  std::unique_ptr<ExpressionEvaluator> copy() override {
    return std::make_unique<LiteralExpressionEvaluator>(expression, value);
  }

 protected:
  void resolveResultVector(const processor::ResultSet& resultSet,
                           storage::MemoryManager* memoryManager) override;

 private:
  common::Value value;
  std::shared_ptr<common::DataChunkState> flatState;
  std::shared_ptr<common::DataChunkState> unFlatState;
};

}  // namespace evaluator
}  // namespace neug