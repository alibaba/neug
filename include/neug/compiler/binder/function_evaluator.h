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
#include "neug/compiler/function/scalar_function.h"

namespace neug {
namespace evaluator {

class FunctionExpressionEvaluator : public ExpressionEvaluator {
  static constexpr EvaluatorType type_ = EvaluatorType::FUNCTION;

 public:
  FunctionExpressionEvaluator(
      std::shared_ptr<binder::Expression> expression,
      std::vector<std::unique_ptr<ExpressionEvaluator>> children);

  void evaluate() override;
  void evaluate(common::sel_t count) override;

  bool selectInternal(common::SelectionVector& selVector) override;

  std::unique_ptr<ExpressionEvaluator> copy() override {
    return std::make_unique<FunctionExpressionEvaluator>(expression,
                                                         copyVector(children));
  }

 protected:
  void resolveResultVector(const processor::ResultSet& resultSet,
                           storage::MemoryManager* memoryManager) override;

  void runExecFunc(void* dataPtr = nullptr);

 private:
  std::vector<std::shared_ptr<common::ValueVector>> parameters;
  std::unique_ptr<function::ScalarFunction> function;
  std::unique_ptr<function::FunctionBindData> bindData;
};

}  // namespace evaluator
}  // namespace neug