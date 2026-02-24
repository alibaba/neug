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

#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression_evaluator.h"
#include "neug/compiler/processor/result/result_set_descriptor.h"

namespace neug {
namespace processor {

class ExpressionMapper {
 public:
  ExpressionMapper() = default;
  explicit ExpressionMapper(const planner::Schema* schema) : schema{schema} {}
  ExpressionMapper(const planner::Schema* schema,
                   evaluator::ExpressionEvaluator* parent)
      : schema{schema}, parentEvaluator{parent} {}

  std::unique_ptr<evaluator::ExpressionEvaluator> getEvaluator(
      std::shared_ptr<binder::Expression> expression);
  std::unique_ptr<evaluator::ExpressionEvaluator> getConstantEvaluator(
      std::shared_ptr<binder::Expression> expression);

 private:
  static std::unique_ptr<evaluator::ExpressionEvaluator> getLiteralEvaluator(
      std::shared_ptr<binder::Expression> expression);

  static std::unique_ptr<evaluator::ExpressionEvaluator> getParameterEvaluator(
      std::shared_ptr<binder::Expression> expression);

  std::unique_ptr<evaluator::ExpressionEvaluator> getReferenceEvaluator(
      std::shared_ptr<binder::Expression> expression) const;

  static std::unique_ptr<evaluator::ExpressionEvaluator>
  getLambdaParamEvaluator(std::shared_ptr<binder::Expression> expression);

  std::unique_ptr<evaluator::ExpressionEvaluator> getCaseEvaluator(
      std::shared_ptr<binder::Expression> expression);

  std::unique_ptr<evaluator::ExpressionEvaluator> getFunctionEvaluator(
      std::shared_ptr<binder::Expression> expression);

  std::unique_ptr<evaluator::ExpressionEvaluator> getNodeEvaluator(
      std::shared_ptr<binder::Expression> expression);

  std::unique_ptr<evaluator::ExpressionEvaluator> getRelEvaluator(
      std::shared_ptr<binder::Expression> expression);

  std::unique_ptr<evaluator::ExpressionEvaluator> getPathEvaluator(
      std::shared_ptr<binder::Expression> expression);

  std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> getEvaluators(
      const binder::expression_vector& expressions);

 private:
  const planner::Schema* schema = nullptr;
  // TODO: comment
  evaluator::ExpressionEvaluator* parentEvaluator = nullptr;
};

}  // namespace processor
}  // namespace neug