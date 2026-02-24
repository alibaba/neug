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

#include "neug/compiler/binder/expression_evaluator_utils.h"

#include "neug/compiler/binder/expression_mapper.h"
#include "neug/compiler/common/types/value/value.h"

using namespace neug::common;
using namespace neug::processor;

namespace neug {
namespace evaluator {

Value ExpressionEvaluatorUtils::evaluateConstantExpression(
    std::shared_ptr<binder::Expression> expression,
    main::ClientContext* clientContext) {
  auto exprMapper = ExpressionMapper();
  auto evaluator = exprMapper.getConstantEvaluator(expression);
  auto emptyResultSet = std::make_unique<ResultSet>(0);
  evaluator->init(*emptyResultSet, clientContext);
  evaluator->evaluate();
  auto& selVector = evaluator->resultVector->state->getSelVector();
  NEUG_ASSERT(selVector.getSelSize() == 1);
  return *evaluator->resultVector->getAsValue(selVector[0]);
}

}  // namespace evaluator
}  // namespace neug