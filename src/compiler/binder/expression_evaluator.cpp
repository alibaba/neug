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

#include "neug/compiler/binder/expression_evaluator.h"

#include "neug/utils/exception/exception.h"

using namespace neug::common;

namespace neug {
namespace evaluator {

void ExpressionEvaluator::init(const processor::ResultSet& resultSet,
                               main::ClientContext* clientContext) {
  localState.clientContext = clientContext;
  for (auto& child : children) {
    child->init(resultSet, clientContext);
  }
  resolveResultVector(resultSet, clientContext->getMemoryManager());
}

void ExpressionEvaluator::resolveResultStateFromChildren(
    const std::vector<ExpressionEvaluator*>& inputEvaluators) {
  if (resultVector->state != nullptr) {
    return;
  }
  for (auto& input : inputEvaluators) {
    if (!input->isResultFlat()) {
      isResultFlat_ = false;
      resultVector->setState(input->resultVector->state);
      return;
    }
  }
  // All children are flat.
  isResultFlat_ = true;
  // We need to leave capacity for multiple evaluations
  resultVector->setState(std::make_shared<DataChunkState>());
  resultVector->state->initOriginalAndSelectedSize(1);
  resultVector->state->setToFlat();
}

void ExpressionEvaluator::evaluate(common::sel_t) {
  // LCOV_EXCL_START
  THROW_RUNTIME_ERROR(
      stringFormat("Cannot evaluate expression {} with count. This should "
                   "never happen.",
                   expression->toString()));
  // LCOV_EXCL_STOP
}

bool ExpressionEvaluator::select(common::SelectionVector& selVector,
                                 bool shouldSetSelVectorToFiltered) {
  bool ret = selectInternal(selVector);
  if (shouldSetSelVectorToFiltered && selVector.isUnfiltered()) {
    selVector.setToFiltered();
  }
  return ret;
}

}  // namespace evaluator
}  // namespace neug