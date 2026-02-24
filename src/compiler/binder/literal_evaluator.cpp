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

#include "neug/compiler/binder/literal_evaluator.h"

#include "neug/compiler/common/types/value/value.h"

using namespace neug::common;
using namespace neug::storage;
using namespace neug::main;

namespace neug {
namespace evaluator {

void LiteralExpressionEvaluator::evaluate() {}

void LiteralExpressionEvaluator::evaluate(sel_t count) {
  unFlatState->getSelVectorUnsafe().setSelSize(count);
  resultVector->setState(unFlatState);
  for (auto i = 1ul; i < count; i++) {
    resultVector->copyFromVectorData(i, resultVector.get(), 0);
  }
}

bool LiteralExpressionEvaluator::selectInternal(SelectionVector&) {
  NEUG_ASSERT(resultVector->dataType.getLogicalTypeID() == LogicalTypeID::BOOL);
  auto pos = resultVector->state->getSelVector()[0];
  NEUG_ASSERT(pos == 0u);
  return resultVector->getValue<bool>(pos) && (!resultVector->isNull(pos));
}

void LiteralExpressionEvaluator::resolveResultVector(
    const processor::ResultSet& /*resultSet*/, MemoryManager* memoryManager) {
  resultVector =
      std::make_shared<ValueVector>(value.getDataType().copy(), memoryManager);
  flatState = DataChunkState::getSingleValueDataChunkState();
  unFlatState = std::make_shared<DataChunkState>();
  resultVector->setState(flatState);
  if (value.isNull()) {
    resultVector->setNull(0 /* pos */, true);
  } else {
    resultVector->copyFromValue(resultVector->state->getSelVector()[0], value);
  }
}

}  // namespace evaluator
}  // namespace neug