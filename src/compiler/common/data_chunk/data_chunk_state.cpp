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

#include "neug/compiler/common/data_chunk/data_chunk_state.h"

#include "neug/compiler/common/data_chunk/sel_vector.h"
#include "neug/compiler/common/system_config.h"
#include "neug/compiler/common/types/types.h"

namespace neug {
namespace common {

DataChunkState::DataChunkState() : DataChunkState(DEFAULT_VECTOR_CAPACITY) {}

std::shared_ptr<DataChunkState> DataChunkState::getSingleValueDataChunkState() {
  auto state = std::make_shared<DataChunkState>(1);
  state->initOriginalAndSelectedSize(1);
  state->setToFlat();
  return state;
}

void DataChunkState::slice(offset_t offset) {
  // NOTE: this operation has performance penalty. Ideally we should directly
  // modify selVector instead of creating a new one.
  auto slicedSelVector =
      std::make_shared<SelectionVector>(DEFAULT_VECTOR_CAPACITY);
  for (auto i = 0u; i < selVector->getSelSize() - offset; i++) {
    slicedSelVector->getMutableBuffer()[i] = selVector->operator[](i + offset);
  }
  slicedSelVector->setToFiltered(selVector->getSelSize() - offset);
  selVector = std::move(slicedSelVector);
}

}  // namespace common
}  // namespace neug
