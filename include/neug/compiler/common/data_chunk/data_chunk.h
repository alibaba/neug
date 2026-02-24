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

#include <memory>
#include <vector>

#include "neug/compiler/common/copy_constructors.h"
#include "neug/compiler/common/data_chunk/data_chunk_state.h"
#include "neug/compiler/common/vector/value_vector.h"

namespace neug {
namespace common {

// A DataChunk represents tuples as a set of value vectors and a selector array.
// The data chunk represents a subset of a relation i.e., a set of tuples as
// lists of the same length. It is appended into DataChunks and passed as
// intermediate representations between operators. A data chunk further contains
// a DataChunkState, which keeps the data chunk's size, selector, and currIdx
// (used when flattening and implies the value vector only contains the elements
// at currIdx of each value vector).
class NEUG_API DataChunk {
 public:
  DataChunk() : DataChunk{0} {}
  explicit DataChunk(uint32_t numValueVectors)
      : DataChunk(numValueVectors, std::make_shared<DataChunkState>()){};

  DataChunk(uint32_t numValueVectors,
            const std::shared_ptr<DataChunkState>& state)
      : valueVectors(numValueVectors), state{state} {};
  DELETE_COPY_DEFAULT_MOVE(DataChunk);

  void insert(uint32_t pos, std::shared_ptr<ValueVector> valueVector);

  void resetAuxiliaryBuffer();

  inline uint32_t getNumValueVectors() const { return valueVectors.size(); }

  inline const ValueVector& getValueVector(uint64_t valueVectorPos) const {
    return *valueVectors[valueVectorPos];
  }
  inline ValueVector& getValueVectorMutable(uint64_t valueVectorPos) const {
    return *valueVectors[valueVectorPos];
  }

 public:
  std::vector<std::shared_ptr<ValueVector>> valueVectors;
  std::shared_ptr<DataChunkState> state;
};

}  // namespace common
}  // namespace neug
