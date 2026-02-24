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

#include <unordered_set>

#include "neug/compiler/common/data_chunk/data_chunk.h"
#include "neug/compiler/processor/data_pos.h"
#include "result_set_descriptor.h"

namespace neug {
namespace processor {

class ResultSet {
 public:
  ResultSet() : ResultSet(0) {}
  explicit ResultSet(common::idx_t numDataChunks)
      : multiplicity{1}, dataChunks(numDataChunks) {}
  ResultSet(ResultSetDescriptor* resultSetDescriptor,
            storage::MemoryManager* memoryManager);

  void insert(common::idx_t pos, std::shared_ptr<common::DataChunk> dataChunk) {
    NEUG_ASSERT(dataChunks.size() > pos);
    dataChunks[pos] = std::move(dataChunk);
  }

  std::shared_ptr<common::DataChunk> getDataChunk(
      data_chunk_pos_t dataChunkPos) {
    return dataChunks[dataChunkPos];
  }
  std::shared_ptr<common::ValueVector> getValueVector(
      const DataPos& dataPos) const {
    return dataChunks[dataPos.dataChunkPos]
        ->valueVectors[dataPos.valueVectorPos];
  }

  // Our projection does NOT explicitly remove dataChunk from resultSet.
  // Therefore, caller should always provide a set of positions when reading
  // from multiple dataChunks.
  uint64_t getNumTuples(
      const std::unordered_set<uint32_t>& dataChunksPosInScope) {
    return getNumTuplesWithoutMultiplicity(dataChunksPosInScope) * multiplicity;
  }

  uint64_t getNumTuplesWithoutMultiplicity(
      const std::unordered_set<uint32_t>& dataChunksPosInScope);

 public:
  uint64_t multiplicity;
  std::vector<std::shared_ptr<common::DataChunk>> dataChunks;
};

}  // namespace processor
}  // namespace neug
