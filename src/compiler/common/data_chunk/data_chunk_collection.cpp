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

#include "neug/compiler/common/data_chunk/data_chunk_collection.h"

#include "neug/compiler/common/system_config.h"

namespace neug {
namespace common {

DataChunkCollection::DataChunkCollection(storage::MemoryManager* mm) : mm{mm} {}

void DataChunkCollection::append(DataChunk& chunk) {
  auto numTuplesToAppend = chunk.state->getSelVector().getSelSize();
  auto numTuplesAppended = 0u;
  while (numTuplesAppended < numTuplesToAppend) {
    if (chunks.empty() || chunks.back().state->getSelVector().getSelSize() ==
                              DEFAULT_VECTOR_CAPACITY) {
      allocateChunk(chunk);
    }
    auto& chunkToAppend = chunks.back();
    auto numTuplesToCopy =
        std::min((uint64_t) numTuplesToAppend - numTuplesAppended,
                 DEFAULT_VECTOR_CAPACITY -
                     chunkToAppend.state->getSelVector().getSelSize());
    for (auto vectorIdx = 0u; vectorIdx < chunk.getNumValueVectors();
         vectorIdx++) {
      for (auto i = 0u; i < numTuplesToCopy; i++) {
        auto srcPos = chunk.state->getSelVector()[numTuplesAppended + i];
        auto dstPos = chunkToAppend.state->getSelVector().getSelSize() + i;
        chunkToAppend.getValueVectorMutable(vectorIdx).copyFromVectorData(
            dstPos, &chunk.getValueVector(vectorIdx), srcPos);
      }
    }
    chunkToAppend.state->getSelVectorUnsafe().incrementSelSize(numTuplesToCopy);
    numTuplesAppended += numTuplesToCopy;
  }
}

void DataChunkCollection::merge(DataChunk chunk) {
  if (chunks.empty()) {
    initTypes(chunk);
  }
  NEUG_ASSERT(chunk.getNumValueVectors() == types.size());
  for (auto vectorIdx = 0u; vectorIdx < chunk.getNumValueVectors();
       vectorIdx++) {
    NEUG_ASSERT(chunk.getValueVector(vectorIdx).dataType == types[vectorIdx]);
  }
  chunks.push_back(std::move(chunk));
}

void DataChunkCollection::initTypes(DataChunk& chunk) {
  types.clear();
  types.reserve(chunk.getNumValueVectors());
  for (auto vectorIdx = 0u; vectorIdx < chunk.getNumValueVectors();
       vectorIdx++) {
    types.push_back(chunk.getValueVector(vectorIdx).dataType.copy());
  }
}

void DataChunkCollection::allocateChunk(DataChunk& chunk) {
  if (chunks.empty()) {
    types.reserve(chunk.getNumValueVectors());
    for (auto vectorIdx = 0u; vectorIdx < chunk.getNumValueVectors();
         vectorIdx++) {
      types.push_back(chunk.getValueVector(vectorIdx).dataType.copy());
    }
  }
  DataChunk newChunk(types.size(), std::make_shared<DataChunkState>());
  for (auto i = 0u; i < types.size(); i++) {
    newChunk.insert(i, std::make_shared<ValueVector>(types[i].copy(), mm));
  }
  chunks.push_back(std::move(newChunk));
}

}  // namespace common
}  // namespace neug
