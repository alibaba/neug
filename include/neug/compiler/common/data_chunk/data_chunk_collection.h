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

#include "neug/compiler/common/data_chunk/data_chunk.h"

namespace neug {
namespace common {

// TODO(Guodong): Should rework this to use ColumnChunk.
class DataChunkCollection {
 public:
  explicit DataChunkCollection(storage::MemoryManager* mm);
  DELETE_COPY_DEFAULT_MOVE(DataChunkCollection);

  void append(DataChunk& chunk);
  inline const std::vector<common::DataChunk>& getChunks() const {
    return chunks;
  }
  inline std::vector<common::DataChunk>& getChunksUnsafe() { return chunks; }
  inline uint64_t getNumChunks() const { return chunks.size(); }
  inline const DataChunk& getChunk(uint64_t idx) const {
    NEUG_ASSERT(idx < chunks.size());
    return chunks[idx];
  }
  inline DataChunk& getChunkUnsafe(uint64_t idx) {
    NEUG_ASSERT(idx < chunks.size());
    return chunks[idx];
  }
  inline void merge(DataChunkCollection* other) {
    for (auto& chunk : other->chunks) {
      merge(std::move(chunk));
    }
  }
  void merge(DataChunk chunk);

 private:
  void allocateChunk(DataChunk& chunk);

  void initTypes(DataChunk& chunk);

 private:
  storage::MemoryManager* mm;
  std::vector<LogicalType> types;
  std::vector<DataChunk> chunks;
};

}  // namespace common
}  // namespace neug
