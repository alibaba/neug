/** Copyright 2020 Alibaba Group Holding Limited.
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
 * This file is based on the DuckDB project
 * (https://github.com/duckdb/duckdb) Licensed under the MIT License.
 */

#pragma once
#include "neug/storages/index/art_tree/common.h"
namespace neug::art {
struct ARTArenaChunk {
  explicit ARTArenaChunk(idx_t size);
  ~ARTArenaChunk();
  // NeuG allocation backend replaces DuckDB AllocatedData/Allocator.
  std::unique_ptr<uint8_t[]> data;
  idx_t current_position;
  idx_t maximum_size;
  unsafe_unique_ptr<ARTArenaChunk> next;
  ARTArenaChunk* prev;
};
class ARTArenaAllocator {
 public:
  static constexpr idx_t ARENA_ALLOCATOR_INITIAL_CAPACITY = 2048;
  static constexpr idx_t ARENA_ALLOCATOR_MAX_CAPACITY = 1ULL << 24ULL;
  explicit ARTArenaAllocator(
      idx_t initial_capacity = ARENA_ALLOCATOR_INITIAL_CAPACITY)
      : initial_capacity(initial_capacity), head(nullptr), tail(nullptr) {}
  data_ptr_t Allocate(idx_t len) {
    D_ASSERT(!head || head->current_position <= head->maximum_size);
    if (!head || head->current_position + len > head->maximum_size)
      AllocateNewBlock(len);
    D_ASSERT(head->current_position + len <= head->maximum_size);
    auto result = head->data.get() + head->current_position;
    head->current_position += len;
    return result;
  }

 private:
  void AllocateNewBlock(idx_t min_size);
  idx_t initial_capacity;
  unsafe_unique_ptr<ARTArenaChunk> head;
  ARTArenaChunk* tail;
  idx_t allocated_size = 0;
};
}  // namespace neug::art
