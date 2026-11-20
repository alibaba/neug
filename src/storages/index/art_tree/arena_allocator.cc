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

#include "neug/storages/index/art_tree/arena_allocator.h"
namespace neug::art {
ARTArenaChunk::ARTArenaChunk(idx_t size)
    : current_position(0), maximum_size(size), prev(nullptr) {
  D_ASSERT(size > 0);
  data = std::make_unique<uint8_t[]>(size);
}
ARTArenaChunk::~ARTArenaChunk() {
  if (next) {
    auto current_next = std::move(next);
    while (current_next) {
      current_next = std::move(current_next->next);
    }
  }
}

void ARTArenaAllocator::AllocateNewBlock(idx_t min_size) {
  idx_t capacity;
  // start off with either (1) initial capacity (if we have no block) or (2)
  // capacity of the previous block
  if (!head) {
    capacity = initial_capacity;
  } else {
    capacity = head->maximum_size;
  }
  // capacity of the previous block can be bigger than the max capacity if we
  // allocate len > max capacity for new blocks - try to set it back to the max
  // capacity
  if (capacity > ARENA_ALLOCATOR_MAX_CAPACITY) {
    capacity = ARENA_ALLOCATOR_MAX_CAPACITY;
  }
  // if we are below the max capacity - double the size of the block
  if (capacity < ARENA_ALLOCATOR_MAX_CAPACITY) {
    capacity *= 2;
  }
  // we double the size until we can fit `len`
  // this is generally only relevant if len is very large
  while (capacity < min_size) {
    capacity *= 2;
  }
  auto new_chunk = std::make_unique<ARTArenaChunk>(capacity);
  if (head) {
    head->prev = new_chunk.get();
    new_chunk->next = std::move(head);
  } else {
    tail = new_chunk.get();
  }
  head = std::move(new_chunk);
  allocated_size += capacity;
}

}  // namespace neug::art
