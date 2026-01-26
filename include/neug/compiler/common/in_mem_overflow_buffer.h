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

#include <iterator>
#include <memory>
#include <vector>

#include "neug/compiler/common/copy_constructors.h"
#include "neug/utils/api.h"

namespace neug {
namespace storage {
class MemoryBuffer;
class MemoryManager;
}  // namespace storage

namespace common {

struct NEUG_API BufferBlock {
 public:
  explicit BufferBlock(std::unique_ptr<storage::MemoryBuffer> block);
  ~BufferBlock();

  uint64_t size() const;
  uint8_t* data() const;

 public:
  uint64_t currentOffset;
  std::unique_ptr<storage::MemoryBuffer> block;

  void resetCurrentOffset() { currentOffset = 0; }
};

class InMemOverflowBuffer {
 public:
  explicit InMemOverflowBuffer(storage::MemoryManager* memoryManager)
      : memoryManager{memoryManager} {};

  DEFAULT_BOTH_MOVE(InMemOverflowBuffer);

  uint8_t* allocateSpace(uint64_t size);

  void merge(InMemOverflowBuffer& other) {
    move(begin(other.blocks), end(other.blocks), back_inserter(blocks));
    // We clear the other InMemOverflowBuffer's block because when it is
    // deconstructed, InMemOverflowBuffer's deconstructed tries to free these
    // pages by calling memoryManager->freeBlock, but it should not because this
    // InMemOverflowBuffer still needs them.
    other.blocks.clear();
  }

  // Releases all memory accumulated for string overflows so far and
  // re-initializes its state to an empty buffer. If there is a large string
  // that used point to any of these overflow buffers they will error.
  void resetBuffer();

  storage::MemoryManager* getMemoryManager() { return memoryManager; }

 private:
  bool requireNewBlock(uint64_t sizeToAllocate) {
    return blocks.empty() || (currentBlock()->currentOffset + sizeToAllocate) >
                                 currentBlock()->size();
  }

  void allocateNewBlock(uint64_t size);

  BufferBlock* currentBlock() { return blocks.back().get(); }

 private:
  std::vector<std::unique_ptr<BufferBlock>> blocks;
  storage::MemoryManager* memoryManager;
};

}  // namespace common
}  // namespace neug
