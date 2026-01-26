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

#include "neug/compiler/common/in_mem_overflow_buffer.h"

#include <bit>
#include "neug/compiler/common/system_config.h"
#include "neug/compiler/storage/buffer_manager/memory_manager.h"

using namespace neug::storage;

namespace neug {
namespace common {

BufferBlock::BufferBlock(std::unique_ptr<storage::MemoryBuffer> block)
    : currentOffset{0}, block{std::move(block)} {}

BufferBlock::~BufferBlock() = default;

uint64_t BufferBlock::size() const { return block->getBuffer().size(); }

uint8_t* BufferBlock::data() const { return block->getBuffer().data(); }

uint8_t* InMemOverflowBuffer::allocateSpace(uint64_t size) {
  if (requireNewBlock(size)) {
    if (!blocks.empty() && currentBlock()->currentOffset == 0) {
      blocks.pop_back();
    }
    allocateNewBlock(size);
  }
  auto data = currentBlock()->data() + currentBlock()->currentOffset;
  currentBlock()->currentOffset += size;
  return data;
}

void InMemOverflowBuffer::resetBuffer() {
  if (!blocks.empty()) {
    auto lastBlock = std::move(blocks.back());
    blocks.clear();
    lastBlock->resetCurrentOffset();
    blocks.push_back(std::move(lastBlock));
  }
}

void InMemOverflowBuffer::allocateNewBlock(uint64_t size) {}

}  // namespace common
}  // namespace neug
