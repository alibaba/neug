#include "neug/compiler/common/in_mem_overflow_buffer.h"

#include <bit>
#include "neug/compiler/common/system_config.h"
#include "neug/compiler/storage/buffer_manager/memory_manager.h"

using namespace gs::storage;

namespace gs {
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
}  // namespace gs
