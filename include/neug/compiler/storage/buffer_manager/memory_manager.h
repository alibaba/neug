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

#include <cstdint>
#include <memory>
#include <mutex>
#include <stack>

#include <span>
#include "neug/compiler/common/system_config.h"
#include "neug/compiler/common/types/types.h"

namespace neug {

namespace common {
class VirtualFileSystem;
}

namespace storage {

class MemoryManager;
class FileHandle;
class BufferManager;
class ChunkedNodeGroup;

class MemoryBuffer {
  friend class Spiller;

 public:
  NEUG_API MemoryBuffer(MemoryManager* mm, common::page_idx_t blockIdx,
                        uint8_t* buffer,
                        uint64_t size = common::TEMP_PAGE_SIZE) {}
  NEUG_API ~MemoryBuffer() = default;
  DELETE_COPY_AND_MOVE(MemoryBuffer);

  std::span<uint8_t> getBuffer() const {
    NEUG_ASSERT(!evicted);
    return buffer;
  }
  uint8_t* getData() const { return getBuffer().data(); }

  MemoryManager* getMemoryManager() const { return mm; }

 private:
  void prepareLoadFromDisk();

  void setSpilledToDisk(uint64_t filePosition);

 private:
  std::span<uint8_t> buffer;
  uint64_t filePosition = UINT64_MAX;
  MemoryManager* mm;
  common::page_idx_t pageIdx;
  bool evicted;
};

/*
 * The Memory Manager (MM) is used for allocating/reclaiming intermediate memory
 * blocks. It can allocate a memory buffer of size PAGE_256KB from the buffer
 * manager backed by a BMFileHandle with temp in-mem file.
 *
 * The MemoryManager holds a BMFileHandle backed by
 * a temp in-mem file, and is responsible for allocating/reclaiming memory
 * buffers of its size class from the buffer manager. The MemoryManager keeps
 * track of free pages in the BMFileHandle, so that it can reuse those freed
 * pages without allocating new pages. The MemoryManager is thread-safe, so that
 * multiple threads can allocate/reclaim memory blocks with the same size class
 * at the same time.
 *
 * MM will return a MemoryBuffer to the caller, which is a wrapper of the
 * allocated memory block, and it will automatically call its allocator to
 * reclaim the memory block when it is destroyed.
 */
class NEUG_API MemoryManager {
  friend class MemoryBuffer;

 public:
  MemoryManager() = default;
  MemoryManager(BufferManager* bm, common::VirtualFileSystem* vfs) {}

  ~MemoryManager() = default;

  std::unique_ptr<MemoryBuffer> allocateBuffer(
      bool initializeToZero = false, uint64_t size = common::TEMP_PAGE_SIZE);
  common::page_offset_t getPageSize() const { return pageSize; }

  BufferManager* getBufferManager() const { return bm; }

 private:
  void freeBlock(common::page_idx_t pageIdx, std::span<uint8_t> buffer);
  std::span<uint8_t> mallocBuffer(bool initializeToZero, uint64_t size);

 private:
  FileHandle* fh;
  BufferManager* bm;
  common::page_offset_t pageSize;
  std::stack<common::page_idx_t> freePages;
  std::mutex allocatorLock;
};

}  // namespace storage
}  // namespace neug
