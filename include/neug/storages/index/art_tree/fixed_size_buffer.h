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
#include "neug/storages/container/i_container.h"
#include "neug/storages/index/art_tree/common.h"
namespace neug {
class CheckpointFileManager;
}
namespace neug::art {
class FixedSizeAllocator;
class FixedSizeBuffer {
  friend class FixedSizeAllocator;
  friend class SegmentHandle;

 public:
  static constexpr idx_t BASE[] = {
      0x00000000FFFFFFFF, 0x0000FFFF, 0x00FF, 0x0F, 0x3, 0x1};
  static constexpr uint8_t SHIFT[] = {32, 16, 8, 4, 2, 1};
  // Total allocation, reserved header, and space for the bitmap and segments.
  static constexpr idx_t BUFFER_ALLOC_SIZE = 256 * 1024;
  static constexpr idx_t BUFFER_HEADER_SIZE = sizeof(idx_t);
  static constexpr idx_t BUFFER_DATA_SIZE =
      BUFFER_ALLOC_SIZE - BUFFER_HEADER_SIZE;
  FixedSizeBuffer(CheckpointFileManager& allocator, MemoryLevel level);
  explicit FixedSizeBuffer(std::shared_ptr<IDataContainer> container);
  void Detach(CheckpointFileManager& allocator, MemoryLevel level);

 private:
  uint32_t AllocateSegment(idx_t bitmask_count, idx_t available_segments);
  void FreeSegment(idx_t offset, idx_t available_segments);
  std::shared_ptr<IDataContainer> container;
  idx_t segment_count = 0;
  bool dirty = true;
};
// IDataContainer remains resident; caller-held tree lock protects its lifetime.
class SegmentHandle {
 public:
  SegmentHandle(FixedSizeBuffer& buffer, idx_t offset);
  SegmentHandle(const SegmentHandle&) = delete;
  SegmentHandle& operator=(const SegmentHandle&) = delete;
  SegmentHandle(SegmentHandle&&) noexcept = default;
  SegmentHandle& operator=(SegmentHandle&&) noexcept = default;
  template <class T>
  const T& GetRef() const {
    return *reinterpret_cast<const T*>(ptr);
  }
  template <class T>
  T& GetRef() {
    return *reinterpret_cast<T*>(ptr);
  }
  template <class T = data_t>
  T* GetPtr() {
    return reinterpret_cast<T*>(ptr);
  }
  template <class T = data_t>
  const T* GetPtr() const {
    return reinterpret_cast<const T*>(ptr);
  }
  // Call under the caller's write lock whenever modifying through this handle,
  // including writes through a pointer retained across Save().
  void MarkModified() { buffer_ptr->dirty = true; }

 private:
  FixedSizeBuffer* buffer_ptr;
  data_ptr_t ptr;
};
}  // namespace neug::art
