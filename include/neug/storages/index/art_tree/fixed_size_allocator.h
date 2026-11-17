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
#include <unordered_set>
#include "neug/storages/index/art_tree/fixed_size_buffer.h"
#include "neug/storages/index/art_tree/index_pointer.h"
#include "neug/storages/index/art_tree/memory_usage.h"
namespace neug::art {
struct ARTBufferMetadata {
  idx_t buffer_id;
  idx_t segment_count;
  string address;
};
class ARTOptionalIdx {
 public:
  ARTOptionalIdx() = default;
  ARTOptionalIdx(idx_t value);
  ARTOptionalIdx& operator=(idx_t index);
  bool IsValid() const;
  idx_t GetIndex() const;
  void SetInvalid();

 private:
  std::optional<idx_t> value;
};
class FixedSizeAllocator {
 public:
  explicit FixedSizeAllocator(idx_t segment_size, Checkpoint& checkpoint,
                              MemoryLevel level = MemoryLevel::kInMemory);
  void Detach(Checkpoint& checkpoint, MemoryLevel level);
  idx_t GetCapacity() const;
  idx_t GetBufferCapacity() const;
  idx_t GetCapacity(const IndexPointer ptr) const;
  IndexPointer New();
  void Free(const IndexPointer ptr);
  SegmentHandle GetHandle(const IndexPointer ptr);
  template <class T>
  unsafe_optional_ptr<T> Get(const IndexPointer ptr) {
    return reinterpret_cast<T*>(Get(ptr));
  }
  data_ptr_t Get(const IndexPointer ptr);
  void Reset();
  idx_t GetSegmentSize() const;
  idx_t GetSegmentCount() const;
  idx_t GetUpperBoundBufferId() const;
  bool Empty() const;
  void RemoveEmptyBuffers();
  void VerifyBuffers() const;
  // Save each container through Checkpoint and return metadata for restoration.
  vector<ARTBufferMetadata> Save(Checkpoint& checkpoint);
  void Load(Checkpoint& checkpoint,
            const vector<ARTBufferMetadata>& buffer_metadata,
            idx_t expected_segment_count);
  // Report memory requested by the ART index, excluding additional overhead
  // introduced by the underlying allocator's size-rounding policy.
  MemoryUsage GetMemoryUsage() const;

 private:
  std::shared_ptr<CheckpointFileManager> runtime_allocator;
  MemoryLevel memory_level;
  idx_t segment_size;
  idx_t bitmask_count;
  idx_t bitmask_offset;
  idx_t available_segments_per_buffer;
  idx_t total_segment_count = 0;
  using BufferEntry = std::pair<const idx_t, std::unique_ptr<FixedSizeBuffer>>;
  std::unordered_map<idx_t, std::unique_ptr<FixedSizeBuffer>, std::hash<idx_t>,
                     std::equal_to<idx_t>, CountingAllocator<BufferEntry>>
      buffers;
  std::unordered_set<idx_t, std::hash<idx_t>, std::equal_to<idx_t>,
                     CountingAllocator<idx_t>>
      buffers_with_free_space;
  ARTOptionalIdx buffer_with_free_space;
  idx_t GetAvailableBufferId() const;
  void NextBufferWithFreeSpace();
};
}  // namespace neug::art
