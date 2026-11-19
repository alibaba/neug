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
#include "neug/storages/module_descriptor.h"
namespace neug {
class CheckpointObjectReader;
class CheckpointObjectWriter;
struct CheckpointObjectSlice;
}  // namespace neug

namespace neug::art {
struct ARTBufferMetadata {
  idx_t buffer_id;
  string address;
  // Offsets are relative to the object payload, after FileHeader.
  idx_t offset;
  idx_t length;
};
using ARTBufferMetadataMap = std::unordered_map<
    idx_t, ARTBufferMetadata, std::hash<idx_t>, std::equal_to<idx_t>,
    CountingAllocator<std::pair<const idx_t, ARTBufferMetadata>>>;
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
  // Save a single allocator and record its references in the target checkpoint.
  const ARTBufferMetadataMap& Save(Checkpoint& checkpoint,
                                   ModuleDescriptor& descriptor);
  void Detach(Checkpoint& checkpoint, MemoryLevel level);
  idx_t GetCapacity() const;
  idx_t GetBufferCapacity() const;
  idx_t GetCapacity(const IndexPointer ptr) const;
  IndexPointer New();
  void Free(const IndexPointer ptr);
  SegmentHandle GetHandle(const IndexPointer ptr);
  // Legacy pointer access marks the buffer dirty by default. For read-only
  // access use a const T with dirty=false, or a segment handle without marking.
  template <class T>
  unsafe_optional_ptr<T> Get(const IndexPointer ptr, const bool dirty = true) {
    return reinterpret_cast<T*>(Get(ptr, dirty));
  }
  data_ptr_t Get(const IndexPointer ptr, const bool dirty = true);
  void Reset();
  idx_t GetSegmentSize() const;
  idx_t GetSegmentCount() const;
  idx_t GetUpperBoundBufferId() const;
  bool Empty() const;
  void RemoveEmptyBuffers();
  void VerifyBuffers() const;
  // Keep this allocator alive and hold its write lock through writer.Commit().
  // Uncommitted buffers remain dirty for retry.
  void Save(CheckpointObjectWriter& writer);
  // Serialize object references after a successful writer.Commit().
  const ARTBufferMetadataMap& SaveMetadata(ModuleDescriptor& descriptor,
                                           const string& buffer_prefix);
  // Allocate buffers from checkpoint and read object slices through reader.
  // Allocators in the same tree can share the reader.
  void Load(Checkpoint& checkpoint, CheckpointObjectReader& reader,
            const ARTBufferMetadataMap& saved_metadata,
            idx_t expected_segment_count);
  // Report memory requested by the ART index, excluding additional overhead
  // introduced by the underlying allocator's size-rounding policy.
  MemoryUsage GetMemoryUsage() const;

 private:
  struct SaveCallback {
    // The shared writer commits after all allocators have appended their data.
    // Its final object address is available only after Commit() succeeds; this
    // callback then records the slice location and clears the buffer's dirty
    // flag.
    FixedSizeAllocator& allocator;
    idx_t buffer_id;

    void operator()(const string& address,
                    const CheckpointObjectSlice& slice) const;
  };
  std::shared_ptr<CheckpointFileManager> runtime_allocator;
  MemoryLevel memory_level;
  idx_t segment_size;
  idx_t bitmask_count;
  idx_t bitmask_offset;
  idx_t available_segments_per_buffer;
  idx_t total_segment_count = 0;
  ARTBufferMetadataMap buffer_metadata;
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
