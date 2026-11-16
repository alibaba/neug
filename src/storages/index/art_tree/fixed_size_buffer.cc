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

#include "neug/storages/index/art_tree/fixed_size_buffer.h"
#include "neug/storages/checkpoint_file_manager.h"
namespace neug::art {
SegmentHandle::SegmentHandle(FixedSizeBuffer& buffer, idx_t offset)
    : ptr(static_cast<data_ptr_t>(buffer.container->GetData()) +
          FixedSizeBuffer::BUFFER_HEADER_SIZE + offset) {}

constexpr idx_t FixedSizeBuffer::BASE[];
constexpr uint8_t FixedSizeBuffer::SHIFT[];
FixedSizeBuffer::FixedSizeBuffer(CheckpointFileManager& allocator,
                                 MemoryLevel level)
    : container(allocator.CreateRuntimeContainer(BUFFER_ALLOC_SIZE, level)) {
  if (!container || !container->GetData())
    throw InternalException("Cannot allocate ART buffer");
  std::memset(container->GetData(), 0, BUFFER_ALLOC_SIZE);
}
FixedSizeBuffer::FixedSizeBuffer(std::shared_ptr<IDataContainer> storage)
    : container(std::move(storage)) {
  if (!container || !container->GetData() ||
      container->GetDataSize() != BUFFER_ALLOC_SIZE)
    throw SerializationException("Invalid ART buffer size");
}
void FixedSizeBuffer::Detach(CheckpointFileManager& allocator,
                             MemoryLevel level) {
  // Keep each buffer independent, including when changing its memory backend.
  auto replacement = allocator.CreateRuntimeContainer(BUFFER_ALLOC_SIZE, level);
  if (!replacement || !replacement->GetData())
    throw InternalException("Cannot detach ART buffer");
  std::memcpy(replacement->GetData(), container->GetData(), BUFFER_ALLOC_SIZE);
  container = std::move(replacement);
}
uint32_t FixedSizeBuffer::AllocateSegment(const idx_t bitmask_count,
                                          const idx_t available_segments) {
  D_ASSERT(segment_count < available_segments);

  // Get a handle to the buffer's validity mask (offset 0).
  SegmentHandle handle(*this, 0);
  const auto bitmask_ptr = handle.GetPtr<validity_t>();

  ValidityMask mask(bitmask_ptr, available_segments);
  const auto data = mask.GetData();

  // fills up a buffer sequentially before searching for free bits
  if (mask.RowIsValid(segment_count)) {
    const auto offset = segment_count;
    mask.SetInvalid(offset);
    segment_count++;
    return UnsafeNumericCast<uint32_t>(offset);
  }

  for (idx_t entry_idx = 0; entry_idx < bitmask_count; entry_idx++) {
    // get an entry with free bits
    if (data[entry_idx] == 0) {
      continue;
    }

    // find the position of the free bit
    auto entry = data[entry_idx];
    idx_t first_valid_bit = 0;

    // this loop finds the position of the rightmost set bit in entry and stores
    // it in first_valid_bit
    for (idx_t i = 0; i < 6; i++) {
      // set the left half of the bits of this level to zero and test if the
      // entry is still not zero
      if (entry & BASE[i]) {
        // first valid bit is in the rightmost s[i] bits
        // permanently set the left half of the bits to zero
        entry &= BASE[i];
      } else {
        // first valid bit is in the leftmost s[i] bits
        // shift by s[i] for the next iteration and add s[i] to the position of
        // the rightmost set bit
        entry >>= SHIFT[i];
        first_valid_bit += SHIFT[i];
      }
    }
    D_ASSERT(entry);

    const auto offset = entry_idx * sizeof(validity_t) * 8 + first_valid_bit;
    D_ASSERT(mask.RowIsValid(offset));
    mask.SetInvalid(offset);
    segment_count++;
    return UnsafeNumericCast<uint32_t>(offset);
  }

  throw InternalException("Invalid bitmask for FixedSizeAllocator");
}

void FixedSizeBuffer::FreeSegment(const idx_t offset,
                                  const idx_t available_segments) {
  D_ASSERT(segment_count > 0);
  D_ASSERT(offset < available_segments);

  SegmentHandle handle(*this, 0);
  const auto bitmask_ptr = handle.GetPtr<validity_t>();

  ValidityMask mask(bitmask_ptr, available_segments);
  D_ASSERT(!mask.RowIsValid(offset));
  mask.SetValid(offset);

  segment_count--;
}

}  // namespace neug::art
