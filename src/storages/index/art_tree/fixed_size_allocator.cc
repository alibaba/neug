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

#include "neug/storages/index/art_tree/fixed_size_allocator.h"

#include <charconv>
#include <string_view>

#include "neug/storages/checkpoint.h"
#include "neug/storages/container/anon_mmap_container.h"
#include "neug/utils/exception/exception.h"
namespace neug::art {
namespace {

idx_t ParseBufferID(std::string_view value) {
  idx_t result = 0;
  const auto* end = value.data() + value.size();
  const auto [ptr, error] = std::from_chars(value.data(), end, result);
  if (error != std::errc{} || ptr != end)
    throw SerializationException("Invalid ART metadata integer: " +
                                 string(value));
  return result;
}

}  // namespace

ARTOptionalIdx::ARTOptionalIdx(idx_t value) : value(value) {}
ARTOptionalIdx& ARTOptionalIdx::operator=(idx_t index) {
  value = index;
  return *this;
}
bool ARTOptionalIdx::IsValid() const { return value.has_value(); }
idx_t ARTOptionalIdx::GetIndex() const {
  D_ASSERT(IsValid());
  return *value;
}
void ARTOptionalIdx::SetInvalid() { value.reset(); }

idx_t FixedSizeAllocator::GetCapacity() const {
  return buffers.size() * available_segments_per_buffer;
}
idx_t FixedSizeAllocator::GetBufferCapacity() const {
  return available_segments_per_buffer;
}
idx_t FixedSizeAllocator::GetCapacity(const IndexPointer ptr) const {
  D_ASSERT(buffers.find(ptr.GetBufferId()) != buffers.end());
  return available_segments_per_buffer;
}
SegmentHandle FixedSizeAllocator::GetHandle(const IndexPointer ptr) {
  auto buffer_it = buffers.find(ptr.GetBufferId());
  D_ASSERT(buffer_it != buffers.end());
  D_ASSERT(ptr.GetOffset() < available_segments_per_buffer);
  auto offset = ptr.GetOffset() * segment_size + bitmask_offset;
  return SegmentHandle(*buffer_it->second, offset);
}
data_ptr_t FixedSizeAllocator::Get(const IndexPointer ptr, const bool dirty) {
  auto handle = GetHandle(ptr);
  if (dirty)
    handle.MarkModified();
  return handle.GetPtr();
}
idx_t FixedSizeAllocator::GetSegmentSize() const { return segment_size; }
idx_t FixedSizeAllocator::GetSegmentCount() const {
  return total_segment_count;
}
bool FixedSizeAllocator::Empty() const { return total_segment_count == 0; }

FixedSizeAllocator::FixedSizeAllocator(const idx_t segment_size,
                                       Checkpoint& checkpoint,
                                       MemoryLevel level)
    : runtime_allocator(checkpoint.runtime_allocator()),
      memory_level(level),
      segment_size(segment_size) {
  if (!segment_size)
    throw InternalException("Invalid ART segment size");
  if (segment_size > FixedSizeBuffer::BUFFER_DATA_SIZE - sizeof(validity_t)) {
    throw InternalException(
        "The maximum segment size of fixed-size allocators is " +
        to_string(FixedSizeBuffer::BUFFER_DATA_SIZE - sizeof(validity_t)));
  }

  // calculate how many segments fit into one buffer
  // (available_segments_per_buffer)

  idx_t bits_per_value = sizeof(validity_t) * 8;
  idx_t byte_count = 0;

  bitmask_count = 0;
  available_segments_per_buffer = 0;

  while (byte_count < FixedSizeBuffer::BUFFER_DATA_SIZE) {
    if (!bitmask_count ||
        (bitmask_count * bits_per_value) % available_segments_per_buffer == 0) {
      if (FixedSizeBuffer::BUFFER_DATA_SIZE - byte_count < sizeof(validity_t)) {
        break;
      }
      // we need to add another validity_t value to the bitmask, to allow
      // storing another bits_per_value segments on a buffer
      bitmask_count++;
      byte_count += sizeof(validity_t);
    }

    auto remaining_bytes = FixedSizeBuffer::BUFFER_DATA_SIZE - byte_count;
    auto remaining_segments =
        std::min(remaining_bytes / segment_size, bits_per_value);

    if (remaining_segments == 0) {
      break;
    }

    available_segments_per_buffer += remaining_segments;
    byte_count += remaining_segments * segment_size;
  }

  bitmask_offset = bitmask_count * sizeof(validity_t);
}

IndexPointer FixedSizeAllocator::New() {
  // No more free segments available.
  if (!buffer_with_free_space.IsValid()) {
    // Add a new buffer.
    auto buffer_id = GetAvailableBufferId();
    buffers[buffer_id] =
        std::make_unique<FixedSizeBuffer>(*runtime_allocator, memory_level);
    buffers_with_free_space.insert(buffer_id);
    buffer_with_free_space = buffer_id;

    // Set and initialize the bitmask of the new buffer.
    D_ASSERT(buffers.find(buffer_id) != buffers.end());
    auto& buffer = buffers.find(buffer_id)->second;

    // Get a handle to the buffer's validity mask (offset 0).
    SegmentHandle handle(*buffer, 0);
    const auto bitmask_ptr = handle.GetPtr<validity_t>();
    ValidityMask mask(bitmask_ptr, available_segments_per_buffer);
    mask.SetAllValid(available_segments_per_buffer);
  }

  // Extract an index pointer to a free segment.

  D_ASSERT(buffer_with_free_space.IsValid());
  auto buffer_id = buffer_with_free_space.GetIndex();

  D_ASSERT(buffers.find(buffer_id) != buffers.end());
  auto& buffer = buffers.find(buffer_id)->second;
  auto offset =
      buffer->AllocateSegment(bitmask_count, available_segments_per_buffer);

  total_segment_count++;

  // If the buffer is full, we cache the next buffer that we're going to fill.
  if (buffer->segment_count == available_segments_per_buffer) {
    buffers_with_free_space.erase(buffer_id);
    NextBufferWithFreeSpace();
  }

  return IndexPointer(uint32_t(buffer_id), offset);
}

void FixedSizeAllocator::Free(const IndexPointer ptr) {
  auto buffer_id = ptr.GetBufferId();
  auto offset = ptr.GetOffset();

  auto buffer_it = buffers.find(buffer_id);
  D_ASSERT(buffer_it != buffers.end());
  auto& buffer = buffer_it->second;

  buffer->FreeSegment(offset, available_segments_per_buffer);

  // Adjust the allocator fields.
  D_ASSERT(total_segment_count > 0);
  total_segment_count--;

  // Register the buffer before checking whether it became empty.
  buffers_with_free_space.insert(buffer_id);
  if (!buffer_with_free_space.IsValid()) {
    buffer_with_free_space = buffer_id;
  }

  // Early-out, if the buffer is not empty.
  if (buffer->segment_count != 0) {
    return;
  }

  // The segment count went to 0, i.e, the buffer is now empty.
  // We keep the buffer alive, if it is the only buffer with free space.
  // We do so to prevent too much buffer creation fluctuation.
  if (buffers_with_free_space.size() == 1) {
    D_ASSERT(*buffers_with_free_space.begin() == buffer_id);
    D_ASSERT(buffer_with_free_space.GetIndex() == buffer_id);
    return;
  }

  D_ASSERT(buffer_with_free_space.IsValid());
  buffers_with_free_space.erase(buffer_id);
  buffers.erase(buffer_it);

  // Cache the next buffer that we're going to fill.
  if (buffer_with_free_space.GetIndex() == buffer_id) {
    NextBufferWithFreeSpace();
  }
}

void FixedSizeAllocator::Detach(Checkpoint& target, MemoryLevel level) {
  auto allocator = target.runtime_allocator();
  for (auto& [id, buffer] : buffers)
    buffer->Detach(*allocator, level);
  runtime_allocator = std::move(allocator);
  memory_level = level;
}

void FixedSizeAllocator::Reset() {
  buffers.clear();
  buffer_metadata.clear();
  buffers_with_free_space.clear();
  buffer_with_free_space.SetInvalid();
  total_segment_count = 0;
}

MemoryUsage FixedSizeAllocator::GetMemoryUsage() const {
  MemoryUsage usage;
  usage.directory_bytes =
      sizeof(FixedSizeAllocator) + buffers.get_allocator().Bytes() +
      buffers_with_free_space.get_allocator().Bytes() +
      buffer_metadata.get_allocator().Bytes() + 3 * sizeof(std::atomic<size_t>);
  usage.occupied_node_bytes = total_segment_count * segment_size;
  for (const auto& [id, buffer] : buffers) {
    // The bitmap is inside the allocation. Split it out without double
    // counting.
    usage.node_buffer_bytes +=
        FixedSizeBuffer::BUFFER_DATA_SIZE - bitmask_offset;
    usage.occupancy_bytes += bitmask_offset;
    usage.directory_bytes += sizeof(FixedSizeBuffer) + sizeof(AnonMMap) +
                             FixedSizeBuffer::BUFFER_HEADER_SIZE;
  }
  return usage;
}

idx_t FixedSizeAllocator::GetUpperBoundBufferId() const {
  idx_t upper_bound_id = 0;
  for (auto& buffer : buffers) {
    if (buffer.first >= upper_bound_id) {
      upper_bound_id = buffer.first + 1;
    }
  }
  return upper_bound_id;
}

void FixedSizeAllocator::Save(CheckpointObjectWriter& writer) {
  for (const auto& [id, buffer] : buffers) {
    if (!buffer->segment_count)
      continue;
    auto saved = buffer_metadata.find(id);
    if (!buffer->dirty && saved != buffer_metadata.end()) {
      auto address = writer.MaterializeObject(saved->second.address);
      if (address != saved->second.address) {
        // The object has already been copied to disk, so this save only
        // updates its path for the manifest. If the checkpoint fails and GC
        // removes the copy, the dirty flag makes the next save rewrite the
        // buffer from memory.
        buffer->dirty = true;
        saved->second.address = std::move(address);
      }
      continue;
    }
    writer.Append(buffer->container->GetData(),
                  buffer->container->GetDataSize(), SaveCallback{*this, id});
  }
}

void FixedSizeAllocator::SaveCallback::operator()(
    const string& address, const CheckpointObjectSlice& slice) const {
  allocator.buffer_metadata.insert_or_assign(
      buffer_id,
      ARTBufferMetadata{buffer_id, address, slice.offset, slice.length});
}

const ARTBufferMetadataMap& FixedSizeAllocator::SaveMetadata(
    ModuleDescriptor& descriptor, const string& buffer_prefix) {
  // Remove directory entries for buffers that have been emptied or freed.
  for (auto it = buffer_metadata.begin(); it != buffer_metadata.end();) {
    auto buffer = buffers.find(it->first);
    if (buffer == buffers.end() || !buffer->second->segment_count)
      it = buffer_metadata.erase(it);
    else
      ++it;
  }

  for (const auto& [id, metadata] : buffer_metadata) {
    const auto key = buffer_prefix + to_string(id);
    descriptor.set_path(key, metadata.address);
    descriptor.set_slice(key, {metadata.offset, metadata.length});
  }
  return buffer_metadata;
}

void FixedSizeAllocator::Load(Checkpoint& checkpoint,
                              CheckpointObjectReader& reader,
                              const ModuleDescriptor& descriptor,
                              const string& buffer_prefix) {
  ARTBufferMetadataMap restored_metadata;
  Reset();
  for (const auto& [key, address] : descriptor.paths()) {
    if (!key.starts_with(buffer_prefix))
      continue;
    const auto id =
        ParseBufferID(std::string_view(key).substr(buffer_prefix.size()));
    if (id > std::numeric_limits<uint32_t>::max() || address.empty())
      throw SerializationException("Invalid ART buffer metadata");
    if (restored_metadata.contains(id))
      throw SerializationException("Duplicate ART buffer ID");
    CheckpointObjectView view;
    try {
      view = reader.Get(descriptor, key);
    } catch (const exception::CheckpointException& error) {
      throw SerializationException(string(error.what()));
    }
    if (view.length != FixedSizeBuffer::BUFFER_ALLOC_SIZE)
      throw SerializationException("Invalid ART buffer length: " + key);
    // Runtime buffers stay independent, so growing/detaching one does not
    // change addresses or retain an entire packed object mapping.
    auto buffer = std::make_unique<FixedSizeBuffer>(
        *checkpoint.runtime_allocator(), memory_level);
    std::memcpy(buffer->container->GetData(), view.data, view.length);
    SegmentHandle handle(*buffer, 0);
    ValidityMask free_mask(handle.GetPtr<validity_t>(),
                           available_segments_per_buffer);
    for (idx_t slot = 0; slot < available_segments_per_buffer; ++slot)
      buffer->segment_count += !free_mask.RowIsValid(slot);
    if (!buffer->segment_count)
      throw SerializationException("Empty ART buffer in checkpoint");
    buffer->dirty = false;
    total_segment_count += buffer->segment_count;
    if (buffer->segment_count < available_segments_per_buffer)
      buffers_with_free_space.insert(id);
    buffers.emplace(id, std::move(buffer));
    restored_metadata.emplace(
        id, ARTBufferMetadata{id, address, view.offset, view.length});
  }
  NextBufferWithFreeSpace();
  buffer_metadata = std::move(restored_metadata);
  runtime_allocator = checkpoint.runtime_allocator();
}

idx_t FixedSizeAllocator::GetAvailableBufferId() const {
  idx_t buffer_id = buffers.size();
  while (buffers.find(buffer_id) != buffers.end()) {
    D_ASSERT(buffer_id > 0);
    buffer_id--;
  }
  return buffer_id;
}

void FixedSizeAllocator::RemoveEmptyBuffers() {
  auto buffer_it = buffers.begin();
  while (buffer_it != buffers.end()) {
    if (buffer_it->second->segment_count != 0) {
      ++buffer_it;
      continue;
    }
    buffers_with_free_space.erase(buffer_it->first);
    buffer_it = buffers.erase(buffer_it);
  }
  NextBufferWithFreeSpace();
}

void FixedSizeAllocator::VerifyBuffers() const {
  idx_t count = 0;
  auto buffer_it = buffers.begin();
  while (buffer_it != buffers.end()) {
    if (buffer_it->second->segment_count == 0) {
      count++;
    }
    buffer_it++;
  }

  if (count > 1) {
    throw InternalException(
        "expected one, but got %d empty buffers in allocator", count);
  }
}

void FixedSizeAllocator::VerifyReference(const IndexPointer ptr) const {
  const auto buffer = buffers.find(ptr.GetBufferId());
  if (buffer == buffers.end())
    throw SerializationException("ART node references a missing buffer");
  const auto slot = ptr.GetOffset();
  if (slot >= available_segments_per_buffer)
    throw SerializationException("ART node slot is out of bounds");
  SegmentHandle bitmap(*buffer->second, 0);
  ValidityMask free_slots(bitmap.GetPtr<validity_t>(),
                          available_segments_per_buffer);
  if (free_slots.RowIsValid(slot))
    throw SerializationException("ART node references an unallocated slot");
}

void FixedSizeAllocator::NextBufferWithFreeSpace() {
  if (!buffers_with_free_space.empty()) {
    buffer_with_free_space = *buffers_with_free_space.begin();
    return;
  }
  buffer_with_free_space.SetInvalid();
}

}  // namespace neug::art
