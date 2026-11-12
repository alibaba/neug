/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/storages/chunk/object_writer.h"

#include <array>
#include <cstring>

#include "neug/utils/exception/exception.h"

namespace neug {

uint32_t Crc32c(const void* data, size_t length) {
  // CRC32C (Castagnoli), reflected polynomial 0x82F63B78. Table built once.
  static const std::array<uint32_t, 256> table = [] {
    std::array<uint32_t, 256> t{};
    for (uint32_t i = 0; i < 256; ++i) {
      uint32_t crc = i;
      for (int j = 0; j < 8; ++j) {
        crc = (crc >> 1) ^ (0x82F63B78u & (-(crc & 1)));
      }
      t[i] = crc;
    }
    return t;
  }();

  const auto* p = static_cast<const unsigned char*>(data);
  uint32_t crc = 0xFFFFFFFFu;
  for (size_t i = 0; i < length; ++i) {
    crc = table[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
  }
  return crc ^ 0xFFFFFFFFu;
}

std::vector<std::string> ExtractChunkDirObjectIds(const void* data,
                                                  size_t size) {
  return DecodeChunkDirectory(data, size).object_ids;
}

size_t ObjectWriter::AppendBlock(const void* data, uint32_t length) {
  // Different columns/append suffixes have different widths. Every slice
  // starts on a natural alignment suitable for all fixed-width property types.
  buffer_.resize((buffer_.size() + 15) & ~size_t{15}, 0);
  ObjectSlice slice;
  slice.offset = buffer_.size();
  slice.length = length;
  slice.crc32c = Crc32c(data, length);
  slice.object_id = 0;  // pending until the containing object is committed

  const size_t index = slices_.size();
  slices_.push_back(slice);

  const auto* begin = static_cast<const char*>(data);
  buffer_.insert(buffer_.end(), begin, begin + length);

  if (buffer_.size() >= target_bytes_) {
    FlushCurrentObject();
  }
  return index;
}

void ObjectWriter::Seal() {
  if (!buffer_.empty()) {
    FlushCurrentObject();
  }
}

void ObjectWriter::FlushCurrentObject() {
  const uint64_t object_id = sink_(buffer_.data(), buffer_.size());
  for (size_t i = current_object_first_slice_; i < slices_.size(); ++i) {
    slices_[i].object_id = object_id;
  }
  current_object_first_slice_ = slices_.size();
  buffer_.clear();
}

}  // namespace neug
