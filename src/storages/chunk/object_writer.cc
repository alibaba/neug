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

std::vector<std::string> ExtractChunkDirObjectPaths(const void* data,
                                                    size_t size) {
  // Mirrors the little-endian layout written by ChunkedColumn::Dump:
  //   [u64 rows_per_chunk][u64 chunk_count][u64 row_count][u64 object_count]
  //   per object: [u32 path_len][path bytes]
  //   per chunk:  [u32 obj_index][u64 offset][u32 length][u32 crc32c]
  // Only the object-path table is needed to retain the referenced objects.
  std::vector<std::string> paths;
  const char* cursor = static_cast<const char*>(data);
  const char* const end = cursor + size;
  auto read_u64 = [&](uint64_t& out) -> bool {
    if (cursor + 8 > end) {
      return false;
    }
    out = 0;
    for (int i = 0; i < 8; ++i) {
      out |= static_cast<uint64_t>(static_cast<unsigned char>(*cursor++))
             << (8 * i);
    }
    return true;
  };
  auto read_u32 = [&](uint32_t& out) -> bool {
    if (cursor + 4 > end) {
      return false;
    }
    out = 0;
    for (int i = 0; i < 4; ++i) {
      out |= static_cast<uint32_t>(static_cast<unsigned char>(*cursor++))
             << (8 * i);
    }
    return true;
  };

  uint64_t rows_per_chunk = 0;
  uint64_t chunk_count = 0;
  uint64_t row_count = 0;
  uint64_t object_count = 0;
  if (!read_u64(rows_per_chunk) || !read_u64(chunk_count) ||
      !read_u64(row_count) || !read_u64(object_count)) {
    return paths;  // truncated header
  }
  for (uint64_t o = 0; o < object_count; ++o) {
    uint32_t path_len = 0;
    if (!read_u32(path_len) || cursor + path_len > end) {
      break;  // truncated path entry
    }
    paths.emplace_back(cursor, path_len);
    cursor += path_len;
  }
  return paths;
}

size_t ObjectWriter::AppendBlock(const void* data, uint32_t length) {
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
