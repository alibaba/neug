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

#include "neug/storages/chunk/chunk_types.h"

#include <filesystem>
#include <limits>
#include "neug/utils/exception/exception.h"

namespace neug {
namespace {
constexpr uint32_t kMagic = 0x4443474e;  // NGCD, little endian
constexpr uint32_t kVersion = 1;
void Put(std::vector<char>& out, uint64_t value, size_t bytes) {
  for (size_t i = 0; i < bytes; ++i)
    out.push_back(static_cast<char>(value >> (8 * i)));
}
class Reader {
 public:
  Reader(const void* data, size_t size)
      : data_(static_cast<const char*>(data)), remaining_(size) {}
  uint64_t Read(size_t bytes) {
    if (remaining_ < bytes)
      THROW_CHECKPOINT_EXCEPTION("Truncated chunk directory");
    uint64_t value = 0;
    for (size_t i = 0; i < bytes; ++i)
      value |= static_cast<uint64_t>(static_cast<unsigned char>(*data_++))
               << (8 * i);
    remaining_ -= bytes;
    return value;
  }
  std::string String(size_t bytes) {
    if (bytes == 0 || remaining_ < bytes)
      THROW_CHECKPOINT_EXCEPTION("Invalid chunk object ID");
    std::string result(data_, bytes);
    data_ += bytes;
    remaining_ -= bytes;
    return result;
  }
  size_t remaining() const { return remaining_; }

 private:
  const char* data_;
  size_t remaining_;
};
bool PowerOfTwo(uint64_t n) { return n != 0 && (n & (n - 1)) == 0; }
ObjectSlice ReadSlice(Reader& reader, const ChunkDirectory& d, uint64_t rows) {
  ObjectSlice slice;
  slice.object_id = reader.Read(4);
  slice.offset = reader.Read(8);
  slice.length = reader.Read(4);
  slice.crc32c = reader.Read(4);
  if (slice.object_id >= d.object_ids.size() || slice.length == 0 ||
      (d.row_width != 0 && slice.length != rows * d.row_width) ||
      slice.offset > std::numeric_limits<uint64_t>::max() - slice.length)
    THROW_CHECKPOINT_EXCEPTION("Invalid chunk directory slice");
  return slice;
}
void PutSlice(std::vector<char>& out, const ObjectSlice& slice) {
  if (slice.object_id > std::numeric_limits<uint32_t>::max())
    THROW_CHECKPOINT_EXCEPTION("Chunk object index overflow");
  Put(out, slice.object_id, 4);
  Put(out, slice.offset, 8);
  Put(out, slice.length, 4);
  Put(out, slice.crc32c, 4);
}
}  // namespace

ChunkDirectory DecodeChunkDirectory(const void* data, size_t size) {
  if (data == nullptr || size < 32)
    THROW_CHECKPOINT_EXCEPTION("Truncated chunk directory");
  Reader first(data, size);
  const bool versioned = first.Read(4) == kMagic;
  size_t payload_size = size;
  if (versioned) {
    if (size < 60)
      THROW_CHECKPOINT_EXCEPTION("Truncated chunk directory header");
    Reader trailer(static_cast<const char*>(data) + size - 4, 4);
    if (Crc32c(data, size - 4) != trailer.Read(4))
      THROW_CHECKPOINT_EXCEPTION("Chunk directory checksum mismatch");
    payload_size -= 4;
  }
  Reader reader(data, payload_size);
  ChunkDirectory d;
  uint64_t page_count;
  if (versioned) {
    (void) reader.Read(4);
    if (reader.Read(4) != kVersion)
      THROW_CHECKPOINT_EXCEPTION("Unsupported chunk directory version");
    d.row_width = reader.Read(4);
    if (reader.Read(4) != 0 || d.row_width == 0)
      THROW_CHECKPOINT_EXCEPTION("Invalid chunk directory flags or row width");
    d.rows_per_chunk = reader.Read(8);
    d.rows_per_page = reader.Read(8);
    d.row_count = reader.Read(8);
    page_count = reader.Read(8);
  } else {
    d.rows_per_chunk = reader.Read(8);
    d.rows_per_page = d.rows_per_chunk;
    page_count = reader.Read(8);
    d.row_count = reader.Read(8);
  }
  const uint64_t object_count = reader.Read(8);
  if (!PowerOfTwo(d.rows_per_chunk) || !PowerOfTwo(d.rows_per_page) ||
      d.rows_per_page > d.rows_per_chunk ||
      d.rows_per_page > std::numeric_limits<uint32_t>::max() ||
      (d.row_width &&
       d.rows_per_page > std::numeric_limits<uint32_t>::max() / d.row_width) ||
      page_count != d.row_count / d.rows_per_page +
                        (d.row_count % d.rows_per_page != 0) ||
      object_count > reader.remaining() / 4 ||
      page_count > reader.remaining() / 20)
    THROW_CHECKPOINT_EXCEPTION("Inconsistent chunk directory header");
  for (uint64_t i = 0; i < object_count; ++i) {
    auto id = reader.String(reader.Read(4));
    std::filesystem::path path(id);
    // The initial PR encoded absolute paths. Resolve their basename in this
    // database's object store, never follow the original path after relocation.
    if (!versioned && path.is_absolute())
      id = path.filename().string();
    path = std::filesystem::path(id);
    if (path.empty() || path.is_absolute() || path.has_parent_path() ||
        id == "." || id == "..")
      THROW_CHECKPOINT_EXCEPTION("Invalid chunk object ID");
    d.object_ids.push_back(std::move(id));
  }
  for (uint64_t i = 0; i < page_count; ++i) {
    ChunkDirectoryPage page;
    page.prefix_rows = versioned ? reader.Read(4) : d.rows_per_page;
    if (page.prefix_rows == 0 || page.prefix_rows > d.rows_per_page)
      THROW_CHECKPOINT_EXCEPTION("Invalid chunk prefix range");
    page.prefix = ReadSlice(reader, d, page.prefix_rows);
    if (page.prefix_rows < d.rows_per_page)
      page.suffix = ReadSlice(reader, d, d.rows_per_page - page.prefix_rows);
    d.pages.push_back(page);
  }
  if (reader.remaining() != 0)
    THROW_CHECKPOINT_EXCEPTION("Trailing chunk directory data");
  return d;
}

std::vector<char> EncodeChunkDirectory(const ChunkDirectory& d) {
  std::vector<char> out;
  Put(out, kMagic, 4);
  Put(out, kVersion, 4);
  Put(out, d.row_width, 4);
  Put(out, 0, 4);
  Put(out, d.rows_per_chunk, 8);
  Put(out, d.rows_per_page, 8);
  Put(out, d.row_count, 8);
  Put(out, d.pages.size(), 8);
  Put(out, d.object_ids.size(), 8);
  for (const auto& id : d.object_ids) {
    if (id.size() > std::numeric_limits<uint32_t>::max())
      THROW_CHECKPOINT_EXCEPTION("Chunk object ID overflow");
    Put(out, id.size(), 4);
    out.insert(out.end(), id.begin(), id.end());
  }
  for (const auto& page : d.pages) {
    Put(out, page.prefix_rows, 4);
    PutSlice(out, page.prefix);
    if (page.prefix_rows < d.rows_per_page)
      PutSlice(out, page.suffix);
  }
  Put(out, Crc32c(out.data(), out.size()), 4);
  // Keep writers and readers on the same invariant set.
  (void) DecodeChunkDirectory(out.data(), out.size());
  return out;
}
}  // namespace neug
