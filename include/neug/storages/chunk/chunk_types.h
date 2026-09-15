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
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace neug {

/// Descriptor path key for a chunked column's serialized chunk-directory
/// object. Shared by ChunkedColumn (Dump/Open) and checkpoint GC, which parses
/// the directory to retain the chunk objects it references.
inline constexpr const char* kChunkDirPath = "chunk_dir";

/// CRC32C (Castagnoli) checksum of a payload, used to verify object-slice
/// integrity on read. Software table-driven implementation; may be replaced by
/// a hardware CRC32C intrinsic later without changing callers.
uint32_t Crc32c(const void* data, size_t length);

/// Parse a serialized chunk-directory blob (the format written by
/// ChunkedColumn::Dump) and return the checkpoint-local object IDs it
/// references. Checkpoint GC uses this to retain chunk objects, which live
/// inside the directory blob and are therefore invisible in the module
/// descriptor's own paths. Malformed blobs throw rather than returning a
/// partial result that could make GC reclaim live objects.
std::vector<std::string> ExtractChunkDirObjectIds(const void* data,
                                                  size_t size);

/// Location of a persisted chunk block within an immutable object.
///
/// Fixed 24-byte POD. `object_id` is an in-memory index into the checkpoint's
/// ObjectWriter table; persisted directories store a local table of stable
/// object IDs and encode only its per-directory index, never an absolute path.
struct ObjectSlice {
  uint64_t object_id = 0;  // stable object-store identity
  uint64_t offset = 0;     // payload byte offset within the object
  uint32_t length = 0;     // payload byte length
  uint32_t crc32c = 0;     // payload checksum
};

/// A physical page may have an immutable prefix and an appendable suffix.
struct ChunkDirectoryPage {
  uint32_t prefix_rows = 0;
  ObjectSlice prefix;
  ObjectSlice suffix;
};

struct ChunkDirectory {
  uint32_t row_width = 0;  // zero only when decoding the legacy directory
  uint64_t rows_per_chunk = 0;
  uint64_t rows_per_page = 0;
  uint64_t row_count = 0;
  std::vector<std::string> object_ids;
  std::vector<ChunkDirectoryPage> pages;
};

/// Versioned, checksummed encoding shared by column recovery and GC.
std::vector<char> EncodeChunkDirectory(const ChunkDirectory& directory);
ChunkDirectory DecodeChunkDirectory(const void* data, size_t size);

/// Reserved for a future global chunk index. The current directory format uses
/// the module key and local chunk ordinal, so this type is intentionally not
/// part of the persisted checkpoint contract yet.
struct ChunkKey {
  uint64_t column_uid = 0;
  uint32_t chunk_index = 0;
};

/// Reserved allocator for the future global chunk index. It is not persisted
/// until that index becomes part of the storage format.
class ColumnUidRegistry {
 public:
  explicit ColumnUidRegistry(uint64_t next = 1) : next_(next) {}

  /// Allocate a fresh uid.
  uint64_t Allocate() { return next_++; }

  /// The next uid to allocate (persisted for recovery).
  uint64_t next() const { return next_; }

  /// Restore the counter from a persisted manifest scalar.
  void SetNext(uint64_t next) { next_ = next; }

 private:
  uint64_t next_;
};

}  // namespace neug
