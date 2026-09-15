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
/// ChunkedColumn::Dump) and return the object path of every chunk it
/// references. Checkpoint GC uses this to retain chunk objects, which live
/// inside the directory blob and are therefore invisible in the module
/// descriptor's own paths. Bounds-checked: a truncated/malformed blob yields
/// only the paths fully present.
std::vector<std::string> ExtractChunkDirObjectPaths(const void* data,
                                                    size_t size);

/// Location of a persisted chunk block within an immutable object.
///
/// Fixed 24-byte POD; serialized little-endian into a chunk directory as a
/// contiguous array. `object_id` is the stable object-store identity, so a
/// slice never embeds an absolute path or an in-memory pointer.
struct ObjectSlice {
  uint64_t object_id = 0;  // stable object-store identity
  uint64_t offset = 0;     // payload byte offset within the object
  uint32_t length = 0;     // payload byte length
  uint32_t crc32c = 0;     // payload checksum
};

/// Stable identity of a chunk: a stable column uid plus the logical chunk
/// index. Deliberately independent of the reorderable schema label ids.
struct ChunkKey {
  uint64_t column_uid = 0;
  uint32_t chunk_index = 0;
};

/// Allocates stable, monotonic, never-reused column uids, independent of schema
/// label ids so that schema reorder/drop does not disturb chunk identity.
/// Persisted as the manifest scalar `column_uid_next`.
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
