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
#include <functional>
#include <utility>
#include <vector>

#include "neug/storages/chunk/chunk_types.h"

namespace neug {

/// Packs chunk block payloads into immutable objects and assigns each block an
/// ObjectSlice.
///
/// Checkpoint-scoped. Blocks are concatenated into the current object's payload
/// buffer; when the buffer reaches kObjectTargetBytes (or Seal() is called) the
/// buffer is published through the CommitSink, which returns the object's
/// stable id. The slices belonging to that object then receive the id.
///
/// The payload is the concatenation of block bytes and is committed through the
/// checkpoint container path, which adds the standard file header. Each
/// block's offset/length/crc32c is relative to the payload exposed by
/// IDataContainer and lives in the chunk directory.
class ObjectWriter {
 public:
  static constexpr size_t kObjectTargetBytes = 64ull * 1024 * 1024;

  /// Publishes an accumulated payload buffer as an immutable object and returns
  /// its stable object-store id.
  using CommitSink = std::function<uint64_t(const void* data, size_t length)>;

  explicit ObjectWriter(CommitSink sink,
                        size_t target_bytes = kObjectTargetBytes)
      : sink_(std::move(sink)), target_bytes_(target_bytes) {}

  /// Append a block payload and return its index into slices(). The returned
  /// slice's object_id is finalized once the containing object is committed
  /// (on reaching target_bytes or Seal()); read the authoritative value from
  /// slices() after that point.
  size_t AppendBlock(const void* data, uint32_t length);

  /// Commit the current partial object, if any. Idempotent when empty.
  void Seal();

  /// All slices appended so far. A slice's object_id is valid only after the
  /// object containing it has been committed.
  const std::vector<ObjectSlice>& slices() const { return slices_; }

 private:
  void FlushCurrentObject();

  CommitSink sink_;
  size_t target_bytes_;
  std::vector<char> buffer_;               // current object's payload
  std::vector<ObjectSlice> slices_;        // all appended slices
  size_t current_object_first_slice_ = 0;  // first slice index in buffer_
};

}  // namespace neug
