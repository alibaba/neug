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

#pragma once

#include <cstddef>
#include <cstdio>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "neug/storages/checkpoint_file_manager.h"
#include "neug/utils/api.h"
#include "neug/utils/md5_utils.h"

namespace neug {

// A checkpoint object can pack data from multiple containers into one file
// to reduce the number of small checkpoint files. Each slice is located
// by its byte offset and length within the object payload.
//
// The writer appends data to the object file and passes the object address
// and slice location to the caller's callback after committing the object.
// The reader provides slices for database recovery and keeps file mappings
// alive for the duration of the reader to avoid reopening the same object.

class Checkpoint;
struct ModuleDescriptor;

struct CheckpointObjectSlice {
  // Relative to the object payload, after FileHeader.
  size_t offset;
  size_t length;
};

struct CheckpointObjectView {
  const void* data;
  size_t length;
  size_t offset;
};

/// Reuses verified object mappings for a group of checkpoint reads.
/// Keep the checkpoint alive and serialize access to the reader.
class NEUG_API CheckpointObjectReader {
 public:
  CheckpointObjectReader(const CheckpointObjectReader&) = delete;
  CheckpointObjectReader& operator=(const CheckpointObjectReader&) = delete;

  /// Read a named object or slice. Whole objects start at payload offset zero.
  /// The view is valid for the lifetime of the reader.
  CheckpointObjectView Get(const ModuleDescriptor& descriptor,
                           const std::string& name);

 private:
  friend class Checkpoint;
  explicit CheckpointObjectReader(Checkpoint& checkpoint)
      : checkpoint_(checkpoint) {}

  Checkpoint& checkpoint_;
  std::unordered_map<std::string, std::shared_ptr<IDataContainer>> objects_;
};

/// Appends fragments to one immutable checkpoint object in call order. The
/// caller owns the input memory and can reuse it as soon as Append returns.
/// Keep the checkpoint alive for the lifetime of the writer and serialize
/// access to the writer. Destroying an uncommitted writer discards its file.
class NEUG_API CheckpointObjectWriter {
 public:
  using CommitCallback =
      std::function<void(const std::string&, const CheckpointObjectSlice&)>;

  explicit CheckpointObjectWriter(Checkpoint& checkpoint);
  CheckpointObjectWriter(const CheckpointObjectWriter&) = delete;
  CheckpointObjectWriter& operator=(const CheckpointObjectWriter&) = delete;

  /// Write a nonempty fragment and register its required completion callback.
  void Append(const void* data, size_t length, CommitCallback on_commit);

  /// Publish the object and return its path. An empty writer returns an empty
  /// path. Callbacks run synchronously in append order after object
  /// publication. A callback exception propagates after publication and skips
  /// later callbacks. The caller owns metadata for the pending checkpoint
  /// manifest.
  std::string Commit();

 private:
  struct PendingFragment {
    CheckpointObjectSlice slice;
    CommitCallback on_commit;
  };

  void Write(const void* data, size_t length);

  Checkpoint& checkpoint_;
  // Close the stream before the runtime handle removes an uncommitted file.
  CheckpointFileManager::RuntimeFileHandle file_;
  std::unique_ptr<FILE, decltype(&fclose)> output_;
  MD5 checksum_;
  size_t offset_ = 0;
  std::vector<PendingFragment> fragments_;
};

}  // namespace neug
