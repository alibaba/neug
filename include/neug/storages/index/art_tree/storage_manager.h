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

#include <cstdio>
#include <memory>

#include "neug/storages/checkpoint.h"
#include "neug/storages/index/art_tree/common.h"
#include "neug/utils/md5_utils.h"

namespace neug::art {

struct ARTStorageSlice {
  // Relative to the object payload, after FileHeader.
  idx_t offset;
  idx_t length;
  MD5Digest checksum;
};

// Writes buffers into one checkpoint object without consuming their containers.
// The owner commits after all buffers have been appended, then records the
// returned object address alongside each slice in its metadata.
class ARTStorageManager {
 public:
  explicit ARTStorageManager(Checkpoint& checkpoint);
  ARTStorageManager(const ARTStorageManager&) = delete;
  ARTStorageManager& operator=(const ARTStorageManager&) = delete;

  ARTStorageSlice Append(const void* data, idx_t length);
  string Commit();

 private:
  void Write(const void* data, size_t length);

  Checkpoint& checkpoint_;
  // Close the stream before the runtime handle removes an uncommitted file.
  CheckpointFileManager::RuntimeFileHandle file_;
  std::unique_ptr<FILE, decltype(&fclose)> output_;
  MD5 checksum_;
  idx_t offset_ = 0;
};

}  // namespace neug::art
