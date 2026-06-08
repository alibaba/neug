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

#include <memory>
#include <mutex>
#include <set>
#include <string>

#include "neug/storages/container/container_utils.h"
#include "neug/storages/container/i_container.h"
#include "neug/utils/uuid.h"

namespace neug {

/**
 * @brief Manages file lifecycle within a single checkpoint directory.
 *
 * Extracted from Checkpoint to separate file-management concerns (create,
 * commit, link, cleanup) from meta/directory-structure management.
 *
 * Thread safety: all public methods are safe to call concurrently.
 */
class CheckpointFileManager {
 public:
  CheckpointFileManager(const std::string& snapshot_dir,
                        const std::string& runtime_dir);
  ~CheckpointFileManager();

  CheckpointFileManager(const CheckpointFileManager&) = delete;
  CheckpointFileManager& operator=(const CheckpointFileManager&) = delete;

  /// Open (or create) a data container backed by a file.
  std::unique_ptr<IDataContainer> OpenFile(const std::string& file_path,
                                           MemoryLevel level);

  /// Create an anonymous runtime container of the given size.
  std::unique_ptr<IDataContainer> CreateRuntimeContainer(size_t size,
                                                         MemoryLevel level);

  /// Commit a data container to a persistent snapshot file.
  /// Returns the absolute path to the committed file.
  std::string Commit(IDataContainer& buffer);

  /// Allocate a new UUID-named file slot in runtime_dir.
  std::string CreateRuntimeObject();

  /// Finalize a runtime object into snapshot_dir.
  std::string CommitRuntimeObject(const std::string& uuid);

  /// Hardlink (or copy) an external file into snapshot_dir.
  std::string LinkToSnapshot(const std::string& abs_path);

  /// Make an absolute path relative to the checkpoint root.
  std::string MakeRelativePath(const std::string& abs_path,
                               const std::string& checkpoint_root) const;

  /// Resolve a relative path against the checkpoint root.
  std::string ResolveAbsolutePath(const std::string& rel_path,
                                  const std::string& checkpoint_root) const;

  const std::string& snapshot_dir() const { return snapshot_dir_; }
  const std::string& runtime_dir() const { return runtime_dir_; }

 private:
  std::string CommitToSnapshot(const std::string& abs_path);
  std::string commitToSnapshotLocked(const std::string& abs_path);

  std::string snapshot_dir_;
  std::string runtime_dir_;
  mutable std::mutex mutex_;
  std::set<std::string> uncommitted_runtime_objects_;
};

}  // namespace neug
