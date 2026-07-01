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
 private:
  struct RuntimeFileCleanupContext;

 public:
  class RuntimeFileHandle {
   public:
    RuntimeFileHandle() = default;
    ~RuntimeFileHandle();

    RuntimeFileHandle(const RuntimeFileHandle&) = delete;
    RuntimeFileHandle& operator=(const RuntimeFileHandle&) = delete;

    RuntimeFileHandle(RuntimeFileHandle&& other) noexcept;
    RuntimeFileHandle& operator=(RuntimeFileHandle&& other) noexcept;

    const std::string& uuid() const { return uuid_; }
    const std::string& path() const { return path_; }
    bool valid() const { return cleanup_ != nullptr; }

   private:
    friend class CheckpointFileManager;

    RuntimeFileHandle(std::shared_ptr<RuntimeFileCleanupContext> cleanup,
                      std::string uuid, std::string path);

    void Release();

    std::shared_ptr<RuntimeFileCleanupContext> cleanup_;
    std::string uuid_;
    std::string path_;
  };

  CheckpointFileManager(const std::string& snapshot_dir,
                        const std::string& runtime_dir);
  ~CheckpointFileManager();

  CheckpointFileManager(const CheckpointFileManager&) = delete;
  CheckpointFileManager& operator=(const CheckpointFileManager&) = delete;

  /// Open (or create) a data container backed by a file.
  std::shared_ptr<IDataContainer> OpenFile(const std::string& file_path,
                                           MemoryLevel level);

  /// Create an anonymous runtime container of the given size.
  std::shared_ptr<IDataContainer> CreateRuntimeContainer(size_t size,
                                                         MemoryLevel level);

  /// Commit a data container to a persistent snapshot file.
  /// Returns the absolute path to the committed file.
  std::string Commit(IDataContainer& buffer);

  /// Create a runtime file for manual writers. The handle removes the file if
  /// it is destroyed before CommitRuntimeFile() consumes it.
  RuntimeFileHandle CreateRuntimeFile();

  /// Finalize a manually-written runtime file into snapshot_dir.
  std::string CommitRuntimeFile(RuntimeFileHandle file);

  /// Hardlink (or copy) a caller-guaranteed immutable/retired file into
  /// snapshot_dir. This fast path is valid for files that no legal writer can
  /// mutate again, such as clean runtime files from a previous checkpoint.
  ///
  /// Active MAP_SHARED runtime containers must use Commit(), which materializes
  /// an independent snapshot and protects the checkpoint from future mmap
  /// writes through the same backing file.
  std::string LinkToSnapshot(const std::string& abs_path);

  /// fsync snapshot_dir after snapshot file entries are ready.
  bool SyncSnapshotDirectory() const;

  /// Make an absolute path relative to the checkpoint root.
  std::string MakeRelativePath(const std::string& abs_path,
                               const std::string& checkpoint_root) const;

  /// Resolve a relative path against the checkpoint root.
  std::string ResolveAbsolutePath(const std::string& rel_path,
                                  const std::string& checkpoint_root) const;

  const std::string& snapshot_dir() const { return snapshot_dir_; }
  const std::string& runtime_dir() const { return runtime_dir_; }

 private:
  std::string WriteBufferToSnapshot(IDataContainer& buffer);

  std::shared_ptr<IDataContainer> AdoptRuntimeContainer(
      std::unique_ptr<IDataContainer> container) const;

  std::string CreateRuntimeContainerPath();
  std::string CreateRuntimeObjectNameLocked() const;
  std::string CopyToSnapshot(const std::string& abs_path);
  std::string copyToSnapshotLocked(const std::string& abs_path);
  std::string commitRuntimeFileLocked(const std::string& uuid,
                                      const std::string& abs_path);

  std::string snapshot_dir_;
  std::string runtime_dir_;
  mutable std::mutex mutex_;
  std::shared_ptr<RuntimeFileCleanupContext> runtime_cleanup_;
};

}  // namespace neug
