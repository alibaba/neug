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

class Checkpoint;
class CheckpointManager;

/** Shared lifetime owner for one database-open runtime directory. */
class RuntimeWorkspace final {
 public:
  ~RuntimeWorkspace();

  RuntimeWorkspace(const RuntimeWorkspace&) = delete;
  RuntimeWorkspace& operator=(const RuntimeWorkspace&) = delete;

  const std::string& path() const noexcept { return path_; }

 private:
  friend class CheckpointManager;

  static std::shared_ptr<const RuntimeWorkspace> Create(std::string path);

  explicit RuntimeWorkspace(std::string path);

  std::string path_;
};

/**
 * @brief Publishes immutable checkpoint objects and owns one runtime epoch.
 *
 * Extracted from Checkpoint to separate file-management concerns (create,
 * commit, materialize, cleanup) from manifest/directory-structure management.
 *
 * Thread safety: all public methods are safe to call concurrently.
 */
class CheckpointFileManager {
 private:
  struct RuntimeFileCleanupContext;

 public:
  class RuntimeFileHandle {
   public:
    ~RuntimeFileHandle();

    RuntimeFileHandle(const RuntimeFileHandle&) = delete;
    RuntimeFileHandle& operator=(const RuntimeFileHandle&) = delete;

    RuntimeFileHandle(RuntimeFileHandle&& other) noexcept;

    const std::string& path() const { return path_; }

   private:
    friend class CheckpointFileManager;

    RuntimeFileHandle(std::shared_ptr<RuntimeFileCleanupContext> cleanup,
                      std::string path);

    bool valid() const { return cleanup_ != nullptr; }
    void Release();

    std::shared_ptr<RuntimeFileCleanupContext> cleanup_;
    std::string path_;
  };

  ~CheckpointFileManager();

  CheckpointFileManager(const CheckpointFileManager&) = delete;
  CheckpointFileManager& operator=(const CheckpointFileManager&) = delete;

  /// Open (or create) a data container backed by a file.
  std::shared_ptr<IDataContainer> OpenFile(const std::string& file_path,
                                           MemoryLevel level);

  /// Create an anonymous runtime container of the given size.
  std::shared_ptr<IDataContainer> CreateRuntimeContainer(size_t size,
                                                         MemoryLevel level);

  /// Commit a data container to a persistent immutable object.
  ///
  /// This is a consuming operation: the container dumps its current contents
  /// and is closed by the call. The caller must not read from or write to the
  /// container after Commit() returns.
  ///
  /// Returns the absolute path to the committed file.
  std::string Commit(IDataContainer& buffer);

  /// Create a runtime file for manual writers. The handle removes the file if
  /// it is destroyed before CommitRuntimeFile() consumes it.
  RuntimeFileHandle CreateRuntimeFile();

  /// Finalize a manually-written runtime file into the object store.
  std::string CommitRuntimeFile(RuntimeFileHandle&& file);

  /// Reuse an existing object-store path, or copy a runtime file into a fresh
  /// immutable object. Active MAP_SHARED files are never hardlinked.
  std::string MaterializeObject(const std::string& abs_path);

 private:
  friend class Checkpoint;

  CheckpointFileManager(
      const std::string& object_dir,
      std::shared_ptr<const RuntimeWorkspace> runtime_workspace);

  /// fsync the object directory after object entries are ready.
  bool SyncObjectDirectory() const;

  std::shared_ptr<IDataContainer> WrapWithRuntimeCleanup(
      std::unique_ptr<IDataContainer> container) const;

  std::string CreateRuntimeContainerPath();
  std::string CreateRuntimeObjectNameLocked() const;
  std::string materializeObjectLocked(const std::string& abs_path);
  std::string commitRuntimeFileLocked(const std::string& uuid,
                                      const std::string& abs_path);

  std::string object_dir_;
  std::string runtime_dir_;
  mutable std::mutex mutex_;
  std::shared_ptr<RuntimeFileCleanupContext> runtime_cleanup_;
};

}  // namespace neug
