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

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>

#include "neug/storages/checkpoint_file_manager.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/module_descriptor.h"

namespace neug {

class LegacyCheckpointMigrator;

/**
 * @brief A manifest-selected checkpoint root.
 *
 * `CURRENT` selects one immutable-object manifest. Its ID also names the WAL
 * epoch; the manifest's base timestamp bounds replay from that epoch. Runtime
 * working files belong to the current database-open epoch instead:
 *
 * ```
 * db/
 * ├── checkpoint/{CURRENT,manifests,objects}
 * ├── wal/<checkpoint-id>/
 * └── runtime/<open-epoch>/
 * ```
 *
 * # Ownership and synchronization
 *
 * `CheckpointManager` owns checkpoint creation and publication. It serializes
 * staging, manifest persistence, and replacement of `CURRENT`. A
 * `Checkpoint` holds the selected manifest and delegates runtime-file and
 * immutable-object operations to `CheckpointFileManager`.
 *
 * `SetManifest()` and `manifest()` are not internally synchronized: callers
 * must keep manifest construction and inspection within the checkpoint
 * maintenance/publish lifecycle. File operations are synchronized by
 * `CheckpointFileManager`; this does not make concurrent mutation of the
 * same `IDataContainer` safe.
 */
class Checkpoint {
 public:
  ~Checkpoint();
  Checkpoint(const Checkpoint&) = delete;
  Checkpoint& operator=(const Checkpoint&) = delete;

  /// Canonical manifest path for this root.
  const std::string& manifest_path() const { return manifest_path_; }
  uint64_t id() const { return id_; }

  /// Temporary workspace belonging to this database-open epoch.
  const std::string& runtime_dir() const;

  /// WAL epoch directory whose name is this manifest ID.
  const std::string& wal_dir() const { return wal_dir_; }

  std::string allocator_dir() const { return runtime_dir() + "/allocator"; }

  std::shared_ptr<IDataContainer> OpenFile(const std::string& file_path,
                                           MemoryLevel level) {
    return file_mgr_->OpenFile(file_path, level);
  }

  std::shared_ptr<IDataContainer> CreateRuntimeContainer(size_t size,
                                                         MemoryLevel level) {
    return file_mgr_->CreateRuntimeContainer(size, level);
  }

  /// Consume @p buffer and publish it as an immutable object.
  std::string Commit(IDataContainer& buffer) {
    return file_mgr_->Commit(buffer);
  }

  /// Set the complete in-memory manifest. CheckpointManager persists it before
  /// advancing CURRENT.
  void SetManifest(CheckpointManifest&& manifest);

  const CheckpointManifest& manifest() const { return manifest_; }

  /// Allocate an anonymous runtime file handle (RAII).
  CheckpointFileManager::RuntimeFileHandle CreateRuntimeFile() {
    return file_mgr_->CreateRuntimeFile();
  }

  /// Finalize a runtime file as an immutable object and return its path.
  std::string CommitRuntimeFile(
      CheckpointFileManager::RuntimeFileHandle&& file) {
    return file_mgr_->CommitRuntimeFile(std::move(file));
  }

  /// Reuse an object already in the object store; runtime files are copied to
  /// a fresh immutable object.
  std::string MaterializeObject(const std::string& abs_path) {
    return file_mgr_->MaterializeObject(abs_path);
  }

 private:
  friend class CheckpointManager;
  friend class LegacyCheckpointMigrator;

  static std::shared_ptr<Checkpoint> OpenPublished(
      std::string database_dir, uint64_t id,
      std::shared_ptr<const RuntimeWorkspace> runtime_workspace);
  static std::shared_ptr<Checkpoint> CreateStaging(
      std::string database_dir, uint64_t id,
      std::shared_ptr<const RuntimeWorkspace> runtime_workspace);

  Checkpoint(std::string database_dir, uint64_t id,
             std::shared_ptr<const RuntimeWorkspace> runtime_workspace);

  void initialize(bool load_manifest);
  void create_dirs() const;
  void resolve_object_paths();
  void persist_manifest();

  std::string database_dir_;
  std::string manifest_path_;
  std::string object_dir_;
  std::shared_ptr<const RuntimeWorkspace> runtime_workspace_;
  std::string wal_dir_;
  uint64_t id_;
  CheckpointManifest manifest_;
  std::unique_ptr<CheckpointFileManager> file_mgr_;
};

/// File name prefix for the allocator with @p allocator_id under
/// @p allocator_dir, e.g. `<allocator_dir>/allocator_3_`.
inline std::string allocator_prefix(const std::string& allocator_dir,
                                    size_t allocator_id) {
  return (std::filesystem::path(allocator_dir) /
          ("allocator_" + std::to_string(allocator_id) + "_"))
      .string();
}

}  // namespace neug
