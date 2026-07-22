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

#include <cassert>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "neug/storages/checkpoint_file_manager.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/module_descriptor.h"

namespace neug {

/**
 * @brief Represents a single numbered checkpoint directory.
 *
 * A checkpoint lives at `db_dir/checkpoint-NNNNN/` and contains:
 *
 * ```
 * checkpoint-NNNNN/
 * ├── meta        ← CheckpointManifest JSON
 * ├── snapshot/   ← immutable data files
 * ├── runtime/    ← mutable working files (allocator, tmp, …)
 * └── wal/        ← write-ahead log files
 * ```
 *
 * # Thread safety
 *
 * All public methods are individually safe to invoke concurrently *except*
 * the meta accessors `GetMeta()` / `MutableMeta()`, which return references
 * into a meta_ slot that `UpdateMeta()` may replace.
 *
 * Specifically:
 *   * `OpenFile` / `CreateRuntimeContainer` / `Commit` /
 *     `CreateRuntimeFile` / `CommitRuntimeFile` / `LinkToSnapshot` —
 *     internally lock when allocating or publishing runtime files; otherwise
 *     they only read immutable members (`path_`, `id_`).
 *   * `UpdateMeta` — fully serialized: takes the lock for the entire JSON
 *     write + meta swap + orphan cleanup, so concurrent UpdateMeta calls are
 *     safe and won't tear the on-disk meta file.
 *   * `GetMeta` / `MutableMeta` — *not* internally synchronized; the caller
 *     is responsible for ensuring no concurrent UpdateMeta runs while a
 *     reference is held.
 *
 * The constructor and destructor follow standard C++ object-lifetime rules:
 * the instance must not be in use on another thread while either runs.
 */
class Checkpoint {
 public:
  /**
   * @brief Create and initialize a checkpoint at @p path.
   *
   * Creates directories, loads meta JSON, absolutizes paths, and cleans up
   * orphaned runtime files.  Returns nullptr only on unrecoverable errors
   * (currently throws instead).
   */
  static std::shared_ptr<Checkpoint> Open(std::string path, uint32_t id);

  ~Checkpoint();  // defined in .cc where CheckpointManifest is complete
  Checkpoint(const Checkpoint&) = delete;
  Checkpoint& operator=(const Checkpoint&) = delete;

  /// Root path of this checkpoint: `db_dir/checkpoint-NNNNN`.
  const std::string& path() const { return path_; }

  uint32_t id() const { return id_; }

  std::string snapshot_dir() const {
    assert(!IsEmpty());
    return path_ + "/snapshot";
  }

  std::string runtime_dir() const {
    assert(!IsEmpty());
    return path_ + "/runtime";
  }

  std::string wal_dir() const {
    assert(!IsEmpty());
    return path_ + "/wal";
  }

  std::string meta_path() const {
    assert(!IsEmpty());
    return path_ + "/meta";
  }

  std::string allocator_dir() const {
    assert(!IsEmpty());
    return path_ + "/allocator";
  }

  std::shared_ptr<IDataContainer> OpenFile(const std::string& file_path,
                                           MemoryLevel level) {
    return file_mgr_->OpenFile(file_path, level);
  }

  std::shared_ptr<IDataContainer> CreateRuntimeContainer(size_t size,
                                                         MemoryLevel level) {
    return file_mgr_->CreateRuntimeContainer(size, level);
  }

  /// Commit a container to the snapshot directory. This consumes and closes the
  /// container; callers must not access it after Commit() returns.
  std::string Commit(IDataContainer& buffer) {
    return file_mgr_->Commit(buffer);
  }

  void UpdateMeta(CheckpointManifest&& meta);

  const CheckpointManifest& GetMeta() const {
    assert(meta_ != nullptr);
    return *meta_;
  }

  CheckpointManifest& MutableMeta() {
    assert(meta_ != nullptr);
    return *meta_;
  }

  bool IsEmpty() const { return path_.empty(); }

  /// Allocate an anonymous runtime file handle (RAII).
  CheckpointFileManager::RuntimeFileHandle CreateRuntimeFile() {
    return file_mgr_->CreateRuntimeFile();
  }

  /// Move a runtime file into the snapshot directory and return its path.
  std::string CommitRuntimeFile(
      CheckpointFileManager::RuntimeFileHandle&& file) {
    return file_mgr_->CommitRuntimeFile(std::move(file));
  }

  std::string LinkToSnapshot(const std::string& abs_path) {
    return file_mgr_->LinkToSnapshot(abs_path);
  }

 private:
  /// Private constructor — only initializes members. Use Open() to create.
  Checkpoint(std::string path, uint32_t id);

  /// Performs all I/O: create dirs, load meta, absolutize paths, clean orphans.
  void initialize();

  void create_dirs() const;

  std::string path_;
  uint32_t id_;
  mutable std::mutex mutex_;
  std::unique_ptr<CheckpointManifest> meta_;
  std::unique_ptr<CheckpointFileManager> file_mgr_;
};

}  // namespace neug
