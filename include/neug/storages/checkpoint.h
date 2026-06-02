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
#include <filesystem>
#include <memory>
#include <mutex>
#include <set>
#include <string>

#include "neug/storages/container/container_utils.h"
#include "neug/storages/container/i_container.h"
#include "neug/storages/module_descriptor.h"
#include "neug/utils/uuid.h"

namespace neug {

class SnapshotMeta;  // forward declaration to break circular dependency

/**
 * @brief Represents a single numbered checkpoint directory.
 *
 * A checkpoint lives at `db_dir/checkpoint-NNNNN/` and contains:
 *
 * ```
 * checkpoint-NNNNN/
 * ├── meta        ← SnapshotMeta JSON
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
 *   * `OpenFile` / `CreateRuntimeContainer` / `Commit` / `CommitRuntimeObject`
 *     / `CommitToSnapshot` / `LinkToSnapshot` / `CreateRuntimeObject` —
 *     internally lock when mutating `uncommitted_runtime_objects_` or
 *     touching `meta_`; otherwise they only read immutable members
 *     (`path_`, `id_`).
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

  ~Checkpoint();  // defined in .cc where SnapshotMeta is complete
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

  std::unique_ptr<IDataContainer> OpenFile(const std::string& file_path,
                                           MemoryLevel level);

  std::unique_ptr<IDataContainer> CreateRuntimeContainer(size_t size,
                                                         MemoryLevel level);

  /**
   * @brief Commit a data container to a persistent snapshot file and return
   * a ModuleDescriptor whose `path` points to the written file.
   *
   * For MAP_SHARED containers (kSyncToFile) the backing file is already on
   * disk – we just Sync() and record its path.  For all other container
   * types the data is written to a freshly generated UUID file under
   * runtime_dir().
   */
  std::string Commit(IDataContainer& buffer);

  void UpdateMeta(SnapshotMeta&& meta);

  const SnapshotMeta& GetMeta() const {
    assert(meta_ != nullptr);
    return *meta_;
  }

  SnapshotMeta& MutableMeta() {
    assert(meta_ != nullptr);
    return *meta_;
  }

  bool IsEmpty() const { return path_.empty(); }

  std::string CreateRuntimeObject();

  /**
   * @brief Commit a runtime object into this checkpoint's persistent snapshot
   * state.
   *
   * Given the UUID of a file created under runtime_dir(), this finalizes the
   * object via the normal snapshot commit path. In particular, runtime files
   * may be moved into snapshot_dir() as part of the commit process, so callers
   * must not assume the file remains in runtime_dir() after this call.
   *
   * Returns the full absolute path to the committed file.
   */
  std::string CommitRuntimeObject(const std::string& uuid);

  // Create a hardlink of abs_path inside snapshot_dir (O(1), zero data copy).
  // Falls back to file copy on cross-device filesystems.
  // If abs_path is already inside snapshot_dir, returns it unchanged.
  std::string LinkToSnapshot(const std::string& abs_path);

 private:
  /// Private constructor — only initializes members. Use Open() to create.
  Checkpoint(std::string path, uint32_t id);

  /// Performs all I/O: create dirs, load meta, absolutize paths, clean orphans.
  void initialize();

  void create_dirs() const;
  std::string commitToSnapshotLocked(const std::string& abs_path);
  std::string makeRelativePath(const std::string& abs_path) const;
  std::string resolveAbsolutePath(const std::string& rel_path) const;
  std::string CommitToSnapshot(const std::string& abs_path);

  std::string path_;
  uint32_t id_;
  mutable std::mutex mutex_;
  std::unique_ptr<SnapshotMeta> meta_;
  std::set<std::string> uncommitted_runtime_objects_;
};

}  // namespace neug
