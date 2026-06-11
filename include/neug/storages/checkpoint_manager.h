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
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "neug/storages/checkpoint.h"

namespace neug {

// ---------------------------------------------------------------------------

/// Sentinel returned by CheckpointManager::HeadId() when no checkpoint exists.
constexpr int32_t kInvalidCheckpointId = -1;

/**
 * @brief Manages a database directory that holds multiple numbered checkpoints.
 *
 * Directory layout:
 * ```
 * db_dir/
 * ├── checkpoint-00001/
 * │   ├── meta
 * │   ├── snapshot/
 * │   ├── runtime/
 * │   └── wal/
 * ├── checkpoint-00002/
 * └── ...
 * ```
 *
 * `CheckpointManager` does **not** inherit `Module`; it is a directory-level
 * manager, not a data module itself.
 *
 * Thread safety: All public methods are individually thread-safe (guarded by
 * an internal mutex).  Compound operations (e.g. HeadId() followed by
 * GetCheckpoint(id)) are not atomic — callers that race CreateCheckpoint() /
 * Close() must coordinate externally.
 */
class CheckpointManager {
 public:
  CheckpointManager();
  ~CheckpointManager();

  /**
   * @brief Open a database directory.
   * @param db_dir Path to the database directory
   */
  void Open(const std::string& db_dir);

  /**
   * @brief Close the workspace and release resources.
   */
  void Close();

  /**
   * @brief Get the number of checkpoints in the workspace.
   */
  size_t NumCheckpoints() const;

  /**
   * @brief Get the ID of the most recent checkpoint.
   * @return kInvalidCheckpointId (-1) if no checkpoints exist.
   */
  int32_t HeadId() const;

  /**
   * @brief Create a new checkpoint.
   * @return The ID of the new checkpoint
   */
  int32_t CreateCheckpoint();

  /**
   * @brief Remove a checkpoint by ID.
   *
   * Removes the checkpoint from the in-memory map and deletes its directory
   * from disk.  No-op if @p id is not found.
   */
  void RemoveCheckpoint(int32_t id);

  /**
   * @brief Get a checkpoint by ID.
   */
  std::shared_ptr<Checkpoint> GetCheckpoint(int32_t id) const;

  std::string db_dir() const { return db_dir_; }

 private:
  std::string db_dir_;
  std::map<int32_t, std::shared_ptr<Checkpoint>> checkpoints_;
  mutable std::mutex mutex_;
};

}  // namespace neug
