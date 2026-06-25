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
#include <memory>
#include <mutex>
#include <string>

#include "neug/storages/checkpoint.h"

namespace neug {

// ---------------------------------------------------------------------------

/// Sentinel returned by CheckpointManager::CurrentCheckpointId() when no
/// checkpoint exists.
constexpr int32_t kInvalidCheckpointId = -1;

/**
 * @brief Manages the single durable checkpoint pointer for a database.
 *
 * Directory layout:
 * ```
 * db_dir/
 * |-- CHECKPOINT      # contains the current checkpoint directory name
 * `-- checkpoint-N/
 *     |-- meta
 *     |-- snapshot/
 *     |-- runtime/
 *     `-- wal/
 * ```
 *
 * `CheckpointManager` does **not** inherit `Module`; it is a directory-level
 * manager, not a data module itself.
 *
 * Thread safety: All public methods are individually thread-safe (guarded by
 * an internal mutex).  Compound operations (e.g. CurrentCheckpointId()
 * followed by GetCheckpoint(id)) are not atomic; callers that race
 * CreateStagingCheckpoint() / Close() must coordinate externally.
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

  /// Return true if CHECKPOINT currently points at a published checkpoint.
  bool HasCurrentCheckpoint() const;

  /**
   * @brief Get the ID of the current published checkpoint.
   * @return kInvalidCheckpointId (-1) if no checkpoints exist.
   */
  int32_t CurrentCheckpointId() const;

  /**
   * @brief Create a new unpublished staging checkpoint.
   * @return The ID of the staging checkpoint.
   */
  int32_t CreateStagingCheckpoint();

  /**
   * @brief Atomically publish a staging checkpoint as the current checkpoint.
   *
   * Renames `checkpoint-N.next` to `checkpoint-N`, writes the root CHECKPOINT
   * pointer, and removes the previous current checkpoint directory on a
   * best-effort basis.
   */
  std::shared_ptr<Checkpoint> PublishStagingCheckpoint(int32_t id);

  /**
   * @brief Publish a staging checkpoint and return the previous checkpoint
   * path.
   *
   * When @p obsolete_checkpoint_path is non-null, the old checkpoint directory
   * is not removed by the manager. The caller can delete it after completing
   * any required reopen/rebuild work.
   */
  std::shared_ptr<Checkpoint> PublishStagingCheckpoint(
      int32_t id, std::string* obsolete_checkpoint_path);

  /**
   * @brief Restore CHECKPOINT to an already-published checkpoint.
   *
   * Used by DB-level checkpoint creation to roll back the root pointer if the
   * new checkpoint has been published on disk but reopening the live graph
   * fails.
   */
  void RestoreCurrentCheckpoint(std::shared_ptr<Checkpoint> checkpoint);

  /**
   * @brief Discard an unpublished staging checkpoint by ID.
   */
  void DiscardStagingCheckpoint(int32_t id);

  /**
   * @brief Get a checkpoint by ID.
   */
  std::shared_ptr<Checkpoint> GetCheckpoint(int32_t id) const;

  std::string db_dir() const;

 private:
  std::string db_dir_;
  std::shared_ptr<Checkpoint> current_checkpoint_;
  std::shared_ptr<Checkpoint> staging_checkpoint_;
  int32_t current_id_ = kInvalidCheckpointId;
  mutable std::mutex mutex_;
};

}  // namespace neug
