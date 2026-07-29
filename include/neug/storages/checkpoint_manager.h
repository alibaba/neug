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

/**
 * @brief Manages the published checkpoint directories for a database.
 *
 * Directory layout:
 * ```
 * db_dir/
 * |-- checkpoint-N/       # published checkpoint
 * `-- checkpoint-(N+1).next/  # unpublished staging checkpoint
 * ```
 *
 * `CheckpointManager` does **not** inherit `Module`; it is a directory-level
 * manager, not a data module itself.
 *
 * The current checkpoint is the highest valid published generation. Committing
 * a staging checkpoint publishes the next generation but leaves the retired
 * checkpoint directory on disk until the caller releases graph/container
 * resources that may still reference it.
 *
 * Thread safety: All public methods are individually thread-safe (guarded by
 * an internal mutex). Callers that race CreateStagingCheckpoint() / Close()
 * must coordinate externally.
 */
class CheckpointManager {
 public:
  class StagingCheckpoint {
   public:
    ~StagingCheckpoint();

    StagingCheckpoint(StagingCheckpoint&& other) noexcept;
    StagingCheckpoint(const StagingCheckpoint&) = delete;
    StagingCheckpoint& operator=(const StagingCheckpoint&) = delete;

    std::shared_ptr<Checkpoint> checkpoint() const;
    /// Publish this staging checkpoint as the current checkpoint. If
    /// previous_checkpoint_path is non-null, it receives the retired checkpoint
    /// directory path. The caller decides when that directory is safe to
    /// delete.
    std::shared_ptr<Checkpoint> Commit(
        std::string* previous_checkpoint_path = nullptr);
    void Discard() noexcept;

   private:
    friend class CheckpointManager;

    StagingCheckpoint(CheckpointManager& manager,
                      std::shared_ptr<Checkpoint> checkpoint);
    void release();

    CheckpointManager& manager_;
    std::shared_ptr<Checkpoint> checkpoint_;
  };

  /**
   * @brief Open a database directory.
   * @param db_dir Path to the database directory.
   * @param recover_workspace Whether to remove stale staging directories and
   * invalid published checkpoints while opening.
   */
  void Open(const std::string& db_dir, bool recover_workspace = true);

  /**
   * @brief Close the workspace and release resources.
   */
  void Close();

  /// Return true if a published checkpoint is currently open.
  bool HasCurrentCheckpoint() const;

  /**
   * @brief Get the current published checkpoint, or nullptr if none exists.
   */
  std::shared_ptr<Checkpoint> CurrentCheckpoint() const;

  /**
   * @brief Create a new unpublished staging checkpoint.
   * @return Move-only handle that discards the staging checkpoint on scope
   * exit.
   */
  StagingCheckpoint CreateStagingCheckpoint();

  /**
   * @brief Restore the in-memory current checkpoint to an older published
   * checkpoint.
   *
   * Used by DB-level checkpoint creation after it removes a newer published
   * checkpoint whose graph reopen failed.
   */
  void RestoreCurrentCheckpoint(std::shared_ptr<Checkpoint> checkpoint);

  /**
   * @brief Best-effort cleanup of one published checkpoint that is not current.
   *
   * Used by DB-level rollback after it restores the current checkpoint to a
   * previous generation. Refuses to remove the current checkpoint.
   */
  void CleanupPublishedCheckpoint(std::shared_ptr<Checkpoint> checkpoint);

  /**
   * @brief Best-effort cleanup of published checkpoints other than current.
   *
   * The caller must only invoke this after all graph/container resources that
   * might reference retired checkpoint directories have been released.
   */
  void CleanupRetiredCheckpoints();

  std::string db_dir() const;

 private:
  std::shared_ptr<Checkpoint> CommitStagingCheckpoint(
      StagingCheckpoint& staging, std::string* previous_checkpoint_path);
  void DiscardStagingCheckpoint(StagingCheckpoint& staging) noexcept;

  std::string db_dir_;
  std::shared_ptr<Checkpoint> current_checkpoint_;
  std::shared_ptr<Checkpoint> staging_checkpoint_;
  int32_t current_generation_ = -1;
  mutable std::mutex mutex_;
};

}  // namespace neug
