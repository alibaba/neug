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
#include <vector>

#include "neug/storages/checkpoint.h"
#include "neug/utils/api.h"

namespace neug {

/**
 * @brief Owns the database CURRENT selector and checkpoint publication.
 *
 * `checkpoint/CURRENT` is the only normal publication selector. A staging
 * handle may create unreachable immutable objects, a manifest, and its empty
 * WAL epoch, but it is not visible until Publish() durably replaces CURRENT.
 */
class NEUG_API CheckpointManager {
 public:
  class StagingCheckpoint {
   public:
    ~StagingCheckpoint();

    StagingCheckpoint(StagingCheckpoint&& other) noexcept;
    StagingCheckpoint(const StagingCheckpoint&) = delete;
    StagingCheckpoint& operator=(const StagingCheckpoint&) = delete;

    std::shared_ptr<Checkpoint> checkpoint() const;
    /// Make this manifest visible by durably replacing CURRENT.
    std::shared_ptr<Checkpoint> Publish();
    void Discard() noexcept;

   private:
    friend class CheckpointManager;

    StagingCheckpoint(CheckpointManager& manager,
                      std::shared_ptr<Checkpoint> checkpoint);
    void release();

    CheckpointManager& manager_;
    std::shared_ptr<Checkpoint> checkpoint_;
  };

  /// Open @p database_dir. A writer open automatically migrates the newest
  /// valid legacy checkpoint-N v1 generation when CURRENT is absent. A
  /// read-only open never migrates legacy data.
  void Open(const std::string& database_dir, bool create_if_missing = true);
  /// Release manager-owned references. The runtime workspace is reclaimed
  /// after its last checkpoint, container, or runtime-file handle is released.
  void Close();

  /// Return the checkpoint selected by CURRENT, or nullptr before first
  /// publication.
  std::shared_ptr<Checkpoint> Current() const;

  /// Create an unpublished staging checkpoint. Destroying its move-only handle
  /// discards it unless Publish() has made its manifest visible.
  StagingCheckpoint CreateStaging();

  /// Reclaim manifests, WAL epochs, and immutable objects not held by the
  /// current checkpoint or a live Checkpoint shared_ptr, plus runtime
  /// workspaces abandoned by earlier processes. The caller must hold exclusive
  /// database-writer ownership; the current open epoch is always preserved.
  void CollectGarbage();

  std::string database_dir() const;

 private:
  std::shared_ptr<Checkpoint> PublishStagingCheckpoint(
      StagingCheckpoint& staging);
  void DiscardStagingCheckpoint(StagingCheckpoint& staging) noexcept;
  StagingCheckpoint CreateStagingLocked(uint64_t id);

  std::string database_dir_;
  std::shared_ptr<const RuntimeWorkspace> runtime_workspace_;
  std::shared_ptr<Checkpoint> current_checkpoint_;
  std::shared_ptr<Checkpoint> staging_checkpoint_;
  std::vector<std::weak_ptr<Checkpoint>> published_checkpoints_;
  mutable std::mutex mutex_;
};

}  // namespace neug
