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
#include <string>

#include "neug/storages/checkpoint_manager.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace neug {

class Checkpoint;
class GraphSnapshotStore;
class PropertyGraph;

class CheckpointSession {
 public:
  static CheckpointSession Begin(CheckpointManager& checkpoint_mgr,
                                 GraphSnapshotStore& snapshot_store,
                                 bool reopen, const PropertyGraph& graph);

  ~CheckpointSession();

  CheckpointSession(const CheckpointSession&) = delete;
  CheckpointSession& operator=(const CheckpointSession&) = delete;
  CheckpointSession(CheckpointSession&& other) = delete;
  CheckpointSession& operator=(CheckpointSession&& other) = delete;

  Status Dump(PropertyGraph& graph);
  std::shared_ptr<Checkpoint> Commit();
  std::shared_ptr<Checkpoint> CommitAndCleanup();
  void CleanupPreviousCheckpoint();
  void Rollback();

  std::string staging_wal_dir() const;

 private:
  enum class State {
    kInactive,
    kStaging,
    kPublished,
  };

  CheckpointSession(CheckpointManager& checkpoint_mgr,
                    GraphSnapshotStore& snapshot_store, int32_t checkpoint_id,
                    std::shared_ptr<Checkpoint> old_checkpoint,
                    std::shared_ptr<Checkpoint> staging_checkpoint, bool reopen,
                    MemoryLevel memory_level);

  void rollback_staging();
  void cleanup_checkpoint_dir(const std::string& path) const;

  CheckpointManager* checkpoint_mgr_ = nullptr;
  GraphSnapshotStore* snapshot_store_ = nullptr;
  int32_t checkpoint_id_ = kInvalidCheckpointId;
  std::shared_ptr<Checkpoint> old_checkpoint_;
  std::shared_ptr<Checkpoint> staging_checkpoint_;
  bool dumped_ = false;
  bool reopen_ = false;
  MemoryLevel memory_level_ = MemoryLevel::kInMemory;
  std::shared_ptr<Checkpoint> published_checkpoint_;
  std::string previous_checkpoint_path_;
  State state_ = State::kInactive;
};

}  // namespace neug
