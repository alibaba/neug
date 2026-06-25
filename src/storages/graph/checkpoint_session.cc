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

#include "neug/storages/checkpoint_session.h"

#include <glog/logging.h>
#include <filesystem>
#include <utility>

#include "neug/storages/checkpoint.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/utils/exception/exception.h"

namespace neug {

CheckpointSession::CheckpointSession(
    CheckpointManager& checkpoint_mgr, GraphSnapshotStore& snapshot_store,
    int32_t checkpoint_id, std::shared_ptr<Checkpoint> old_checkpoint,
    std::shared_ptr<Checkpoint> staging_checkpoint, bool reopen,
    MemoryLevel memory_level)
    : checkpoint_mgr_(&checkpoint_mgr),
      snapshot_store_(&snapshot_store),
      checkpoint_id_(checkpoint_id),
      old_checkpoint_(std::move(old_checkpoint)),
      staging_checkpoint_(std::move(staging_checkpoint)),
      reopen_(reopen),
      memory_level_(memory_level),
      state_(State::kStaging) {}

CheckpointSession::~CheckpointSession() { Rollback(); }

CheckpointSession CheckpointSession::Begin(CheckpointManager& checkpoint_mgr,
                                           GraphSnapshotStore& snapshot_store,
                                           bool reopen,
                                           const PropertyGraph& graph) {
  std::shared_ptr<Checkpoint> old_checkpoint;
  const auto old_current_id = checkpoint_mgr.CurrentCheckpointId();
  if (old_current_id != kInvalidCheckpointId) {
    old_checkpoint = checkpoint_mgr.GetCheckpoint(old_current_id);
  }

  const auto checkpoint_id = checkpoint_mgr.CreateStagingCheckpoint();
  auto staging_checkpoint = checkpoint_mgr.GetCheckpoint(checkpoint_id);

  return CheckpointSession(
      checkpoint_mgr, snapshot_store, checkpoint_id, std::move(old_checkpoint),
      std::move(staging_checkpoint), reopen, graph.memory_level());
}

Status CheckpointSession::Dump(PropertyGraph& graph) {
  if (state_ != State::kStaging) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Checkpoint session is not staging.");
  }
  if (staging_checkpoint_ == nullptr) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Checkpoint staging target is not set.");
  }

  graph.Compact(MAX_TIMESTAMP);
  graph.DumpToCheckpoint(staging_checkpoint_,
                         /*reopen=*/false,
                         /*cleanup_obsolete_wal=*/false);
  dumped_ = true;
  return Status::OK();
}

std::shared_ptr<Checkpoint> CheckpointSession::Commit() {
  if (state_ == State::kPublished) {
    return published_checkpoint_;
  }
  if (state_ == State::kInactive) {
    THROW_INTERNAL_EXCEPTION("Checkpoint commit on inactive session.");
  }
  try {
    if (checkpoint_mgr_ == nullptr || snapshot_store_ == nullptr) {
      THROW_INTERNAL_EXCEPTION("Invalid checkpoint session.");
    }
    if (!dumped_) {
      THROW_INTERNAL_EXCEPTION("Checkpoint commit without dumping graph.");
    }

    previous_checkpoint_path_.clear();
    try {
      published_checkpoint_ = checkpoint_mgr_->PublishStagingCheckpoint(
          checkpoint_id_, &previous_checkpoint_path_);
      checkpoint_id_ = kInvalidCheckpointId;
    } catch (...) {
      LOG(ERROR) << "Checkpoint publish failed, rolling back checkpoint "
                 << checkpoint_id_;
      throw;
    }

    if (reopen_) {
      try {
        auto reopened_graph = std::make_shared<PropertyGraph>();
        reopened_graph->Open(published_checkpoint_, memory_level_);
        snapshot_store_->ReplaceCurrentSnapshot(std::move(reopened_graph));
      } catch (...) {
        LOG(ERROR) << "Checkpoint reopen failed, rolling back checkpoint "
                   << published_checkpoint_->path();
        auto original_exception = std::current_exception();
        bool restored_old_checkpoint = false;
        if (old_checkpoint_ != nullptr) {
          try {
            checkpoint_mgr_->RestoreCurrentCheckpoint(old_checkpoint_);
            restored_old_checkpoint = true;
          } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to restore previous checkpoint "
                       << old_checkpoint_->path() << ": " << e.what();
          } catch (...) {
            LOG(ERROR) << "Failed to restore previous checkpoint "
                       << old_checkpoint_->path();
          }
        }
        if (restored_old_checkpoint) {
          cleanup_checkpoint_dir(published_checkpoint_->path());
        }
        std::rethrow_exception(original_exception);
      }
    }

    VLOG(1) << "Finish checkpoint: " << published_checkpoint_->path();
    state_ = State::kPublished;
    return published_checkpoint_;
  } catch (...) {
    Rollback();
    throw;
  }
}

std::shared_ptr<Checkpoint> CheckpointSession::CommitAndCleanup() {
  auto checkpoint = Commit();
  CleanupPreviousCheckpoint();
  return checkpoint;
}

void CheckpointSession::CleanupPreviousCheckpoint() {
  if (published_checkpoint_ == nullptr || previous_checkpoint_path_.empty() ||
      previous_checkpoint_path_ == published_checkpoint_->path()) {
    return;
  }
  cleanup_checkpoint_dir(previous_checkpoint_path_);
  previous_checkpoint_path_.clear();
}

void CheckpointSession::Rollback() {
  if (state_ == State::kStaging) {
    rollback_staging();
    state_ = State::kInactive;
  }
}

std::string CheckpointSession::staging_wal_dir() const {
  if (staging_checkpoint_ == nullptr) {
    THROW_INTERNAL_EXCEPTION("Checkpoint session has no staging checkpoint.");
  }
  return staging_checkpoint_->wal_dir();
}

void CheckpointSession::rollback_staging() {
  if (checkpoint_mgr_ == nullptr || checkpoint_id_ == kInvalidCheckpointId) {
    return;
  }
  try {
    checkpoint_mgr_->DiscardStagingCheckpoint(checkpoint_id_);
  } catch (const std::exception& e) {
    LOG(WARNING) << "Failed to discard staging checkpoint " << checkpoint_id_
                 << ": " << e.what();
  } catch (...) {
    LOG(WARNING) << "Failed to discard staging checkpoint " << checkpoint_id_;
  }
  checkpoint_id_ = kInvalidCheckpointId;
}

void CheckpointSession::cleanup_checkpoint_dir(const std::string& path) const {
  if (path.empty()) {
    return;
  }
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    LOG(WARNING) << "CheckpointSession: failed to remove checkpoint " << path
                 << ": " << ec.message();
  }
}

}  // namespace neug
