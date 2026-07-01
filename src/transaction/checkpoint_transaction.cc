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

#include "neug/transaction/checkpoint_transaction.h"

#include <glog/logging.h>
#include <utility>

#include "neug/storages/checkpoint_session.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/wal_manager.h"

namespace neug {

CheckpointTransaction::CheckpointTransaction(CheckpointManager& checkpoint_mgr,
                                             GraphSnapshotStore& snapshot_store,
                                             WalManager& wal_manager,
                                             IVersionManager& vm,
                                             timestamp_t timestamp)
    : guard_(snapshot_store),
      checkpoint_graph_(guard_.get().mutable_graph()->Clone()),
      checkpoint_view_(*checkpoint_graph_),
      checkpoint_session_(
          CheckpointSession::Begin(checkpoint_mgr, snapshot_store,
                                   /*reopen=*/true, *checkpoint_graph_)),
      wal_manager_(wal_manager),
      pending_wal_rotation_(
          wal_manager_.PrepareRotation(checkpoint_session_.staging_wal_dir())),
      prepared_final_wal_dir_(pending_wal_rotation_.final_wal_dir()),
      vm_(vm),
      timestamp_(timestamp) {}

CheckpointTransaction::~CheckpointTransaction() { Abort(); }

timestamp_t CheckpointTransaction::timestamp() const { return timestamp_; }

const Schema& CheckpointTransaction::schema() const {
  return checkpoint_graph_->schema();
}

PropertyGraph& CheckpointTransaction::graph() { return *checkpoint_graph_; }

GraphView& CheckpointTransaction::view() { return checkpoint_view_; }

CheckpointSession& CheckpointTransaction::checkpoint_session() {
  return checkpoint_session_;
}

bool CheckpointTransaction::Commit() {
  if (timestamp_ == INVALID_TIMESTAMP) {
    return true;
  }

  // Phase 1: pre-publish. Release the snapshot guard and publish the
  // checkpoint.  If anything fails here the checkpoint was never made
  // visible, so Abort() (which rolls back the staging session and reverts
  // the compact timestamp) is still safe.
  try {
    guard_.release();
    checkpoint_session_.Commit();
    post_publish_ = true;
  } catch (const std::exception& e) {
    LOG(ERROR) << "Checkpoint transaction commit failed: " << e.what();
    Abort();
    return false;
  } catch (...) {
    LOG(ERROR) << "Checkpoint transaction commit failed.";
    Abort();
    return false;
  }

  // Phase 2: post-publish. The checkpoint is already published and durable.
  // From here on, failures must not fall back to Abort(), because that would
  // revert a compact/checkpoint timestamp whose changes are already visible.
  wal_manager_.InstallPreparedRotation(std::move(pending_wal_rotation_),
                                       prepared_final_wal_dir_);
  try {
    vm_.release_checkpoint_timestamp(timestamp_);
  } catch (const std::exception& e) {
    LOG(FATAL) << "Checkpoint transaction failed to release timestamp after "
                  "publish: "
               << e.what();
  } catch (...) {
    LOG(FATAL) << "Checkpoint transaction failed to release timestamp after "
                  "publish.";
  }
  timestamp_ = INVALID_TIMESTAMP;
  checkpoint_session_.CleanupPreviousCheckpoint();
  prepared_final_wal_dir_.clear();
  checkpoint_view_ = GraphView();
  checkpoint_graph_.reset();
  return true;
}

void CheckpointTransaction::Abort() {
  if (timestamp_ != INVALID_TIMESTAMP) {
    if (!post_publish_) {
      wal_manager_.AbortPreparedRotation(std::move(pending_wal_rotation_));
      checkpoint_session_.Rollback();
      guard_.release();
      vm_.revert_compact_timestamp(timestamp_);
    } else {
      LOG(ERROR) << "Checkpoint transaction abort requested after publish; "
                    "checkpoint cannot be rolled back.";
    }
    timestamp_ = INVALID_TIMESTAMP;
  }
  prepared_final_wal_dir_.clear();
  checkpoint_view_ = GraphView();
  checkpoint_graph_.reset();
}

}  // namespace neug
