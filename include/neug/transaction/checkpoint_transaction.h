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

#include <string>

#include "neug/storages/checkpoint_session.h"
#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/wal/wal_manager.h"
#include "neug/utils/property/types.h"

namespace neug {

class CheckpointManager;
class IVersionManager;
class Schema;

/**
 * @brief Transaction for creating a durable checkpoint with WAL rotation.
 *
 * This is the TP (service-mode) checkpoint path. On Commit(), it:
 *   1. Publishes the staging checkpoint as the current checkpoint.
 *   2. Installs the prepared WAL rotation to the new checkpoint's wal/
 *      directory.
 *   3. GCs the previous checkpoint directory.
 *   4. Resets the VersionManager timestamps to zero.
 *
 * @note **Breaking change**: This class is new in the checkpoint GC refactor.
 * It replaces the old StorageTPUpdateInterface::CreateCheckpoint() path.
 */
class CheckpointTransaction {
 public:
  // Uses the same exclusive timestamp as compaction. Force compaction may run
  // before the new checkpoint is published; rollback restores durability/root
  // pointer state, not byte-for-byte in-memory container contents.
  CheckpointTransaction(CheckpointManager& checkpoint_mgr,
                        GraphSnapshotStore& snapshot_store,
                        WalManager& wal_manager, IVersionManager& vm,
                        timestamp_t timestamp);
  ~CheckpointTransaction();

  timestamp_t timestamp() const;
  const Schema& schema() const;
  PropertyGraph& graph();
  GraphView& view();
  /// Exposed only for execution-layer CHECKPOINT dump. The transaction owns
  /// commit, rollback, and cleanup.
  CheckpointSession& checkpoint_session();

  bool Commit();

  void Abort();

 private:
  SnapshotGuard guard_;
  std::shared_ptr<PropertyGraph> checkpoint_graph_;
  GraphView checkpoint_view_;
  CheckpointSession checkpoint_session_;
  WalManager& wal_manager_;
  PendingWalRotation pending_wal_rotation_;
  std::string prepared_final_wal_dir_;
  IVersionManager& vm_;
  timestamp_t timestamp_;
  bool post_publish_ = false;
};

}  // namespace neug
