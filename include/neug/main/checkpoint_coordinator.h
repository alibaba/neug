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

#include <functional>
#include <string>

#include "neug/config.h"
#include "neug/utils/result.h"

namespace neug {

class CheckpointManager;
class CurrentCowWriteTransaction;
class GraphSnapshotStore;
class UpdateTimestampLease;

/**
 * Owns the database-level checkpoint build and live graph reopen protocol.
 *
 * NeugDB owns one coordinator for the lifetime of an open database. Manual
 * checkpoint callers transfer an UpdateTimestampLease to
 * PublishManualCheckpoint() for the entire graph and runtime-state rotation.
 * The coordinator promotes that lease to the exclusive commit phase before
 * starting maintenance.
 *
 * Two activation hooks run after a checkpoint is published and before new
 * transactions are allowed to start:
 *
 * - PostReopenHandler (mandatory): injected at construction by the database
 * owner and invoked on every reopen (manual and recovery checkpoints). It
 * carries
 * correctness-critical database state activation such as reopening allocators
 * and invalidating the compiled-plan cache. It must be infallible: it runs
 * after the old live graph has been consumed. A manual-path failure terminates
 * the live process; a recovery-path failure aborts database open.
 *
 * - WalEpochActivationHandler (optional): injected by the database owner and
 *   invoked on manual and bulk-checkpoint publication paths. It rotates every
 *   currently active database-owned WAL writer onto the published epoch.
 *
 * Shutdown checkpoints invoke neither, because they do not reopen the graph.
 */
class CheckpointCoordinator {
 public:
  /// Mandatory database-level rotation onto the published checkpoint
  /// generation. Invoked with the generation's allocator directory.
  using PostReopenHandler =
      std::function<void(const std::string& checkpoint_allocator_dir)>;

  /// Optional database-owned state activation after manual or bulk
  /// checkpoint publication. A publisher with live WAL writers must register
  /// the handler that owns those writers before publishing a checkpoint.
  /// Invoked with the published checkpoint's WAL directory.
  using WalEpochActivationHandler =
      std::function<void(const std::string& checkpoint_wal_dir)>;

  CheckpointCoordinator(
      CheckpointManager& checkpoint_manager, GraphSnapshotStore& snapshot_store,
      MemoryLevel memory_level, PostReopenHandler post_reopen_handler,
      WalEpochActivationHandler wal_epoch_activation_handler = {});

  /// Publish a full manual checkpoint, reopen the live graph and allocators,
  /// rotate the service WAL epoch, and reset the transaction timeline. The
  /// caller transfers an active update lease that has not entered the commit
  /// phase and must not hold an ordinary snapshot pin.
  Status PublishManualCheckpoint(UpdateTimestampLease timestamp_lease);

  /// Commit a private bulk COW transaction through a checkpoint. Unlike a
  /// manual checkpoint, this does not maintain or reopen the live graph: it
  /// publishes the transaction's private graph, then installs it as current.
  /// Validation and staging failures abort the private workspace. Once the
  /// consuming dirty-module dump starts, failures are fail-stop because some
  /// payload containers may still be shared with the published base graph.
  /// Manifest publication remains the durable decision point.
  Status CommitCowWrite(CurrentCowWriteTransaction& transaction);

  /// Publish a recovery checkpoint and reopen the live graph.
  Status PublishRecoveryCheckpoint();

  /// Publish the shutdown checkpoint without reopening the live graph.
  /// @p live_graph_consumption_started is reset on entry and set before the
  /// live graph is compacted or consumed. A failure after that point is not
  /// retryable on the same open database.
  Status PublishShutdownCheckpoint(bool& live_graph_consumption_started);

 private:
  enum class Reason {
    kManual,
    kRecovery,
    kShutdown,
  };

  Status execute(Reason reason,
                 bool* live_graph_consumption_started_out = nullptr);
  void invokeWalEpochActivationHandler(const std::string& checkpoint_wal_dir);
  static const char* reasonName(Reason reason);

  CheckpointManager& checkpoint_manager_;
  GraphSnapshotStore& snapshot_store_;
  MemoryLevel memory_level_;
  PostReopenHandler post_reopen_handler_;
  WalEpochActivationHandler wal_epoch_activation_handler_;
};

}  // namespace neug
