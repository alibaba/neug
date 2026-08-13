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

#include <atomic>
#include <functional>
#include <mutex>
#include <string>

#include "neug/config.h"
#include "neug/utils/result.h"

namespace neug {

class CheckpointManager;
class GraphSnapshotStore;
class UpdateTimestampLease;

/**
 * Owns the database-level checkpoint build and live graph reopen protocol.
 *
 * NeugDB owns one coordinator for the lifetime of an open database. Manual
 * checkpoint callers transfer an UpdateTimestampLease to
 * PublishManualCheckpoint()
 * for the entire graph and runtime-state rotation. The coordinator promotes
 * that lease to the exclusive commit phase before starting maintenance.
 *
 * Runtime handlers run after checkpoint publication and before new
 * transactions are allowed to start:
 *
 * - PostReopenHandler (mandatory): injected at construction by the database
 *   owner and invoked on EVERY reopen (manual and recovery checkpoints). It
 *   carries correctness-critical database state activation such as reopening
 *   allocators. It runs only after a full graph reopen and must be infallible:
 *   it runs after the old live graph has been consumed. A manual-path failure
 *   terminates the live process; a recovery-path failure aborts database open.
 *
 * - WalRotationHandler (mandatory): invoked after every live checkpoint
 *   publication. Incremental checkpoints use this handler without reopening
 *   the graph or allocator.
 *
 * - CheckpointActivationHandler (optional): invoked on the manual path after
 *   both mandatory handlers for non-database extensions. DB-owned allocators,
 *   execution slots, and WAL writers must not depend on this callback.
 *
 * Shutdown checkpoints invoke none of these handlers because the database is
 * closing.
 */
class CheckpointCoordinator {
 public:
  /// Mandatory database-level rotation onto the published checkpoint
  /// generation. Invoked with the generation's allocator directory.
  using PostReopenHandler =
      std::function<void(const std::string& checkpoint_allocator_dir)>;

  /// Mandatory WAL epoch rotation after a live checkpoint publication.
  using WalRotationHandler =
      std::function<void(const std::string& checkpoint_wal_dir)>;

  /// Optional service-owned state activation for the manual path.
  /// Invoked with the published checkpoint's WAL directory.
  using CheckpointActivationHandler =
      std::function<void(const std::string& checkpoint_wal_uri)>;

  CheckpointCoordinator(CheckpointManager& checkpoint_manager,
                        GraphSnapshotStore& snapshot_store,
                        MemoryLevel memory_level,
                        PostReopenHandler post_reopen_handler,
                        WalRotationHandler wal_rotation_handler);

  /// Set the single activation handler invoked by PublishManualCheckpoint()
  /// after both mandatory handlers and before retired generations are reclaimed
  /// or the transaction gate is reopened. Setting a second handler without
  /// first clearing the existing one is an error.
  ///
  /// The service sets the handler at startup and clears it before destroying
  /// handler-owned state. ClearActivationHandler() waits for an in-flight
  /// invocation to finish, so no invocation can outlive that call. The handler
  /// must not call SetActivationHandler() or ClearActivationHandler().
  void SetActivationHandler(CheckpointActivationHandler handler);
  void ClearActivationHandler();

  /// Publish a manual checkpoint and activate database- and optional
  /// service-owned runtime state through the configured handlers. The caller
  /// transfers an active update lease that has not entered the commit phase
  /// and must not hold an ordinary snapshot pin.
  Status PublishManualCheckpoint(UpdateTimestampLease timestamp_lease);

  /// Publish dirty storage through non-consuming module snapshots while the
  /// transferred update lease owns fully drained exclusive admission. The live
  /// graph, allocator, view, and visibility timeline are not reopened or reset.
  Status PublishIncrementalCheckpoint(UpdateTimestampLease timestamp_lease);

  /// Publish a recovery checkpoint and reopen the live graph.
  Status PublishRecoveryCheckpoint();

  /// Publish the shutdown checkpoint without reopening the live graph.
  Status PublishShutdownCheckpoint();

  /// Mark current-graph mutation that has no corresponding WAL record.
  void MarkUnloggedMutation() noexcept {
    unlogged_mutation_pending_.store(true, std::memory_order_release);
  }

  bool UnloggedMutationPending() const noexcept {
    return unlogged_mutation_pending_.load(std::memory_order_acquire);
  }

  /// Clear a conservative mark after the caller verifies there are no changes
  /// to checkpoint. Successful checkpoint publication clears it internally.
  void ClearUnloggedMutationIfNoChanges() noexcept {
    unlogged_mutation_pending_.store(false, std::memory_order_release);
  }

 private:
  enum class Reason {
    kManual,
    kRecovery,
    kShutdown,
  };

  Status execute(Reason reason);
  void invokeActivationHandler(const std::string& checkpoint_wal_uri);
  static const char* reasonName(Reason reason);

  CheckpointManager& checkpoint_manager_;
  GraphSnapshotStore& snapshot_store_;
  MemoryLevel memory_level_;
  PostReopenHandler post_reopen_handler_;
  WalRotationHandler wal_rotation_handler_;
  std::mutex activation_handler_mutex_;
  CheckpointActivationHandler activation_handler_;
  std::atomic<bool> unlogged_mutation_pending_{false};
};

}  // namespace neug
