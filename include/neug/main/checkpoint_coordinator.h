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
 * PublishManualCheckpoint() for the entire graph and runtime-state rotation.
 * The coordinator promotes that lease to the exclusive commit phase before
 * starting maintenance.
 *
 * Two extension points run after the graph reopens from a published
 * checkpoint, before retired generations are reclaimed and before new
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
 * - WalEpochActivationHandler (optional, two independent slots): invoked on
 *   manual and incremental publication paths. The database-owned slot rotates
 *   state owned by NeugDB itself (the embedded AP WAL writer); the
 *   service-owned slot activates service state such as execution-slot WAL
 *   rotation for the published checkpoint.
 *
 * Shutdown checkpoints invoke neither, because they do not reopen the graph.
 */
class CheckpointCoordinator {
 public:
  /// Mandatory database-level rotation onto the published checkpoint
  /// generation. Invoked with the generation's allocator directory.
  using PostReopenHandler =
      std::function<void(const std::string& checkpoint_allocator_dir)>;

  /// Optional service-owned state activation after manual or incremental
  /// checkpoint publication. A publisher with live WAL writers must register
  /// the handler that owns those writers before a non-no-op incremental
  /// publication.
  /// Invoked with the published checkpoint's WAL directory.
  using WalEpochActivationHandler =
      std::function<void(const std::string& checkpoint_wal_dir)>;

  CheckpointCoordinator(CheckpointManager& checkpoint_manager,
                        GraphSnapshotStore& snapshot_store,
                        MemoryLevel memory_level,
                        PostReopenHandler post_reopen_handler);

  /// Database-owned WAL epoch activation invoked on manual and incremental
  /// publication paths before the service-owned handler. NeugDB registers it
  /// to rotate database-owned writers (the embedded AP writer) in place;
  /// NeugDBService layers pool rotation on top via its own handler. The
  /// registration and waiting semantics match the service-owned slot below.
  void SetDatabaseWalEpochActivationHandler(WalEpochActivationHandler handler);
  void ClearDatabaseWalEpochActivationHandler();

  /// Set the single service-owned activation handler invoked by
  /// PublishManualCheckpoint() and PublishIncrementalCheckpoint() before
  /// retired checkpoint roots are reclaimed or the transaction gate is
  /// reopened. Setting a second handler without
  /// first clearing the existing one is an error.
  ///
  /// The service sets the handler at startup and clears it before destroying
  /// handler-owned state. ClearWalEpochActivationHandler() waits for an
  /// in-flight invocation to finish, so no invocation can outlive that call.
  /// The handler must not call SetWalEpochActivationHandler() or
  /// ClearWalEpochActivationHandler().
  void SetWalEpochActivationHandler(WalEpochActivationHandler handler);
  void ClearWalEpochActivationHandler();

  /// Publish a full manual checkpoint, reopen the live graph and allocators,
  /// rotate the service WAL epoch, and reset the transaction timeline. The
  /// caller transfers an active update lease that has not entered the commit
  /// phase and must not hold an ordinary snapshot pin.
  Status PublishManualCheckpoint(UpdateTimestampLease timestamp_lease);

  /// Publish a non-compacting checkpoint for an already-mutated live graph.
  /// The caller transfers an active update lease; this method drains readers
  /// before mutating the live snapshot. Only dirty modules are dumped and
  /// reopened; the allocator and transaction timeline remain active. A clean
  /// graph is a no-op and does not rotate a WAL epoch.
  Status PublishIncrementalCheckpoint(UpdateTimestampLease timestamp_lease);

  /// Mark that the live graph may contain in-place mutations not covered by
  /// WAL. The mark is set before mutation begins and is cleared only after a
  /// successful incremental or full checkpoint.
  void MarkIncrementalCheckpointPending() noexcept {
    incremental_checkpoint_pending_.store(true, std::memory_order_release);
  }

  bool HasIncrementalCheckpointPending() const noexcept {
    return incremental_checkpoint_pending_.load(std::memory_order_acquire);
  }

  /// Publish a recovery checkpoint and reopen the live graph.
  Status PublishRecoveryCheckpoint();

  /// Publish the shutdown checkpoint without reopening the live graph.
  Status PublishShutdownCheckpoint();

 private:
  enum class Reason {
    kManual,
    kRecovery,
    kShutdown,
  };

  Status execute(Reason reason);
  void invokeWalEpochActivationHandler(const std::string& checkpoint_wal_dir);
  static const char* reasonName(Reason reason);

  CheckpointManager& checkpoint_manager_;
  GraphSnapshotStore& snapshot_store_;
  MemoryLevel memory_level_;
  PostReopenHandler post_reopen_handler_;
  std::mutex wal_epoch_activation_handler_mutex_;
  WalEpochActivationHandler database_wal_epoch_handler_;
  WalEpochActivationHandler wal_epoch_activation_handler_;
  std::atomic<bool> incremental_checkpoint_pending_{false};
};

}  // namespace neug
