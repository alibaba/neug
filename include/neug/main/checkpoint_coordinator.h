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
 * - WalEpochActivationHandler (optional): set by the service owner and
 *   invoked on the manual path. It activates
 *   service-owned state such as execution-slot WAL rotation for the published
 *   checkpoint.
 *
 * Shutdown checkpoints invoke neither, because they do not reopen the graph.
 */
class CheckpointCoordinator {
 public:
  /// Mandatory database-level rotation onto the published checkpoint
  /// generation. Invoked with the generation's allocator directory.
  using PostReopenHandler =
      std::function<void(const std::string& checkpoint_allocator_dir)>;

  /// Optional service-owned state activation for the manual path.
  /// Invoked with the published checkpoint's WAL directory.
  using WalEpochActivationHandler =
      std::function<void(const std::string& checkpoint_wal_dir)>;

  CheckpointCoordinator(CheckpointManager& checkpoint_manager,
                        GraphSnapshotStore& snapshot_store,
                        MemoryLevel memory_level,
                        PostReopenHandler post_reopen_handler);

  /// Set the single activation handler invoked by PublishManualCheckpoint()
  /// before retired checkpoint roots are reclaimed or the transaction gate is
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
  WalEpochActivationHandler wal_epoch_activation_handler_;
};

}  // namespace neug
