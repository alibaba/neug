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
#include <memory>
#include <mutex>
#include <string>

#include "neug/config.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/update_timestamp_guard.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace neug {

class CheckpointManager;

/**
 * Owns the database-level checkpoint build and live graph reopen protocol.
 *
 * NeugDB owns one coordinator for the lifetime of an open database. AP callers
 * provide synchronization by holding the exclusive query lock. TP callers
 * transfer an UpdateTimestampGuard to ExecuteTpManual() for the entire graph
 * and runtime-state rotation.
 *
 * Two extension points run after the graph reopens from a published
 * checkpoint, before retired generations are reclaimed and before new
 * transactions are allowed to start:
 *
 * - PostReopenHandler (mandatory): injected at construction by the database
 *   owner and invoked on EVERY reopen (AP manual, TP manual, and recovery
 *   checkpoints). It carries correctness-critical state rotation such as
 *   reopening the allocators onto the published generation, and must be
 *   infallible: it runs in the destructive phase where failure terminates
 *   the process.
 *
 * - CheckpointActivationHandler (optional): set by the service owner and
 *   invoked ONLY on the TP manual path, after the post-reopen handler. It
 *   activates service-owned state such as session WAL rotation and cache
 *   invalidation for the published checkpoint.
 *
 * Shutdown checkpoints invoke neither, because they do not reopen the graph.
 */
class CheckpointCoordinator {
 public:
  /// Mandatory database-level rotation onto the published checkpoint
  /// generation. Invoked with the generation's allocator directory.
  using PostReopenHandler =
      std::function<void(const std::string& checkpoint_allocator_dir)>;

  /// Optional service-owned state activation for the TP manual path.
  /// Invoked with the published checkpoint's WAL directory.
  using CheckpointActivationHandler =
      std::function<void(const std::string& checkpoint_wal_uri)>;

  CheckpointCoordinator(CheckpointManager& checkpoint_manager,
                        GraphSnapshotStore& snapshot_store,
                        MemoryLevel memory_level,
                        timestamp_t recovered_wal_timestamp,
                        PostReopenHandler post_reopen_handler);

  /// Set the single activation handler invoked by ExecuteTpManual() after the
  /// post-reopen handler and before retired generations are reclaimed or the
  /// transaction gate is reopened. Setting a second handler without first
  /// clearing the existing one is an error.
  ///
  /// The service sets the handler at startup and clears it before destroying
  /// handler-owned state. ClearActivationHandler() waits for an in-flight
  /// invocation to finish, so no invocation can outlive that call. The handler
  /// must not call SetActivationHandler() or ClearActivationHandler().
  void SetActivationHandler(CheckpointActivationHandler handler);
  void ClearActivationHandler();

  /// Execute an AP checkpoint. The caller must hold the AP exclusive query
  /// lock and must not hold an ordinary snapshot pin.
  Status ExecuteApManual();

  /// Execute a TP checkpoint and activate database- and service-owned runtime
  /// state through the configured handlers.
  Status ExecuteTpManual(UpdateTimestampGuard timestamp_guard);

  /// Publish a recovery checkpoint and reopen the live graph.
  Status ExecuteRecovery();

  /// Publish the shutdown checkpoint without reopening the live graph.
  Status PrepareShutdown();

  timestamp_t recovered_wal_timestamp() const noexcept {
    return recovered_wal_timestamp_;
  }

 private:
  enum class Reason {
    kManual,
    kRecovery,
    kShutdown,
  };

  Status execute(GraphSnapshotStore::CheckpointMaintenanceHandle& maintenance,
                 timestamp_t compact_timestamp, Reason reason,
                 bool invoke_activation_handler);
  Status executeWithNewMaintenance(timestamp_t compact_timestamp, Reason reason,
                                   bool invoke_activation_handler = false);
  void invokeActivationHandler(const std::string& checkpoint_wal_uri);
  static const char* reasonName(Reason reason);

  CheckpointManager& checkpoint_manager_;
  GraphSnapshotStore& snapshot_store_;
  MemoryLevel memory_level_;
  timestamp_t recovered_wal_timestamp_;
  PostReopenHandler post_reopen_handler_;
  std::mutex activation_handler_mutex_;
  CheckpointActivationHandler activation_handler_;
};

}  // namespace neug
