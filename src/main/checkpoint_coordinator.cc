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

#include "neug/main/checkpoint_coordinator.h"

#include <glog/logging.h>

#include <cstdlib>
#include <exception>
#include <string>
#include <utility>

#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/timestamp_lease.h"
#include "neug/utils/exception/exception.h"

namespace neug {

namespace {

[[noreturn]] void fail_stop_live_database(const char* message) noexcept {
  LOG(FATAL) << message
             << "; terminating to prevent access to an invalid live graph";
  std::abort();
}

void cleanup_retired_checkpoints(
    CheckpointManager& checkpoint_manager) noexcept {
  try {
    checkpoint_manager.CleanupRetiredCheckpoints();
  } catch (const std::exception& e) {
    LOG(WARNING) << "Checkpoint GC failed: " << e.what();
  } catch (...) { LOG(WARNING) << "Checkpoint GC failed"; }
}

}  // namespace

const char* CheckpointCoordinator::reasonName(Reason reason) {
  switch (reason) {
  case Reason::kManual:
    return "manual";
  case Reason::kRecovery:
    return "recovery";
  case Reason::kShutdown:
    return "shutdown";
  }
  return "unknown";
}

CheckpointCoordinator::CheckpointCoordinator(
    CheckpointManager& checkpoint_manager, GraphSnapshotStore& snapshot_store,
    MemoryLevel memory_level, PostReopenHandler post_reopen_handler)
    : checkpoint_manager_(checkpoint_manager),
      snapshot_store_(snapshot_store),
      memory_level_(memory_level),
      post_reopen_handler_(std::move(post_reopen_handler)) {
  if (!post_reopen_handler_) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Checkpoint post-reopen handler must not be empty");
  }
}

void CheckpointCoordinator::SetActivationHandler(
    CheckpointActivationHandler handler) {
  if (!handler) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Checkpoint activation handler must not be empty");
  }
  std::lock_guard<std::mutex> lock(activation_handler_mutex_);
  if (activation_handler_) {
    THROW_RUNTIME_ERROR("Checkpoint activation handler is already set");
  }
  activation_handler_ = std::move(handler);
}

void CheckpointCoordinator::ClearActivationHandler() {
  std::lock_guard<std::mutex> lock(activation_handler_mutex_);
  activation_handler_ = nullptr;
}

void CheckpointCoordinator::invokeActivationHandler(
    const std::string& checkpoint_wal_uri) {
  std::lock_guard<std::mutex> lock(activation_handler_mutex_);
  if (activation_handler_) {
    activation_handler_(checkpoint_wal_uri);
  }
}

Status CheckpointCoordinator::PublishManualCheckpoint(
    UpdateTimestampLease timestamp_lease) {
  try {
    timestamp_lease.MakeUpdateExclusive();
  } catch (const std::exception& e) {
    return Status(StatusCode::ERR_INTERNAL_ERROR, e.what());
  } catch (...) {
    return Status(StatusCode::ERR_INTERNAL_ERROR,
                  "Unknown checkpoint preparation failure");
  }

  Status status = execute(Reason::kManual);
  if (!status.ok()) {
    // Preparation failures happen before execute() enters its destructive
    // phase. execute() itself fail-stops after that boundary. The lease
    // destructor releases the timestamp through the ordinary failure path.
    return status;
  }

  // The post-reopen and activation handlers completed every database- and
  // service-owned state transition while this lease still prevented new
  // transactions from starting.
  timestamp_lease.FinishAndResetTimeline();
  return status;
}

Status CheckpointCoordinator::PublishRecoveryCheckpoint() {
  return execute(Reason::kRecovery);
}

Status CheckpointCoordinator::PublishShutdownCheckpoint() {
  return execute(Reason::kShutdown);
}

Status CheckpointCoordinator::execute(Reason reason) {
  const bool reopen_after_checkpoint = reason != Reason::kShutdown;

  // Once the destructive phase begins, a running database cannot safely
  // continue after failure. Recovery and shutdown are not externally visible
  // runtimes, so their callers may still unwind and destroy the consumed graph.
  bool destructive_phase = false;
  try {
    auto staging_checkpoint = checkpoint_manager_.CreateStagingCheckpoint();
    return snapshot_store_.WithCheckpointMaintenance(
        [&](GraphSnapshotStore::CheckpointMaintenanceContext& maintenance)
            -> Status {
          auto& live_graph = maintenance.MutableCurrentSnapshot();

          if (live_graph.HasPendingMutations()) {
            return Status(
                StatusCode::ERR_ILLEGAL_OPERATION,
                "Cannot create a checkpoint while mutations for pending "
                "extension-backed indexes have not been applied. Load the "
                "required extension first.");
          }

          LOG(INFO) << "Executing " << reasonName(reason) << " checkpoint"
                    << (reopen_after_checkpoint
                            ? " and reopening the current graph"
                            : " without reopening the graph");
          destructive_phase = true;
          live_graph.Compact();
          live_graph.DumpAndClear(staging_checkpoint.checkpoint());
          auto published_checkpoint = staging_checkpoint.Commit();
          VLOG(1) << "Finish checkpoint: " << published_checkpoint->path();

          if (reopen_after_checkpoint) {
            // This is the intentional in-place checkpoint reopen path. It is
            // only used inside WithCheckpointMaintenance() after the current
            // slot has no ordinary pins and transaction quiescence has been
            // established by the database lifecycle or a drained update lease.
            maintenance.ReopenCurrentGraphFromCheckpoint(published_checkpoint,
                                                         memory_level_);

            // Correctness-critical rotation first. Infallible by contract; a
            // throwing handler fails the live manual path closed or aborts
            // recovery.
            post_reopen_handler_(published_checkpoint->allocator_dir());

            if (reason == Reason::kManual) {
              invokeActivationHandler(published_checkpoint->wal_dir());
            }

            cleanup_retired_checkpoints(checkpoint_manager_);
          }
          return Status::OK();
        });
  } catch (const exception::IOException& e) {
    if (destructive_phase && reason == Reason::kManual) {
      fail_stop_live_database(e.what());
    }
    return Status(StatusCode::ERR_IO_ERROR, e.what());
  } catch (const std::exception& e) {
    if (destructive_phase && reason == Reason::kManual) {
      fail_stop_live_database(e.what());
    }
    return Status(StatusCode::ERR_INTERNAL_ERROR, e.what());
  } catch (...) {
    if (destructive_phase && reason == Reason::kManual) {
      fail_stop_live_database("Unknown destructive checkpoint failure");
    }
    return Status(StatusCode::ERR_INTERNAL_ERROR,
                  destructive_phase ? "Unknown destructive checkpoint failure"
                                    : "Unknown checkpoint preparation failure");
  }
}

}  // namespace neug
