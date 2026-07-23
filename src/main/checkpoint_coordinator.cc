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
#include "neug/storages/checkpoint_creation.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/transaction/update_timestamp_guard.h"
#include "neug/utils/exception/exception.h"

namespace neug {

namespace {

Status maintenance_preparation_failure(const std::exception& e) {
  return Status(StatusCode::ERR_INTERNAL_ERROR, e.what());
}

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
    MemoryLevel memory_level, timestamp_t recovered_wal_timestamp,
    PostReopenHandler post_reopen_handler)
    : checkpoint_manager_(checkpoint_manager),
      snapshot_store_(snapshot_store),
      memory_level_(memory_level),
      recovered_wal_timestamp_(recovered_wal_timestamp),
      post_reopen_handler_(std::move(post_reopen_handler)) {}

Status CheckpointCoordinator::ExecuteApManual() {
  auto status = executeWithNewMaintenance(Reason::kManual);
  if (status.ok()) {
    recovered_wal_timestamp_ = 0;
  }
  return status;
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

Status CheckpointCoordinator::ExecuteTpManual(
    UpdateTimestampGuard timestamp_guard) {
  Status status = [&]() -> Status {
    try {
      timestamp_guard.BeginCommit();
      timestamp_guard.DrainReaders();
      auto maintenance = snapshot_store_.AcquireCheckpointMaintenance();
      return execute(maintenance, Reason::kManual,
                     /*invoke_activation_handler=*/true);
    } catch (const std::exception& e) {
      return maintenance_preparation_failure(e);
    } catch (...) {
      return Status(StatusCode::ERR_INTERNAL_ERROR,
                    "Unknown checkpoint preparation failure");
    }
  }();

  if (status.ok()) {
    recovered_wal_timestamp_ = 0;
  }
  if (status.ok()) {
    // The post-reopen and activation handlers completed every database- and
    // service-owned state transition while this guard still prevented new
    // transactions from starting.
    timestamp_guard.CompleteCheckpoint();
  } else {
    // Preparation failures happen before execute() enters its destructive
    // phase. execute() itself fail-stops after that boundary.
    timestamp_guard.Release();
  }
  return status;
}

Status CheckpointCoordinator::ExecuteRecovery() {
  auto status = executeWithNewMaintenance(Reason::kRecovery);
  if (status.ok()) {
    recovered_wal_timestamp_ = 0;
  }
  return status;
}

Status CheckpointCoordinator::PrepareShutdown() {
  auto status = executeWithNewMaintenance(Reason::kShutdown);
  if (status.ok()) {
    recovered_wal_timestamp_ = 0;
  }
  return status;
}

Status CheckpointCoordinator::executeWithNewMaintenance(
    Reason reason, bool invoke_activation_handler) {
  try {
    auto maintenance = snapshot_store_.AcquireCheckpointMaintenance();
    return execute(maintenance, reason, invoke_activation_handler);
  } catch (const std::exception& e) {
    return maintenance_preparation_failure(e);
  }
}

Status CheckpointCoordinator::execute(
    GraphSnapshotStore::CheckpointMaintenanceHandle& maintenance, Reason reason,
    bool invoke_activation_handler) {
  CheckpointCreation creation(checkpoint_manager_);
  const bool reopen_after_checkpoint = reason != Reason::kShutdown;

  // Once the destructive phase begins, a running database cannot safely
  // continue after failure. Recovery and shutdown are not externally visible
  // runtimes, so their callers may still unwind and destroy the consumed graph.
  bool destructive_phase = false;
  try {
    auto& live_graph = maintenance.MutableCurrentSnapshot();

    LOG(INFO) << "Executing " << reasonName(reason) << " checkpoint"
              << (reopen_after_checkpoint ? " and reopening the current graph"
                                          : " without reopening the graph");
    destructive_phase = true;
    creation.BuildFrom(live_graph);
    auto published_checkpoint = creation.Publish();

    if (reopen_after_checkpoint) {
      // This is the intentional in-place checkpoint reopen path. It is only
      // used after AcquireCheckpointMaintenance() has verified that the
      // current slot has no ordinary pins and the caller holds the AP exclusive
      // lock or a drained TP update guard.
      maintenance.ReopenCurrentGraphFromCheckpoint(published_checkpoint,
                                                   memory_level_);

      // Correctness-critical rotation first. Infallible by contract; a
      // throwing handler would escape into the destructive-phase fail-stop
      // below, which is the intended behavior.
      post_reopen_handler_(published_checkpoint->allocator_dir());

      if (invoke_activation_handler) {
        invokeActivationHandler(published_checkpoint->wal_dir());
      }

      cleanup_retired_checkpoints(checkpoint_manager_);
    }
    return Status::OK();
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
