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
#include <limits>
#include <string>
#include <utility>

#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/current_cow_write_transaction.h"
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
    checkpoint_manager.CollectGarbage();
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
    MemoryLevel memory_level, PostReopenHandler post_reopen_handler,
    WalEpochActivationHandler wal_epoch_activation_handler)
    : checkpoint_manager_(checkpoint_manager),
      snapshot_store_(snapshot_store),
      memory_level_(memory_level),
      post_reopen_handler_(std::move(post_reopen_handler)),
      wal_epoch_activation_handler_(std::move(wal_epoch_activation_handler)) {
  if (!post_reopen_handler_) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Checkpoint post-reopen handler must not be empty");
  }
}

void CheckpointCoordinator::invokeWalEpochActivationHandler(
    const std::string& checkpoint_wal_dir) {
  if (wal_epoch_activation_handler_) {
    wal_epoch_activation_handler_(checkpoint_wal_dir);
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
    // Preparation failures happen before execute() starts consuming the live
    // graph. execute() itself fail-stops after that boundary. The lease
    // destructor releases the timestamp through the ordinary failure path.
    return status;
  }

  // The graph, allocators, and WAL writers now reference the newly published
  // checkpoint while this lease still prevents new transactions from starting.
  timestamp_lease.FinishAndResetTimeline();
  return status;
}

Status CheckpointCoordinator::CommitCowWrite(
    CurrentCowWriteTransaction& transaction) {
  if (!transaction.active()) {
    return Status::OK();
  }
  auto& workspace = transaction.workspace_;
  const auto& logical_redo = workspace.logical_redo();
  if (logical_redo.op_num() != 0 || logical_redo.content_size() != 0) {
    transaction.Abort();
    return Status::InternalError(
        "Bulk checkpoint commit cannot contain logical WAL redo");
  }
  if (workspace.HasTransientMutation()) {
    transaction.Abort();
    return Status::InternalError(
        "Bulk checkpoint commit cannot contain transient graph mutations");
  }
  if (!workspace.HasBulkMutation()) {
    transaction.Abort();
    return Status::OK();
  }

  bool consuming_checkpoint_started = false;
  try {
    if (workspace.base_planning_generation() ==
        std::numeric_limits<uint64_t>::max()) {
      transaction.Abort();
      return Status::InternalError("Planning generation space exhausted");
    }
    const uint64_t committed_planning_generation =
        workspace.base_planning_generation() + 1;

    auto& graph = *workspace.graph();
    auto preflight = graph.ValidateCheckpointPreconditions();
    if (!preflight.ok()) {
      transaction.Abort();
      return preflight;
    }

    // Finalize only persistent COPY targets before checkpoint consumption,
    // while ordinary rollback remains safe. Vertex COPY has a timestamp-zero
    // tail; edge COPY needs compaction only when it has a neighbor sort key.
    // Keeping the target sets transaction-local avoids compacting unrelated
    // dirty tables inherited by the private COW graph.
    workspace.FinalizeBulkTablesForCheckpoint();

    // This is intentionally not the in-place checkpoint path in execute().
    // `graph` belongs exclusively to this COW transaction, so it can be
    // reopened before publication without changing the live snapshot. Only
    // replaceCurrentSnapshot() below makes the bulk statement visible.
    auto staging_checkpoint = checkpoint_manager_.CreateStaging();
    LOG(INFO) << "Committing private bulk COW graph with checkpoint";
    graph.DetachDirtyModulesForCheckpoint(workspace.detach_state());
    // DumpDirtyAndReopen() consumes dirty containers. Most modules have been
    // detached into this private graph, but VecColumn payload buffers are still
    // shared intentionally by VecColumn::Detach(). Once consumption starts, a
    // later failure may therefore leave the published base graph unusable. Do
    // not report rollback from this point until checkpoint-specific payload
    // detachment exists.
    consuming_checkpoint_started = true;
    graph.DumpDirtyAndReopen(staging_checkpoint.checkpoint(),
                             transaction.timestamp());
    workspace.view().Rebuild(graph);

    auto published_checkpoint = staging_checkpoint.Publish();

    transaction.replaceCurrentSnapshot(committed_planning_generation);
    invokeWalEpochActivationHandler(published_checkpoint->wal_dir());

    const uint32_t snapshot_generation =
        transaction.guard_.Snapshot().snapshot_generation();
    workspace.Reset();
    transaction.guard_.release(snapshot_generation);
    // GC scans and fsyncs checkpoint directories. It is best-effort and does
    // not participate in the durable decision, so do not keep AP admission
    // exclusive while it runs.
    cleanup_retired_checkpoints(checkpoint_manager_);
    return Status::OK();
  } catch (const exception::IOException& e) {
    if (consuming_checkpoint_started) {
      fail_stop_live_database(e.what());
    }
    transaction.Abort();
    return Status(StatusCode::ERR_IO_ERROR, e.what());
  } catch (const std::exception& e) {
    if (consuming_checkpoint_started) {
      fail_stop_live_database(e.what());
    }
    transaction.Abort();
    return Status(StatusCode::ERR_INTERNAL_ERROR, e.what());
  } catch (...) {
    if (consuming_checkpoint_started) {
      fail_stop_live_database("Unknown bulk checkpoint commit failure");
    }
    transaction.Abort();
    return Status(StatusCode::ERR_INTERNAL_ERROR,
                  "Unknown bulk checkpoint preparation failure");
  }
}

Status CheckpointCoordinator::PublishRecoveryCheckpoint() {
  return execute(Reason::kRecovery);
}

Status CheckpointCoordinator::PublishShutdownCheckpoint(
    bool& live_graph_consumption_started) {
  live_graph_consumption_started = false;
  return execute(Reason::kShutdown, &live_graph_consumption_started);
}

Status CheckpointCoordinator::execute(
    Reason reason, bool* live_graph_consumption_started_out) {
  const bool reopen_after_checkpoint = reason != Reason::kShutdown;

  // Once live graph consumption begins, a running database cannot safely
  // continue after failure. Recovery and shutdown are not externally visible
  // runtimes, so their callers may still unwind and destroy the consumed graph.
  bool live_graph_consumption_started = false;
  try {
    auto staging_checkpoint = checkpoint_manager_.CreateStaging();
    auto status = snapshot_store_.WithCheckpointMaintenance(
        [&](GraphSnapshotStore::CheckpointMaintenanceContext& maintenance)
            -> Status {
          auto& live_graph = maintenance.MutableCurrentSnapshot();

          auto preflight = live_graph.ValidateCheckpointPreconditions();
          if (!preflight.ok()) {
            return preflight;
          }

          LOG(INFO) << "Executing " << reasonName(reason) << " checkpoint"
                    << (reopen_after_checkpoint
                            ? " and reopening the current graph"
                            : " without reopening the graph");
          live_graph_consumption_started = true;
          if (live_graph_consumption_started_out != nullptr) {
            *live_graph_consumption_started_out = true;
          }
          live_graph.Compact();
          live_graph.DumpAndClear(staging_checkpoint.checkpoint());
          auto published_checkpoint = staging_checkpoint.Publish();
          VLOG(1) << "Finish checkpoint: "
                  << published_checkpoint->manifest_path();

          if (reopen_after_checkpoint) {
            // This is the intentional in-place checkpoint reopen path. It is
            // only used inside WithCheckpointMaintenance() after the current
            // slot has no ordinary pins and transaction quiescence has been
            // established by the database lifecycle or a drained update lease.
            maintenance.ReopenCurrentGraphFromCheckpoint(published_checkpoint,
                                                         memory_level_);

            // Correctness-critical runtime rotation. This handler is infallible
            // by contract because the live graph was consumed.
            post_reopen_handler_(published_checkpoint->allocator_dir());

            if (reason == Reason::kManual) {
              invokeWalEpochActivationHandler(published_checkpoint->wal_dir());
            }

            cleanup_retired_checkpoints(checkpoint_manager_);
          }
          return Status::OK();
        });
    return status;
  } catch (const exception::IOException& e) {
    if (live_graph_consumption_started && reason == Reason::kManual) {
      fail_stop_live_database(e.what());
    }
    return Status(StatusCode::ERR_IO_ERROR, e.what());
  } catch (const std::exception& e) {
    if (live_graph_consumption_started && reason == Reason::kManual) {
      fail_stop_live_database(e.what());
    }
    return Status(StatusCode::ERR_INTERNAL_ERROR, e.what());
  } catch (...) {
    if (live_graph_consumption_started && reason == Reason::kManual) {
      fail_stop_live_database(
          "Unknown checkpoint failure after live graph consumption started");
    }
    return Status(StatusCode::ERR_INTERNAL_ERROR,
                  live_graph_consumption_started
                      ? "Unknown checkpoint failure after live graph "
                        "consumption started"
                      : "Unknown checkpoint preparation failure");
  }
}

}  // namespace neug
