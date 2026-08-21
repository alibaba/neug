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

#include "neug/storages/graph/graph_stats.h"
#include "neug/transaction/cow_graph_storage_adapter.h"
#include "neug/transaction/cow_graph_workspace.h"
#include "neug/transaction/current_graph_write_guard.h"

namespace neug {

class IWalWriter;

/**
 * @brief Exclusive current-graph write transaction in one of two modes.
 *
 * In COW mode the transaction holds exclusive operation admission from
 * Begin() through Commit()/Abort(). Mutations are isolated in a private clone
 * (CowGraphWorkspace). Commit prepares a replacement for the already-pinned
 * current slot, makes logical redo durable, and then performs a no-fail
 * in-place slot replacement without allocating a new snapshot generation.
 *
 * In in-place mode (BeginInPlace) the workspace borrows the live published
 * graph for bulk/index operations. Those mutations cannot be rolled back, so
 * both Commit() and Abort() publish them by bumping the current slot's
 * generations; destruction of an active transaction publishes as well.
 */
class CurrentCowWriteTransaction {
 public:
  static constexpr timestamp_t kReadTimestamp = MAX_TIMESTAMP;

  CurrentCowWriteTransaction(const CurrentCowWriteTransaction&) = delete;
  CurrentCowWriteTransaction& operator=(const CurrentCowWriteTransaction&) =
      delete;
  CurrentCowWriteTransaction(CurrentCowWriteTransaction&& other) noexcept;
  CurrentCowWriteTransaction& operator=(CurrentCowWriteTransaction&&) = delete;

  ~CurrentCowWriteTransaction() noexcept;

  Status Commit();
  void Abort() noexcept;

  /// Publish an in-place mutation while retaining its exclusive timestamp
  /// lease for the immediately following incremental checkpoint.
  UpdateTimestampLease PublishInPlaceAndReleaseForCheckpoint() noexcept;

  // Entry point for bulk COPY and index DDL: acquires the same exclusive
  // writer admission but skips cloning, borrowing the live published graph
  // and its mutable view instead. Callers must keep the returned transaction
  // alive while referencing graph() / mutable_view() / OpenStorage().
  static CurrentCowWriteTransaction BeginInPlace(
      CurrentGraphWriteGuard guard, Allocator& alloc,
      GraphSnapshotStore& snapshot_store, IWalWriter& wal_writer);

  CowGraphStorageAdapter OpenStorage() {
    // In-place reads keep the legacy semantics of the guard timestamp; COW
    // reads see everything committed into the private clone.
    const timestamp_t read_ts =
        workspace_.is_in_place() ? timestamp() : kReadTimestamp;
    return CowGraphStorageAdapter(workspace_, read_ts, timestamp(), alloc_);
  }

  timestamp_t timestamp() const noexcept { return guard_.Timestamp(); }

  bool is_in_place() const noexcept { return workspace_.is_in_place(); }

  // In-place mode: the borrowed live graph and its mutable view. The write
  // guard keeps them valid until Commit()/Abort().
  PropertyGraph& graph() { return workspace_.storage(); }
  GraphView& mutable_view() { return workspace_.view(); }
  void MarkPlanningChanged() noexcept { workspace_.MarkPlanningChanged(); }

  GraphStats statistic() const {
    return GraphStats(workspace_.view(), workspace_.base_planning_generation());
  }

  const Schema& schema() const { return workspace_.view().schema(); }

 private:
  friend class ExecutionSlot;

  static CurrentCowWriteTransaction Begin(CurrentGraphWriteGuard guard,
                                          Allocator& alloc,
                                          GraphSnapshotStore& snapshot_store,
                                          IWalWriter& wal_writer);

  CurrentCowWriteTransaction(CurrentGraphWriteGuard guard,
                             CowGraphWorkspace workspace, Allocator& alloc,
                             GraphSnapshotStore& snapshot_store,
                             IWalWriter& wal_writer) noexcept;

  bool active() const noexcept { return guard_.active(); }
  Status PrepareCommit(uint64_t& committed_planning_generation);
  void publishInPlace() noexcept;
  void release(bool committed) noexcept;

  CurrentGraphWriteGuard guard_;
  CowGraphWorkspace workspace_;
  // Database-owned. The active write guard prevents checkpoint reopen while
  // the transaction may reference allocator-backed COW storage.
  Allocator& alloc_;
  GraphSnapshotStore& snapshot_store_;
  IWalWriter& wal_writer_;
};

}  // namespace neug
