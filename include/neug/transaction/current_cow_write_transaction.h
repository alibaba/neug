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
#include "neug/transaction/cow_graph_storage.h"
#include "neug/transaction/cow_graph_workspace.h"
#include "neug/transaction/current_graph_write_guard.h"

namespace neug {

class IWalWriter;
class CheckpointCoordinator;

/**
 * @brief Exclusive current-graph private-COW write transaction.
 *
 * Mutations are isolated in a private CowGraphWorkspace. Ordinary commit makes
 * logical redo durable before replacing the already-pinned current slot;
 * checkpoint-backed bulk commit is coordinated by CheckpointCoordinator.
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

  CowGraphStorage OpenStorage() {
    return CowGraphStorage(workspace_, kReadTimestamp, timestamp(), alloc_);
  }

  BulkCowGraphStorage OpenBulkStorage() {
    return BulkCowGraphStorage(workspace_, kReadTimestamp, timestamp(), alloc_);
  }

  timestamp_t timestamp() const noexcept { return guard_.Timestamp(); }

  GraphStats statistic() const {
    return GraphStats(workspace_.view(), workspace_.base_planning_generation());
  }

  const Schema& schema() const { return workspace_.view().schema(); }

 private:
  friend class CheckpointCoordinator;
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
  Status CommitTransient();
  void replaceCurrentSnapshot(uint64_t planning_generation) noexcept;
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
