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
#include "neug/transaction/cow_graph_update_storage.h"
#include "neug/transaction/cow_graph_write_set.h"
#include "neug/transaction/current_graph_write_guard.h"

namespace neug {

class IWalWriter;

/**
 * @brief Exclusive current-graph private-COW write transaction.
 *
 * The transaction holds exclusive operation admission from Begin() through
 * Commit()/Abort(). Mutations are isolated in CowGraphWriteSet. Commit prepares
 * a replacement for the already-pinned current slot, makes logical redo
 * durable, and then performs a no-fail in-place slot replacement without
 * allocating a new snapshot generation.
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

  CowGraphUpdateStorage OpenStorage() {
    return CowGraphUpdateStorage(write_set_, kReadTimestamp, timestamp());
  }

  timestamp_t timestamp() const noexcept { return guard_.Timestamp(); }

  GraphStats statistic() const {
    return GraphStats(write_set_.view(), write_set_.base_planning_generation());
  }

 private:
  friend class ExecutionSlot;

  static CurrentCowWriteTransaction Begin(CurrentGraphWriteGuard guard,
                                          Allocator& alloc,
                                          GraphSnapshotStore& snapshot_store,
                                          IWalWriter& wal_writer);

  CurrentCowWriteTransaction(CurrentGraphWriteGuard guard,
                             CowGraphWriteSet write_set,
                             GraphSnapshotStore& snapshot_store,
                             IWalWriter& wal_writer) noexcept;

  bool active() const noexcept { return guard_.active(); }
  Status PrepareCommit(uint64_t& committed_planning_generation);
  void release(bool committed) noexcept;

  CurrentGraphWriteGuard guard_;
  CowGraphWriteSet write_set_;
  GraphSnapshotStore& snapshot_store_;
  IWalWriter& wal_writer_;
};

}  // namespace neug
