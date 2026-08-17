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

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "neug/storages/allocators.h"
#include "neug/storages/graph/graph_stats.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/cow_graph_update_storage.h"
#include "neug/transaction/cow_graph_write_set.h"
#include "neug/transaction/timestamp_lease.h"

namespace neug {

class ExecutionSlot;
class PropertyGraph;
class IWalWriter;
class IVersionManager;
class Schema;
class CowGraphUpdateStorage;
class TransactionContext;

/**
 * @brief Snapshot-publishing COW write transaction.
 *
 * Owns one CowGraphWriteSet and one update timestamp lease. The transaction
 * borrows its database-owned allocator, publication store and WAL writer when
 * it begins.
 *
 * **COW Design:**
 * - Holds a shared_ptr to a COW-cloned PropertyGraph
 * - CowGraphUpdateStorage performs all DDL/DML modifications on the COW
 *   copy
 * - Commits prepare the snapshot before WAL, then perform a no-fail
 *   publication after WAL succeeds
 * - Abort discards the COW copy (no effect on original)
 *
 * **Concurrency contract** (VersionManager state machine):
 * - Acquisition enters the update-exec phase: waits for in-flight inserts,
 *   blocks new inserts/updates, and lets reads continue on their pinned
 *   snapshots.
 * - Commit calls VersionManager::begin_update_commit to enter the update-commit
 *   phase: briefly blocks new reads and inserts while the COW snapshot is
 *   published. Existing reads continue unaffected.
 *
 * @since v0.1.0
 */
class SnapshotCowWriteTransaction {
 public:
  SnapshotCowWriteTransaction(SnapshotCowWriteTransaction&& other) noexcept;
  SnapshotCowWriteTransaction(const SnapshotCowWriteTransaction&) = delete;
  SnapshotCowWriteTransaction& operator=(const SnapshotCowWriteTransaction&) =
      delete;
  SnapshotCowWriteTransaction& operator=(SnapshotCowWriteTransaction&&) =
      delete;

  // Storage-focused tests may build the transaction directly; runtime code
  // obtains it from ExecutionSlot::BeginSnapshotCowWriteTransaction().
  static SnapshotCowWriteTransaction BeginForTesting(
      IVersionManager& version_manager, GraphSnapshotStore& snapshot_store,
      Allocator& alloc, IWalWriter& wal_writer) {
    return Begin(version_manager, snapshot_store, alloc, wal_writer);
  }

  /**
   * @brief Destructor that calls Abort().
   * @since v0.1.0
   */
  ~SnapshotCowWriteTransaction();

  /**
   * @brief Get the transaction timestamp.
   * @since v0.1.0
   */
  timestamp_t timestamp() const { return timestamp_lease_.Timestamp(); }

  Status Commit();

  void Abort() noexcept;

  CowGraphUpdateStorage OpenStorage();

  // Compatibility read surface for focused transaction tests. Runtime graph
  // access is expected to use the storage returned by OpenStorage().
  const GraphView& view() const { return write_set_.view(); }

  GraphStats statistic() const {
    return GraphStats(write_set_.view(), write_set_.base_planning_generation());
  }

  // --- Read-only accessors (not graph modifications) ---
  const Schema& schema() const { return write_set_.graph()->schema(); }

  Value GetVertexId(label_t label, vid_t lid) const;

  bool GetVertexIndex(label_t label, const Value& id, vid_t& index) const;

  Value GetVertexProperty(label_t label, vid_t lid, int col_id) const;

  std::shared_ptr<RefColumnBase> get_vertex_property_column(
      uint8_t label, const std::string& col_name) const {
    return write_set_.graph()->GetVertexPropertyColumn(label, col_name);
  }

  CsrView GetGenericOutgoingGraphView(label_t v_label, label_t neighbor_label,
                                      label_t edge_label) const {
    return write_set_.graph()->GetGenericOutgoingGraphView(
        v_label, neighbor_label, edge_label, timestamp());
  }

  CsrView GetGenericIncomingGraphView(label_t v_label, label_t neighbor_label,
                                      label_t edge_label) const {
    return write_set_.graph()->GetGenericIncomingGraphView(
        v_label, neighbor_label, edge_label, timestamp());
  }

  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label, int prop_id) const {
    return write_set_.graph()->GetEdgeDataAccessor(src_label, dst_label,
                                                   edge_label, prop_id);
  }

  friend class CowGraphUpdateStorage;

 private:
  friend class ExecutionSlot;
  friend class TransactionContext;

  Status PrepareCommit();

  Status CommitPrepared();

  static SnapshotCowWriteTransaction Begin(IVersionManager& version_manager,
                                           GraphSnapshotStore& snapshot_store,
                                           Allocator& alloc,
                                           IWalWriter& wal_writer);
  static std::optional<SnapshotCowWriteTransaction> TryBegin(
      IVersionManager& version_manager, GraphSnapshotStore& snapshot_store,
      Allocator& alloc, IWalWriter& wal_writer);
  static SnapshotCowWriteTransaction BeginWithLease(
      UpdateTimestampLease timestamp_lease, GraphSnapshotStore& snapshot_store,
      Allocator& alloc, IWalWriter& wal_writer);

  SnapshotCowWriteTransaction(CowGraphWriteSet write_set, Allocator& alloc,
                              UpdateTimestampLease timestamp_lease,
                              GraphSnapshotStore& snapshot_store,
                              IWalWriter& wal_writer);

  void release(std::optional<uint32_t> installed_snapshot_generation) noexcept;

  CowGraphWriteSet write_set_;
  // Database-owned. The active update lease prevents checkpoint reopen while
  // the transaction may reference allocator-backed COW storage.
  Allocator& alloc_;
  UpdateTimestampLease timestamp_lease_;
  GraphSnapshotStore& snapshot_store_;
  IWalWriter& wal_writer_;
  std::optional<GraphSnapshotStore::PreparedSnapshot> prepared_snapshot_;
  bool commit_prepared_{false};
};

inline CowGraphUpdateStorage SnapshotCowWriteTransaction::OpenStorage() {
  return CowGraphUpdateStorage(write_set_, timestamp(), timestamp(), alloc_);
}

}  // namespace neug
