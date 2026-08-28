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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "neug/storages/graph/graph_stats.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/cow_graph_storage.h"
#include "neug/transaction/cow_graph_workspace.h"
#include "neug/transaction/timestamp_lease.h"
#include "neug/utils/result.h"

namespace neug {

class IWalWriter;

/**
 * @brief Versioned private-COW write transaction.
 *
 * Execution holds an update lease that excludes other writers while existing
 * readers continue on pinned snapshots. Commit prepares a new snapshot, makes
 * logical redo durable, briefly blocks new readers, and publishes the prepared
 * snapshot generation. Graph mutation is delegated to CowGraphStorage.
 */
class SnapshotCowWriteTransaction {
 public:
  SnapshotCowWriteTransaction(std::shared_ptr<PropertyGraph> cow_graph,
                              uint64_t planning_generation, Allocator& alloc,
                              IWalWriter& wal_writer,
                              GraphSnapshotStore& snapshot_store,
                              UpdateTimestampLease timestamp_lease);

  SnapshotCowWriteTransaction(const SnapshotCowWriteTransaction&) = delete;
  SnapshotCowWriteTransaction& operator=(const SnapshotCowWriteTransaction&) =
      delete;
  SnapshotCowWriteTransaction(SnapshotCowWriteTransaction&& other) noexcept;
  SnapshotCowWriteTransaction& operator=(SnapshotCowWriteTransaction&&) =
      delete;

  ~SnapshotCowWriteTransaction() noexcept;

  bool Commit();
  void Abort() noexcept;

  CowGraphStorage OpenStorage() {
    return CowGraphStorage(workspace_, timestamp(), timestamp(), alloc_);
  }

  timestamp_t timestamp() const noexcept {
    return timestamp_lease_.Timestamp();
  }
  const GraphView& view() const { return workspace_.view(); }
  GraphStats statistic() const {
    return GraphStats(workspace_.view(), workspace_.base_planning_generation());
  }
  const Schema& schema() const { return workspace_.view().schema(); }
  bool PlanningChanged() const { return workspace_.PlanningChanged(); }

  Value GetVertexId(label_t label, vid_t lid) const;
  bool GetVertexIndex(label_t label, const Value& id, vid_t& index) const;
  Value GetVertexProperty(label_t label, vid_t lid, int col_id) const;

  std::shared_ptr<RefColumnBase> get_vertex_property_column(
      uint8_t label, const std::string& col_name) const {
    return workspace_.graph()->GetVertexPropertyColumn(label, col_name);
  }

  CsrView GetGenericOutgoingGraphView(label_t v_label, label_t neighbor_label,
                                      label_t edge_label) const {
    return workspace_.graph()->GetGenericOutgoingGraphView(
        v_label, neighbor_label, edge_label, timestamp());
  }

  CsrView GetGenericIncomingGraphView(label_t v_label, label_t neighbor_label,
                                      label_t edge_label) const {
    return workspace_.graph()->GetGenericIncomingGraphView(
        v_label, neighbor_label, edge_label, timestamp());
  }

  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label, int prop_id) const {
    return workspace_.graph()->GetEdgeDataAccessor(src_label, dst_label,
                                                   edge_label, prop_id);
  }

 private:
  friend class TransactionContext;

  // ServiceTransactionManager checks its deadline after this fallible prepare
  // step and before entering the WAL durability boundary.
  Status PrepareCommit();
  bool CommitPrepared();

  bool active() const noexcept { return timestamp_lease_.active(); }
  void release(std::optional<uint32_t> installed_snapshot_generation) noexcept;

  CowGraphWorkspace workspace_;
  Allocator& alloc_;
  IWalWriter& wal_writer_;
  GraphSnapshotStore& snapshot_store_;
  UpdateTimestampLease timestamp_lease_;
  std::optional<GraphSnapshotStore::PreparedSnapshot> prepared_snapshot_;
};

}  // namespace neug
