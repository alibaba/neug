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
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "flat_hash_map/flat_hash_map.hpp"
#include "neug/common/types/value.h"
#include "neug/storages/allocators.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/graph_stats.h"
#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/property_graph_cow_state.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/storages/index/storage_index.h"
#include "neug/transaction/timestamp_lease.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/transaction/wal/wal_builder.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"

namespace neug {

class ExecutionSlot;
class PropertyGraph;
class IWalWriter;
class IVersionManager;
class Schema;

/**
 * @brief Shared, private COW graph state used by AP and TP write owners.
 *
 * This is a concrete data-plane state, not a transaction interface. It owns
 * the private graph, detach state, mutable view, base checkpoint and logical
 * redo buffer. Admission, visibility timestamps, WAL writers and publication
 * remain the responsibility of the owning transaction.
 */
class CowGraphState {
 public:
  CowGraphState(std::shared_ptr<PropertyGraph> cow_graph,
                uint64_t base_planning_generation);

  CowGraphState(CowGraphState&&) noexcept = default;
  CowGraphState& operator=(CowGraphState&&) noexcept = default;
  CowGraphState(const CowGraphState&) = delete;
  CowGraphState& operator=(const CowGraphState&) = delete;

  std::shared_ptr<PropertyGraph>& graph() { return cow_graph_; }
  const std::shared_ptr<PropertyGraph>& graph() const { return cow_graph_; }
  PropertyGraphCowState& detach_state() { return detach_state_; }
  const PropertyGraphCowState& detach_state() const { return detach_state_; }
  GraphView& view() { return view_; }
  const GraphView& view() const { return view_; }
  uint64_t base_planning_generation() const {
    return base_planning_generation_;
  }
  std::shared_ptr<Checkpoint>& checkpoint() { return checkpoint_; }
  WalBuilder& logical_redo() { return logical_redo_; }
  const WalBuilder& logical_redo() const { return logical_redo_; }
  bool HasChanges() const { return detach_state_.HasChanges(); }
  void Reset() noexcept;

 private:
  std::shared_ptr<PropertyGraph> cow_graph_;
  PropertyGraphCowState detach_state_;
  GraphView view_;
  uint64_t base_planning_generation_;
  std::shared_ptr<Checkpoint> checkpoint_;
  WalBuilder logical_redo_;
};

/**
 * @brief Resource holder and lifecycle manager for update transactions.
 *
 * UpdateTransaction owns one CowGraphState and one update timestamp lease.
 * Statement-local allocators and commit-time publication/WAL resources are
 * borrowed by the caller and never retained by the transaction.
 *
 * **COW Design:**
 * - Holds a shared_ptr to a COW-cloned PropertyGraph
 * - StorageCOWUpdateInterface performs all DDL/DML modifications on the COW
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
class UpdateTransaction {
 public:
  static UpdateTransaction Begin(IVersionManager& version_manager,
                                 GraphSnapshotStore& snapshot_store);

  UpdateTransaction(UpdateTransaction&& other) noexcept;
  UpdateTransaction(const UpdateTransaction&) = delete;
  UpdateTransaction& operator=(const UpdateTransaction&) = delete;
  UpdateTransaction& operator=(UpdateTransaction&&) = delete;

  /**
   * @brief Destructor that calls Abort().
   * @since v0.1.0
   */
  ~UpdateTransaction();

  /**
   * @brief Get the transaction timestamp.
   * @since v0.1.0
   */
  timestamp_t timestamp() const { return timestamp_lease_.Timestamp(); }

  Status Commit(GraphSnapshotStore& snapshot_store, IWalWriter& wal_writer);

  void MarkPlanningChanged() noexcept {
    cow_graph_state_.detach_state().planning_changed = true;
  }

  void Abort() noexcept;

  static void IngestWal(PropertyGraph& graph, uint32_t timestamp, char* data,
                        size_t length, Allocator& alloc);

  const GraphView& view() const { return cow_graph_state_.view(); }

  CowGraphState& cow_graph_state() { return cow_graph_state_; }
  const CowGraphState& cow_graph_state() const { return cow_graph_state_; }

  GraphStats statistic() const {
    return GraphStats(cow_graph_state_.view(),
                      cow_graph_state_.base_planning_generation());
  }

  // --- Read-only accessors (not graph modifications) ---
  const Schema& schema() const { return cow_graph_state_.graph()->schema(); }

  Value GetVertexId(label_t label, vid_t lid) const;

  bool GetVertexIndex(label_t label, const Value& id, vid_t& index) const;

  Value GetVertexProperty(label_t label, vid_t lid, int col_id) const;

  std::shared_ptr<RefColumnBase> get_vertex_property_column(
      uint8_t label, const std::string& col_name) const {
    return cow_graph_state_.graph()->GetVertexPropertyColumn(label, col_name);
  }

  CsrView GetGenericOutgoingGraphView(label_t v_label, label_t neighbor_label,
                                      label_t edge_label) const {
    return cow_graph_state_.graph()->GetGenericOutgoingGraphView(
        v_label, neighbor_label, edge_label, timestamp());
  }

  CsrView GetGenericIncomingGraphView(label_t v_label, label_t neighbor_label,
                                      label_t edge_label) const {
    return cow_graph_state_.graph()->GetGenericIncomingGraphView(
        v_label, neighbor_label, edge_label, timestamp());
  }

  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label, int prop_id) const {
    return cow_graph_state_.graph()->GetEdgeDataAccessor(src_label, dst_label,
                                                         edge_label, prop_id);
  }

  friend class StorageCOWUpdateInterface;

 private:
  UpdateTransaction(CowGraphState cow_graph_state,
                    UpdateTimestampLease timestamp_lease);

  void release(std::optional<uint32_t> installed_snapshot_generation) noexcept;

  CowGraphState cow_graph_state_;

  UpdateTimestampLease timestamp_lease_;
};

class StorageCOWUpdateInterface : public StorageUpdateInterface {
 public:
  StorageCOWUpdateInterface(UpdateTransaction& txn, Allocator& alloc)
      : StorageCOWUpdateInterface(txn.cow_graph_state(), txn.timestamp(),
                                  alloc) {}

  StorageCOWUpdateInterface(CowGraphState& cow_graph_state,
                            timestamp_t read_timestamp, Allocator& alloc)
      : StorageUpdateInterface(cow_graph_state.view(), read_timestamp),
        cow_graph_(cow_graph_state.graph()),
        cow_state_(cow_graph_state.detach_state()),
        mut_view_(cow_graph_state.view()),
        alloc_(alloc),
        ckp_(cow_graph_state.checkpoint()),
        wal_(&cow_graph_state.logical_redo()) {}
  ~StorageCOWUpdateInterface() = default;

 private:
  // Marks go to the COW clone; abort discards them with the clone.
  void MarkVertexTableDirty(label_t label) override {
    cow_graph_->MarkVertexTableDirty(label);
  }
  void MarkEdgeTableDirty(label_t src, label_t dst, label_t edge) override {
    cow_graph_->MarkEdgeTableDirty(src, dst, edge);
  }
  void MarkSchemaDirty() override {
    cow_graph_->MarkSchemaDirty();
    cow_state_.schema_changed = true;
    cow_state_.planning_changed = true;
  }

  // --- DML *Impl ---
  Status UpdateVertexPropertyImpl(label_t label, vid_t lid, int col_id,
                                  const Value& value) override;
  Status UpdateEdgePropertyImpl(label_t src_label, vid_t src, label_t dst_label,
                                vid_t dst, label_t edge_label,
                                int32_t oe_offset, int32_t ie_offset,
                                int32_t col_id, const Value& value) override;
  Status AddVertexImpl(label_t label, const Value& id,
                       const std::vector<Value>& props, vid_t& vid) override;
  Status AddEdgeImpl(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
                     label_t edge_label, const std::vector<Value>& properties,
                     const void*& prop) override;
  Status DeleteVertexImpl(label_t label, vid_t lid) override;
  Status DeleteEdgesImpl(label_t src_label, vid_t src, label_t dst_label,
                         vid_t dst, label_t edge_label) override;
  Status DeleteEdgeImpl(label_t src_label, vid_t src, label_t dst_label,
                        vid_t dst, label_t edge_label, int32_t oe_offset,
                        int32_t ie_offset) override;

  // --- Batch *Impl ---
  result<std::vector<vid_t>> BatchAddVerticesImpl(
      label_t v_label_id,
      std::shared_ptr<IDataChunkSupplier> supplier) override;
  Status BatchAddEdgesImpl(
      label_t src_label, label_t dst_label, label_t edge_label,
      std::shared_ptr<IDataChunkSupplier> supplier) override;
  Status BatchDeleteVerticesImpl(label_t v_label_id,
                                 const std::vector<vid_t>& vids) override;
  Status BatchDeleteEdgesImpl(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::tuple<vid_t, vid_t>>& edges) override;
  Status BatchDeleteEdgesImpl(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
      const std::vector<std::pair<vid_t, int32_t>>& ie_edges) override;

  // --- DDL *Impl ---
  Status CreateVertexTypeImpl(const CreateVertexTypeParam& config) override;
  Status CreateEdgeTypeImpl(const CreateEdgeTypeParam& config) override;
  Status AddVertexPropertiesImpl(
      label_t label, const AddVertexPropertiesParam& config) override;
  Status AddEdgePropertiesImpl(label_t src, label_t dst, label_t edge,
                               const AddEdgePropertiesParam& config) override;
  Status RenameVertexPropertiesImpl(
      label_t label, const RenameVertexPropertiesParam& config) override;
  Status RenameEdgePropertiesImpl(
      label_t src, label_t dst, label_t edge,
      const RenameEdgePropertiesParam& config) override;
  Status DeleteVertexPropertiesImpl(
      label_t label, const DeleteVertexPropertiesParam& config) override;
  Status DeleteEdgePropertiesImpl(
      label_t src, label_t dst, label_t edge,
      const DeleteEdgePropertiesParam& config) override;
  Status DeleteVertexTypeImpl(label_t label) override;
  Status DeleteEdgeTypeImpl(label_t src, label_t dst, label_t edge) override;

  // --- COW detach helpers ---
  Status detachVertexTableForInsert(label_t label);
  Status detachVertexTableForDelete(label_t label);
  Status detachVertexColumn(label_t label, int32_t col_id);
  Status detachEdgeTableForInsert(uint32_t edge_triplet_id);
  Status detachEdgeTableForDelete(uint32_t edge_triplet_id);
  Status detachEdgeColumn(uint32_t edge_triplet_id, int32_t col_id);
  Status detachAdjlists(uint32_t edge_triplet_id, vid_t src_lid, vid_t dst_lid,
                        Allocator& alloc);
  Status detachForResize(label_t label, size_t capacity);
  Status detachForResize(label_t src_label, label_t dst_label,
                         label_t edge_label, size_t capacity);
  Status prepareVertexDelete(label_t label, const std::vector<vid_t>& lids);
  Status detachIndex(StorageIndex& index);

  std::shared_ptr<PropertyGraph>& cow_graph_;
  PropertyGraphCowState& cow_state_;
  GraphView& mut_view_;
  Allocator& alloc_;
  std::shared_ptr<Checkpoint>& ckp_;
  WalBuilder* wal_;
};

}  // namespace neug
