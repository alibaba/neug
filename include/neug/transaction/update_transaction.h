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
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "flat_hash_map/flat_hash_map.hpp"
#include "neug/execution/common/types/value.h"
#include "neug/execution/execute/query_cache.h"
#include "neug/storages/allocators.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/property_graph_cow_state.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/transaction/wal/wal_builder.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"

namespace neug {

class PropertyGraph;
class IWalWriter;
class IVersionManager;
class Schema;

/**
 * @brief Resource holder and lifecycle manager for update transactions.
 *
 * UpdateTransaction owns the COW-cloned PropertyGraph and all associated
 * resources (WAL buffer, allocator, version manager, snapshot store).
 * Graph modification logic (DDL/DML) is implemented by StorageTPUpdateInterface
 * which accesses UpdateTransaction's private members via friend declaration.
 *
 * **COW Design:**
 * - Holds a shared_ptr to a COW-cloned PropertyGraph
 * - StorageTPUpdateInterface performs all DDL/DML modifications on the COW copy
 * - Commit flushes WAL and publishes the COW copy via
 * GraphSnapshotStore::PublishSnapshot()
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
  /**
   * @brief Construct an UpdateTransaction with a COW PropertyGraph.
   *
   * @param cow_graph PropertyGraph COW clone
   * @param alloc Reference to memory allocator
   * @param logger Reference to WAL writer
   * @param vm Reference to version manager
   * @param snapshot_store Reference to GraphSnapshotStore for commit
   * @param cache Reference to query cache
   * @param timestamp Transaction timestamp
   *
   * @note NeugDB is responsible for creating the COW copy via Clone()
   * @since v0.1.0
   */
  UpdateTransaction(std::shared_ptr<PropertyGraph> cow_graph, Allocator& alloc,
                    IWalWriter& logger, IVersionManager& vm,
                    GraphSnapshotStore& snapshot_store,
                    execution::LocalQueryCache& cache, timestamp_t timestamp);

  /**
   * @brief Destructor that calls Abort().
   * @since v0.1.0
   */
  ~UpdateTransaction();

  /**
   * @brief Get the transaction timestamp.
   * @since v0.1.0
   */
  timestamp_t timestamp() const;

  bool Commit();

  void Abort();

  static void IngestWal(PropertyGraph& graph, uint32_t timestamp, char* data,
                        size_t length, Allocator& alloc);

  PropertyGraph& graph() const { return *cow_graph_; }

  const GraphView& view() const { return view_; }

  // --- Read-only accessors (not graph modifications) ---
  const Schema& schema() const { return cow_graph_->schema(); }

  execution::Value GetVertexId(label_t label, vid_t lid) const;

  bool GetVertexIndex(label_t label, const execution::Value& id,
                      vid_t& index) const;

  execution::Value GetVertexProperty(label_t label, vid_t lid,
                                     int col_id) const;

  std::shared_ptr<RefColumnBase> get_vertex_property_column(
      uint8_t label, const std::string& col_name) const {
    return cow_graph_->GetVertexPropertyColumn(label, col_name);
  }

  CsrView GetGenericOutgoingGraphView(label_t v_label, label_t neighbor_label,
                                      label_t edge_label) const {
    return cow_graph_->GetGenericOutgoingGraphView(v_label, neighbor_label,
                                                   edge_label, timestamp_);
  }

  CsrView GetGenericIncomingGraphView(label_t v_label, label_t neighbor_label,
                                      label_t edge_label) const {
    return cow_graph_->GetGenericIncomingGraphView(v_label, neighbor_label,
                                                   edge_label, timestamp_);
  }

  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label, int prop_id) const {
    return cow_graph_->GetEdgeDataAccessor(src_label, dst_label, edge_label,
                                           prop_id);
  }

  friend class StorageTPUpdateInterface;

 private:
  void release();

  // COW storage - the cloned PropertyGraph
  std::shared_ptr<PropertyGraph> cow_graph_;
  PropertyGraphCowState cow_state_;
  GraphView view_;

  Allocator& alloc_;
  IWalWriter& logger_;
  IVersionManager& vm_;
  GraphSnapshotStore& snapshot_store_;
  execution::LocalQueryCache& pipeline_cache_;
  timestamp_t timestamp_;

  std::shared_ptr<Checkpoint> ckp_;
  WalBuilder wal_builder_;
};

class StorageTPUpdateInterface : public StorageUpdateInterface {
 public:
  explicit StorageTPUpdateInterface(UpdateTransaction& txn)
      : StorageUpdateInterface(txn.view(), txn.timestamp()),
        cow_graph_(txn.cow_graph_),
        cow_state_(txn.cow_state_),
        mut_view_(txn.view_),
        alloc_(txn.alloc_),
        ckp_(txn.ckp_),
        wal_(txn.wal_builder_) {}
  ~StorageTPUpdateInterface() = default;

  // --- DML methods ---
  Status UpdateVertexProperty(label_t label, vid_t lid, int col_id,
                              const execution::Value& value) override;
  Status UpdateEdgeProperty(label_t src_label, vid_t src, label_t dst_label,
                            vid_t dst, label_t edge_label, int32_t oe_offset,
                            int32_t ie_offset, int32_t col_id,
                            const execution::Value& value) override;
  Status AddVertex(label_t label, const execution::Value& id,
                   const std::vector<execution::Value>& props,
                   vid_t& vid) override;
  Status AddEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
                 label_t edge_label,
                 const std::vector<execution::Value>& properties,
                 const void*& prop) override;
  Status DeleteVertex(label_t label, vid_t lid) override;
  Status DeleteEdges(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
                     label_t edge_label) override;
  Status DeleteEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
                    label_t edge_label, int32_t oe_offset,
                    int32_t ie_offset) override;

  // --- Batch methods ---
  void CreateCheckpoint() override;
  Status BatchAddVertices(
      label_t v_label_id,
      std::shared_ptr<IDataChunkSupplier> supplier) override;
  Status BatchAddEdges(label_t src_label, label_t dst_label, label_t edge_label,
                       std::shared_ptr<IDataChunkSupplier> supplier) override;
  Status BatchDeleteVertices(label_t v_label_id,
                             const std::vector<vid_t>& vids) override;
  Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::tuple<vid_t, vid_t>>& edges) override;
  Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
      const std::vector<std::pair<vid_t, int32_t>>& ie_edges) override;

  // --- DDL methods ---
  Status CreateVertexType(const CreateVertexTypeParam& config) override;
  Status CreateEdgeType(const CreateEdgeTypeParam& config) override;
  Status AddVertexProperties(const AddVertexPropertiesParam& config) override;
  Status AddEdgeProperties(const AddEdgePropertiesParam& config) override;
  Status RenameVertexProperties(
      const RenameVertexPropertiesParam& config) override;
  Status RenameEdgeProperties(const RenameEdgePropertiesParam& config) override;
  Status DeleteVertexProperties(
      const DeleteVertexPropertiesParam& config) override;
  Status DeleteEdgeProperties(const DeleteEdgePropertiesParam& config) override;
  Status DeleteVertexType(const std::string& vertex_type_name) override;
  Status DeleteEdgeType(const std::string& src_type,
                        const std::string& dst_type,
                        const std::string& edge_type) override;

 private:
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

  std::shared_ptr<PropertyGraph>& cow_graph_;
  PropertyGraphCowState& cow_state_;
  GraphView& mut_view_;
  Allocator& alloc_;
  std::shared_ptr<Checkpoint>& ckp_;
  WalBuilder& wal_;
};

}  // namespace neug
