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
#include <map>
#include <memory>
#include <stack>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "flat_hash_map/flat_hash_map.hpp"
#include "neug/common/extra_type_info.h"
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
#include "neug/utils/id_indexer.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/in_archive.h"

namespace neug {

class PropertyGraph;
class IWalWriter;
class IVersionManager;
class Schema;
template <typename INDEX_T>
class IdIndexerBase;

/**
 * @brief Transaction for updating existing graph elements (vertices and edges).
 *
 * UpdateTransaction handles modifications to existing graph data with ACID
 * guarantees using Copy-on-Write (COW) for snapshot isolation.
 *
 * **COW Design:**
 * - Holds a shared_ptr to a COW-cloned PropertyGraph
 * - All DDL/DML modifications happen immediately on the COW copy
 * - Commit returns the COW copy for GraphSnapshotStore::PublishSnapshot()
 * - Abort discards the COW copy (no effect on original)
 *
 * **Key Features:**
 * - Update vertex and edge properties
 * - DDL operations (create/delete types, add/delete properties)
 * - Write-Ahead Logging for durability
 * - MVCC support with timestamp management
 * - Lazy materialization for efficient COW
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
   * @note NeugDB is responsible for creating the COW copy via
   * CloneForCow()
   * @since v0.1.0
   */
  UpdateTransaction(std::shared_ptr<PropertyGraph> cow_graph, Allocator& alloc,
                    IWalWriter& logger, IVersionManager& vm,
                    GraphSnapshotStore& snapshot_store,
                    execution::LocalQueryCache& cache, timestamp_t timestamp);

  /**
   * @brief Destructor that calls Abort().
   *
   * @since v0.1.0
   */
  ~UpdateTransaction();

  /**
   * @brief Get the transaction timestamp.
   *
   * @return timestamp_t The timestamp for this transaction
   *
   * Implementation: Returns timestamp_ member variable.
   *
   * @since v0.1.0
   */
  timestamp_t timestamp() const;

  /**
   * @brief Get read-only access to the graph schema.
   *
   * @since v0.1.0
   */
  const Schema& schema() const { return cow_graph_->schema(); }

  bool Commit();

  void Abort();

  Status CreateVertexType(const CreateVertexTypeParam& config);

  Status CreateEdgeType(const CreateEdgeTypeParam& config);

  Status AddVertexProperties(const AddVertexPropertiesParam& config);

  Status AddEdgeProperties(const AddEdgePropertiesParam& config);

  Status RenameVertexProperties(const RenameVertexPropertiesParam& config);

  Status RenameEdgeProperties(const RenameEdgePropertiesParam& config);

  Status DeleteVertexProperties(const DeleteVertexPropertiesParam& config);

  Status DeleteEdgeProperties(const DeleteEdgePropertiesParam& config);

  Status DeleteVertexType(const std::string& vertex_type_name);

  Status DeleteEdgeType(const std::string& src_type,
                        const std::string& dst_type,
                        const std::string& edge_type);

  Status AddVertex(label_t label, const execution::Value& oid,
                   const std::vector<execution::Value>& props, vid_t& vid);

  /**
   * @brief Delete vertex and its associated edges.
   * @param label Vertex label id.
   * @param lid Local vertex id.
   * @return true if deletion is successful, false otherwise.
   * @note We always delete vertex in detach mode.
   */
  bool DeleteVertex(label_t label, vid_t lid);

  Status AddEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
                 label_t edge_label,
                 const std::vector<execution::Value>& properties,
                 const void*& prop);

  bool DeleteEdges(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
                   label_t edge_label);

  bool DeleteEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
                  label_t edge_label, int32_t oe_offset, int32_t ie_offset);

  std::shared_ptr<RefColumnBase> get_vertex_property_column(
      uint8_t label, const std::string& col_name) const {
    return cow_graph_->GetVertexPropertyColumn(label, col_name);
  }

  execution::Value GetVertexProperty(label_t label, vid_t lid,
                                     int col_id) const;

  bool UpdateVertexProperty(label_t label, vid_t lid, int col_id,
                            const execution::Value& value);

  bool UpdateEdgeProperty(label_t src_label, vid_t src, label_t dst_label,
                          vid_t dst, label_t edge_label, int32_t col_id,
                          const execution::Value& value);

  bool UpdateEdgeProperty(label_t src_label, vid_t src, label_t dst_label,
                          vid_t dst, label_t edge_label, int32_t oe_offset,
                          int32_t ie_offset, int32_t col_id,
                          const execution::Value& value);

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

  static void IngestWal(PropertyGraph& graph, uint32_t timestamp, char* data,
                        size_t length, Allocator& alloc);
  execution::Value GetVertexId(label_t label, vid_t lid) const;

  bool GetVertexIndex(label_t label, const execution::Value& id,
                      vid_t& index) const;

  PropertyGraph& graph() const { return *cow_graph_; }

  const GraphView& view() const { return view_; }

  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label, int prop_id) const {
    return cow_graph_->GetEdgeDataAccessor(src_label, dst_label, edge_label,
                                           prop_id);
  }

  void CreateCheckpoint();

  /**
   * Batch add vertices with given label, ids and property table.
   * We will not generate wal log for this operation. Assume the ids and
   * table are valid.
   * @param v_label_id The label id of the vertices to be added.
   * @param ids The ids of the vertices to be added.
   * @param table The property table of the vertices to be added.
   * @return Status
   */
  // TODO(zhanglei): Remove batch method from UpdateTransaction after
  // refactoring GraphInterface.
  inline Status BatchAddVertices(
      label_t v_label_id, std::shared_ptr<IRecordBatchSupplier> supplier) {
    // TODO(zhanglei): Currently not supported in TP mode. If in the future we
    // support TP mode, we need to also materialize the edge table and ensure
    // adjlists are mutable here.
    return cow_graph_->BatchAddVertices(v_label_id, supplier);
  }

  // TODO(zhanglei): Remove batch method from UpdateTransaction after
  // refactoring GraphInterface.
  inline Status BatchAddEdges(label_t src_label, label_t dst_label,
                              label_t edge_label,
                              std::shared_ptr<IRecordBatchSupplier> supplier) {
    // TODO(zhanglei): Currently not supported in TP mode. If in the future we
    // support TP mode, we need to also materialize the edge table and ensure
    // adjlists are mutable here.
    return cow_graph_->BatchAddEdges(src_label, dst_label, edge_label,
                                     std::move(supplier));
  }

  // TODO(zhanglei): Remove batch method from UpdateTransaction after
  // refactoring GraphInterface.
  Status BatchDeleteVertices(label_t v_label_id,
                             const std::vector<vid_t>& vids);

 private:
  bool IsValidLid(label_t label, vid_t lid) const;

  void release();

  // --- PropertyGraphCowState-driven lazy materialization helpers ---
  void MaterializeVertexTableForInsert(label_t label);
  void MaterializeVertexTableForDelete(label_t label);
  void MaterializeVertexColumnForWrite(label_t label, int32_t col_id);
  void MaterializeEdgeTableForInsert(uint32_t edge_triplet_id);
  void MaterializeEdgeTableForDelete(uint32_t edge_triplet_id);
  void MaterializeEdgeColumnForWrite(uint32_t edge_triplet_id, int32_t col_id);
  void MaterializeAdjlistsForWrite(uint32_t edge_triplet_id, vid_t src_lid,
                                   vid_t dst_lid, Allocator& alloc);
  void PrepareVertexCapacityForWrite(label_t label, size_t capacity);
  void PrepareEdgeCapacityForWrite(label_t src_label, label_t dst_label,
                                   label_t edge_label, size_t capacity);

  /// Prepare COW for deleting vertices: materialize vertex timestamp, related
  /// edge CSRs, and per-vertex adjlists so that the storage layer can safely
  /// mutate them under snapshot isolation.
  void PrepareVertexDeleteForWrite(label_t label,
                                   const std::vector<vid_t>& lids);

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

  // Materialization context (from cow_graph_)
  std::shared_ptr<Checkpoint> ckp_;
  InArchive arc_;
  int op_num_;

  // Set by any successful DDL method. When true, Commit calls
  // pipeline_cache_.clearGlobalCache(...) after PublishSnapshot to bump the
  // GlobalQueryCache version + refresh planner meta. Pure DML transactions
  // leave this false to skip cache invalidation.
  bool schema_changed_{false};
  bool checkpoint_created_{false};
};

class StorageTPUpdateInterface : public StorageUpdateInterface {
 public:
  explicit StorageTPUpdateInterface(UpdateTransaction& txn)
      : StorageUpdateInterface(txn.view(), txn.graph(), txn.timestamp()),
        txn_(txn) {}
  ~StorageTPUpdateInterface() {}

  void UpdateVertexProperty(label_t label, vid_t lid, int col_id,
                            const execution::Value& value) override {
    txn_.UpdateVertexProperty(label, lid, col_id, value);
  }

  void UpdateEdgeProperty(label_t src_label, vid_t src, label_t dst_label,
                          vid_t dst, label_t edge_label, int32_t oe_offset,
                          int32_t ie_offset, int32_t col_id,
                          const execution::Value& value) override {
    txn_.UpdateEdgeProperty(src_label, src, dst_label, dst, edge_label,
                            oe_offset, ie_offset, col_id, value);
  }

  Status AddVertex(label_t label, const execution::Value& id,
                   const std::vector<execution::Value>& props,
                   vid_t& vid) override {
    return txn_.AddVertex(label, id, props, vid);
  }

  Status AddEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
                 label_t edge_label,
                 const std::vector<execution::Value>& properties,
                 const void*& prop) override {
    return txn_.AddEdge(src_label, src, dst_label, dst, edge_label, properties,
                        prop);
  }
  void CreateCheckpoint() override;

  Status BatchAddVertices(
      label_t v_label_id,
      std::shared_ptr<IRecordBatchSupplier> supplier) override;
  Status BatchAddEdges(label_t src_label, label_t dst_label, label_t edge_label,
                       std::shared_ptr<IRecordBatchSupplier> supplier) override;
  Status BatchDeleteVertices(label_t v_label_id,
                             const std::vector<vid_t>& vids) override;
  Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::tuple<vid_t, vid_t>>& edges) override;
  Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
      const std::vector<std::pair<vid_t, int32_t>>& ie_edges) override;
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
  UpdateTransaction& txn_;
};

}  // namespace neug
