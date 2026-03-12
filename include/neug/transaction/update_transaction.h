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
#include "neug/execution/execute/query_cache.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/transaction/undo_log.h"
#include "neug/utils/allocators.h"
#include "neug/utils/id_indexer.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/in_archive.h"

#define ENSURE_VERTEX_LABEL_NOT_DELETED(label)                            \
  do {                                                                    \
    if (deleted_vertex_labels_.count(label)) {                            \
      THROW_RUNTIME_ERROR("Vertex label is deleted in this transaction"); \
    }                                                                     \
  } while (0)

#define ENSURE_EDGE_LABEL_NOT_DELETED(src_label, dst_label, edge_label) \
  do {                                                                  \
    if (deleted_edge_labels_.count(                                     \
            std::make_tuple(src_label, dst_label, edge_label))) {       \
      THROW_RUNTIME_ERROR("Edge label is deleted in this transaction"); \
    }                                                                   \
  } while (0)

#define ENSURE_VERTEX_PROPERTY_NOT_DELETED(label, prop_name)                 \
  do {                                                                       \
    if (label < deleted_vertex_properties_.size() &&                         \
        deleted_vertex_properties_[label].count(prop_name)) {                \
      THROW_RUNTIME_ERROR("Vertex property is deleted in this transaction"); \
    }                                                                        \
  } while (0)

#define ENSURE_EDGE_PROPERTY_NOT_DELETED(index, prop_name)                 \
  do {                                                                     \
    if (deleted_edge_properties_.find(index) !=                            \
            deleted_edge_properties_.end() &&                              \
        deleted_edge_properties_.at(index).count(prop_name)) {             \
      THROW_RUNTIME_ERROR("Edge property is deleted in this transaction"); \
    }                                                                      \
  } while (0)

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
 * guarantees. It supports updating vertex properties, edge properties, and
 * provides options for vertex column resizing during updates.
 *
 * **Key Features:**
 * - Update vertex and edge properties
 * - Configurable vertex column resizing behavior
 * - Write-Ahead Logging for durability
 * - MVCC support with timestamp management
 * - Commit/abort transaction semantics
 *
 * **Implementation Details:**
 * - Uses work_dir for temporary storage during updates
 * - Destructor calls release() for cleanup
 * - Integrates with version manager for timestamp coordination
 *
 * @since v0.1.0
 */
class UpdateTransaction {
 public:
  /**
   * @brief Construct an UpdateTransaction.
   *
   * @param session Reference to the database session
   * @param graph Reference to the property graph (mutable for updates)
   * @param alloc Reference to memory allocator
   * @param work_dir Working directory for temporary files
   * @param logger Reference to WAL writer
   * @param vm Reference to version manager
   * @param timestamp Transaction timestamp
   *
   * @since v0.1.0
   */
  UpdateTransaction(PropertyGraph& graph, Allocator& alloc, IWalWriter& logger,
                    IVersionManager& vm, execution::LocalQueryCache& cache,
                    timestamp_t timestamp);

  /**
   * @brief Destructor that calls release().
   *
   * Implementation: Calls release() to clean up resources and release
   * timestamp.
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
   * @return const Schema& Reference to the graph schema
   *
   * Implementation: Returns graph_.schema().
   *
   * @since v0.1.0
   */
  const Schema& schema() const { return graph_.schema(); }

  bool Commit();

  void Abort();

  Status CreateVertexType(
      const std::string& name,
      const std::vector<std::tuple<DataType, std::string, Property>>&
          properties,
      const std::vector<std::string>& primary_key_names,
      bool error_on_conflict);

  Status CreateEdgeType(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::tuple<DataType, std::string, Property>>&
          properties,
      bool error_on_conflict, EdgeStrategy oe_edge_strategy,
      EdgeStrategy ie_edge_strategy);

  Status AddVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::tuple<DataType, std::string, Property>>&
          add_properties,
      bool error_on_conflict);

  Status AddEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::tuple<DataType, std::string, Property>>&
          add_properties,
      bool error_on_conflict);

  Status RenameVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict);

  Status RenameEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict);

  Status DeleteVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::string>& delete_properties,
      bool error_on_conflict);

  Status DeleteEdgeProperties(const std::string& src_type,
                              const std::string& dst_type,
                              const std::string& edge_type,
                              const std::vector<std::string>& delete_properties,
                              bool error_on_conflict);

  Status DeleteVertexType(const std::string& vertex_type_name,
                          bool error_on_conflict = true);

  Status DeleteEdgeType(const std::string& src_type,
                        const std::string& dst_type,
                        const std::string& edge_type, bool error_on_conflict);

  bool AddVertex(label_t label, const Property& oid,
                 const std::vector<Property>& props, vid_t& vid);

  /**
   * @brief Delete vertex and its associated edges.
   * @param label Vertex label id.
   * @param lid Local vertex id.
   * @return true if deletion is successful, false otherwise.
   * @note We always delete vertex in detach mode.
   */
  bool DeleteVertex(label_t label, vid_t lid);

  bool AddEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
               label_t edge_label, const std::vector<Property>& properties);

  bool DeleteEdges(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
                   label_t edge_label);

  bool DeleteEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
                  label_t edge_label, int32_t oe_offset, int32_t ie_offset);

  std::shared_ptr<RefColumnBase> get_vertex_property_column(
      uint8_t label, const std::string& col_name) const {
    ENSURE_VERTEX_LABEL_NOT_DELETED(label);
    ENSURE_VERTEX_PROPERTY_NOT_DELETED(label, col_name);
    return graph_.GetVertexPropertyColumn(label, col_name);
  }

  Property GetVertexProperty(label_t label, vid_t lid, int col_id) const;

  bool UpdateVertexProperty(label_t label, vid_t lid, int col_id,
                            const Property& value);

  bool UpdateEdgeProperty(label_t src_label, vid_t src, label_t dst_label,
                          vid_t dst, label_t edge_label, int32_t col_id,
                          const Property& value);

  bool UpdateEdgeProperty(label_t src_label, vid_t src, label_t dst_label,
                          vid_t dst, label_t edge_label, int32_t oe_offset,
                          int32_t ie_offset, int32_t col_id,
                          const Property& value);

  GenericView GetGenericOutgoingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    ENSURE_VERTEX_LABEL_NOT_DELETED(v_label);
    ENSURE_VERTEX_LABEL_NOT_DELETED(neighbor_label);
    ENSURE_EDGE_LABEL_NOT_DELETED(v_label, neighbor_label, edge_label);
    return graph_.GetGenericOutgoingGraphView(v_label, neighbor_label,
                                              edge_label, timestamp_);
  }

  GenericView GetGenericIncomingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    ENSURE_VERTEX_LABEL_NOT_DELETED(v_label);
    ENSURE_VERTEX_LABEL_NOT_DELETED(neighbor_label);
    ENSURE_EDGE_LABEL_NOT_DELETED(neighbor_label, v_label, edge_label);
    return graph_.GetGenericIncomingGraphView(v_label, neighbor_label,
                                              edge_label, timestamp_);
  }

  static void IngestWal(PropertyGraph& graph, const std::string& work_dir,
                        uint32_t timestamp, char* data, size_t length,
                        Allocator& alloc);
  Property GetVertexId(label_t label, vid_t lid) const;

  bool GetVertexIndex(label_t label, const Property& id, vid_t& index) const;

  PropertyGraph& graph() const { return graph_; }

  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label, int prop_id) const {
    ENSURE_VERTEX_LABEL_NOT_DELETED(src_label);
    ENSURE_VERTEX_LABEL_NOT_DELETED(dst_label);
    ENSURE_EDGE_LABEL_NOT_DELETED(src_label, dst_label, edge_label);
    auto edge_schema =
        graph_.schema().get_edge_schema(src_label, dst_label, edge_label);
    if (edge_schema->property_names.size() <= static_cast<size_t>(prop_id)) {
      if (edge_schema->property_names.size() != 0) {
        THROW_RUNTIME_ERROR("Invalid edge property id");
      }
    }
    auto index =
        graph_.schema().generate_edge_label(src_label, dst_label, edge_label);
    ENSURE_EDGE_PROPERTY_NOT_DELETED(index,
                                     edge_schema->property_names[prop_id]);
    return graph_.GetEdgeDataAccessor(src_label, dst_label, edge_label,
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
    ENSURE_VERTEX_LABEL_NOT_DELETED(v_label_id);
    return graph_.BatchAddVertices(v_label_id, supplier);
  }

  // TODO(zhanglei): Remove batch method from UpdateTransaction after
  // refactoring GraphInterface.
  inline Status BatchAddEdges(label_t src_label, label_t dst_label,
                              label_t edge_label,
                              std::shared_ptr<IRecordBatchSupplier> supplier) {
    ENSURE_VERTEX_LABEL_NOT_DELETED(src_label);
    ENSURE_VERTEX_LABEL_NOT_DELETED(dst_label);
    ENSURE_EDGE_LABEL_NOT_DELETED(src_label, dst_label, edge_label);
    return graph_.BatchAddEdges(src_label, dst_label, edge_label,
                                std::move(supplier));
  }

  // TODO(zhanglei): Remove batch method from UpdateTransaction after
  // refactoring GraphInterface.
  inline Status BatchDeleteVertices(label_t v_label_id,
                                    const std::vector<vid_t>& vids) {
    ENSURE_VERTEX_LABEL_NOT_DELETED(v_label_id);
    return graph_.BatchDeleteVertices(v_label_id, vids);
  }

 private:
  bool IsValidLid(label_t label, vid_t lid) const;

  void release();

  void applyVertexTypeDeletions();

  void applyEdgeTypeDeletions();

  void applyVertexPropDeletion();

  void applyEdgePropDeletion();

  void invalidate_query_cache_if_needed();

  // Revert all changes made in this transaction.
  void revert_changes();

  PropertyGraph& graph_;
  Allocator& alloc_;
  IWalWriter& logger_;
  IVersionManager& vm_;
  execution::LocalQueryCache& pipeline_cache_;
  timestamp_t timestamp_;

  InArchive arc_;
  int op_num_;
  bool schema_changed_{false};

  std::unordered_set<label_t> deleted_vertex_labels_;
  std::unordered_set<std::tuple<label_t, label_t, label_t>,
                     hash_tuple::hash<label_t, label_t, label_t>>
      deleted_edge_labels_;
  std::vector<std::unordered_set<std::string>> deleted_vertex_properties_;
  std::unordered_map<uint32_t, std::unordered_set<std::string>>
      deleted_edge_properties_;
  std::stack<std::unique_ptr<IUndoLog>> undo_logs_;
};

class StorageTPUpdateInterface : public StorageUpdateInterface {
 public:
  explicit StorageTPUpdateInterface(UpdateTransaction& txn)
      : StorageUpdateInterface(txn.graph(), txn.timestamp()), txn_(txn) {}
  ~StorageTPUpdateInterface() {}

  inline void UpdateVertexProperty(label_t label, vid_t lid, int col_id,
                                   const Property& value) override {
    txn_.UpdateVertexProperty(label, lid, col_id, value);
  }

  inline void UpdateEdgeProperty(label_t src_label, vid_t src,
                                 label_t dst_label, vid_t dst,
                                 label_t edge_label, int32_t oe_offset,
                                 int32_t ie_offset, int32_t col_id,
                                 const Property& value) override {
    txn_.UpdateEdgeProperty(src_label, src, dst_label, dst, edge_label,
                            oe_offset, ie_offset, col_id, value);
  }

  inline bool AddVertex(label_t label, const Property& id,
                        const std::vector<Property>& props,
                        vid_t& vid) override {
    return txn_.AddVertex(label, id, props, vid);
  }

  inline bool AddEdge(label_t src_label, vid_t src, label_t dst_label,
                      vid_t dst, label_t edge_label,
                      const std::vector<Property>& properties) override {
    return txn_.AddEdge(src_label, src, dst_label, dst, edge_label, properties);
  }
  void CreateCheckpoint() override;
  inline UpdateTransaction& GetTransaction() { return txn_; }
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
  Status CreateVertexType(
      const std::string& name,
      const std::vector<std::tuple<DataType, std::string, Property>>&
          properties,
      const std::vector<std::string>& primary_key_names,
      bool error_on_conflict) override;
  Status CreateEdgeType(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::tuple<DataType, std::string, Property>>&
          properties,
      bool error_on_conflict, EdgeStrategy oe_edge_strategy,
      EdgeStrategy ie_edge_strategy) override;
  Status AddVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::tuple<DataType, std::string, Property>>&
          add_properties,
      bool error_on_conflict) override;
  Status AddEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::tuple<DataType, std::string, Property>>&
          add_properties,
      bool error_on_conflict) override;
  Status RenameVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict) override;
  Status RenameEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict) override;
  Status DeleteVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::string>& delete_properties,
      bool error_on_conflict) override;
  Status DeleteEdgeProperties(const std::string& src_type,
                              const std::string& dst_type,
                              const std::string& edge_type,
                              const std::vector<std::string>& delete_properties,
                              bool error_on_conflict) override;
  Status DeleteVertexType(const std::string& vertex_type_name,
                          bool error_on_conflict = true) override;
  Status DeleteEdgeType(const std::string& src_type,
                        const std::string& dst_type,
                        const std::string& edge_type,
                        bool error_on_conflict) override;

 private:
  UpdateTransaction& txn_;
};

}  // namespace neug
