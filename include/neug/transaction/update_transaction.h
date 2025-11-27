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

#ifndef INCLUDE_NEUG_TRANSACTION_UPDATE_TRANSACTION_H_
#define INCLUDE_NEUG_TRANSACTION_UPDATE_TRANSACTION_H_

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
#include "libgrape-lite/grape/serialization/in_archive.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/transaction/undo_log.h"
#include "neug/utils/allocators.h"
#include "neug/utils/id_indexer.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"

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

namespace gs {

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
 * - insert_vertex_with_resize_ controls whether to resize vertex columns
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
   * Implementation: Stores references and initializes
   * insert_vertex_with_resize_=false.
   *
   * @since v0.1.0
   */
  UpdateTransaction(PropertyGraph& graph, Allocator& alloc,
                    const std::string& work_dir, IWalWriter& logger,
                    IVersionManager& vm, timestamp_t timestamp);

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

  bool CreateVertexType(
      const std::string& name,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          properties,
      const std::vector<std::string>& primary_key_names,
      bool error_on_conflict);

  bool CreateEdgeType(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          properties,
      bool error_on_conflict, EdgeStrategy oe_edge_strategy,
      EdgeStrategy ie_edge_strategy);

  bool AddVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          add_properties,
      bool error_on_conflict);

  bool AddEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          add_properties,
      bool error_on_conflict);

  bool RenameVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict);

  bool RenameEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict);

  bool DeleteVertexProperties(const std::string& vertex_type_name,
                              const std::vector<std::string>& delete_properties,
                              bool error_on_conflict);

  bool DeleteEdgeProperties(const std::string& src_type,
                            const std::string& dst_type,
                            const std::string& edge_type,
                            const std::vector<std::string>& delete_properties,
                            bool error_on_conflict);

  bool DeleteVertexType(const std::string& vertex_type_name,
                        bool error_on_conflict = true);

  bool DeleteEdgeType(const std::string& src_type, const std::string& dst_type,
                      const std::string& edge_type, bool error_on_conflict);

  bool AddVertex(label_t label, const Property& oid,
                 const std::vector<Property>& props, vid_t& vid);

  bool DeleteVertex(label_t label, vid_t lid);

  bool AddEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
               label_t edge_label, const std::vector<Property>& properties);

  VertexSet GetVertexSet(label_t label) const;

  std::shared_ptr<RefColumnBase> get_vertex_property_column(
      uint8_t label, const std::string& col_name) const {
    ENSURE_VERTEX_LABEL_NOT_DELETED(label);
    ENSURE_VERTEX_PROPERTY_NOT_DELETED(label, col_name);
    return graph_.GetVertexPropertyColumn(label, col_name);
  }

  class edge_iterator {
   public:
    edge_iterator(bool dir, label_t label, vid_t v, label_t neighbor_label,
                  label_t edge_label, const vid_t* aeb, const vid_t* aee,
                  const NbrList& edges, const EdgeDataAccessor& ed_accessor,
                  int32_t prop_id, UpdateTransaction* txn);
    ~edge_iterator();

    Property GetData() const;

    void SetData(const Property& value);

    bool IsValid() const;

    void Next();

    void Forward(size_t offset);

    vid_t GetNeighbor() const;

    label_t GetNeighborLabel() const;

    label_t GetEdgeLabel() const;

   private:
    bool dir_;

    label_t label_;
    vid_t v_;

    label_t neighbor_label_;
    label_t edge_label_;

    const vid_t* added_edges_cur_;
    const vid_t* added_edges_end_;

    NbrIterator init_iter_;
    NbrIterator init_iter_end_;
    EdgeDataAccessor ed_accessor_;
    int32_t prop_id_;

    UpdateTransaction* txn_;
    size_t offset_;
  };

  edge_iterator GetOutEdgeIterator(label_t label, vid_t u,
                                   label_t neighbor_label, label_t edge_label,
                                   int prop_id);

  edge_iterator GetInEdgeIterator(label_t label, vid_t u,
                                  label_t neighbor_label, label_t edge_label,
                                  int prop_id);

  Property GetVertexProperty(label_t label, vid_t lid, int col_id) const;

  bool UpdateVertexProperty(label_t label, vid_t lid, int col_id,
                            const Property& value);

  // set col_id = -1 to set the whole edge data
  void SetEdgeData(bool dir, label_t label, vid_t v, label_t neighbor_label,
                   vid_t nbr, label_t edge_label, const Property& value,
                   int32_t col_id = 0);

  bool GetUpdatedEdgeData(bool dir, label_t label, vid_t v,
                          label_t neighbor_label, vid_t nbr, label_t edge_label,
                          int32_t prop_id, Property& ret) const;

  static void IngestWal(PropertyGraph& graph, const std::string& work_dir,
                        uint32_t timestamp, char* data, size_t length,
                        Allocator& alloc);
  Property GetVertexId(label_t label, vid_t lid) const;

  bool GetVertexIndex(label_t label, const Property& id, vid_t& index) const;

  PropertyGraph& graph() const { return graph_; }

  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label, int prop_id) const {
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

  // TODO(zhanglei): Remove batch method from UpdateTransaction after
  // refactoring GraphInterface.
  inline Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::tuple<vid_t, vid_t>>& edges_vec) {
    ENSURE_EDGE_LABEL_NOT_DELETED(src_v_label_id, dst_v_label_id,
                                  edge_label_id);
    return graph_.BatchDeleteEdges(src_v_label_id, dst_v_label_id,
                                   edge_label_id, edges_vec);
  }

  inline Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
      const std::vector<std::pair<vid_t, int32_t>>& ie_edges) {
    ENSURE_EDGE_LABEL_NOT_DELETED(src_v_label_id, dst_v_label_id,
                                  edge_label_id);
    return graph_.BatchDeleteEdges(src_v_label_id, dst_v_label_id,
                                   edge_label_id, oe_edges, ie_edges);
  }

  inline std::string work_dir() const { return graph_.work_dir(); }

 private:
  void set_edge_data_with_offset(bool dir, label_t label, vid_t v,
                                 label_t neighbor_label, vid_t nbr,
                                 label_t edge_label, const Property& value,
                                 size_t offset, int32_t col_id = 0);

  size_t get_in_csr_index(label_t src_label, label_t dst_label,
                          label_t edge_label) const;

  size_t get_out_csr_index(label_t src_label, label_t dst_label,
                           label_t edge_label) const;

  bool IsValidLid(label_t label, vid_t lid) const;

  void release();

  void applyVerticesUpdates();

  void applyEdgesUpdates();

  void applyVertexTypeDeletions();

  void applyEdgeTypeDeletions();

  void applyVertexPropDeletion();

  void applyEdgePropDeletion();

  Property own_property_memory(const Property& prop);

  // Revert all changes made in this transaction.
  void revert_changes();

  PropertyGraph& graph_;
  Allocator& alloc_;
  IWalWriter& logger_;
  IVersionManager& vm_;
  timestamp_t timestamp_;

  grape::InArchive arc_;
  int op_num_;

  size_t vertex_label_num_;
  size_t edge_label_num_;

  std::vector<vid_t> added_vertices_base_;

  std::vector<ska::flat_hash_map<vid_t, std::vector<vid_t>>> added_edges_;
  std::vector<ska::flat_hash_map<
      vid_t, ska::flat_hash_map<
                 vid_t, std::vector<std::tuple<Property, int32_t, size_t>>>>>
      updated_edge_data_;

  std::vector<std::string> sv_vec_;
  std::unordered_set<label_t> deleted_vertex_labels_;
  std::unordered_set<std::tuple<label_t, label_t, label_t>,
                     hash_tuple::hash<label_t, label_t, label_t>>
      deleted_edge_labels_;
  std::vector<std::unordered_set<std::string>> deleted_vertex_properties_;
  std::unordered_map<uint32_t, std::unordered_set<std::string>>
      deleted_edge_properties_;
  std::stack<std::unique_ptr<IUndoLog>> undo_logs_;
};

}  // namespace gs

#endif  // INCLUDE_NEUG_TRANSACTION_UPDATE_TRANSACTION_H_
