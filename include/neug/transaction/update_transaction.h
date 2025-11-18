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
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "flat_hash_map/flat_hash_map.hpp"
#include "libgrape-lite/grape/serialization/in_archive.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/utils/allocators.h"
#include "neug/utils/id_indexer.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"

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
   * @brief Configure whether to resize vertex columns during property updates.
   *
   * By default, update transactions will not resize vertex columns when
   * updating properties. Setting this to true enables column resizing if
   * needed.
   *
   * @param insert_vertex_with_resize true to enable column resizing, false to
   * disable
   *
   * Implementation: Sets insert_vertex_with_resize_ member variable.
   *
   * @since v0.1.0
   */
  void set_insert_vertex_with_resize(bool insert_vertex_with_resize);

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

  bool AddVertex(label_t label, const Property& oid,
                 const std::vector<Property>& props);

  bool AddVertex(label_t label, const Property& oid,
                 const std::vector<Property>& props, vid_t& vid);

  bool AddEdge(label_t src_label, const Property& src, label_t dst_label,
               const Property& dst, label_t edge_label,
               const std::vector<Property>& properties);

  bool AddEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
               label_t edge_label, const std::vector<Property>& properties);

  class vertex_iterator {
   public:
    vertex_iterator(label_t label, vid_t cur, vid_t& num, timestamp_t ts,
                    UpdateTransaction* txn);
    ~vertex_iterator();
    bool IsValid() const;
    void Next();
    void Goto(vid_t target);

    Property GetId() const;

    vid_t GetIndex() const;

    Property GetField(int col_id) const;

    bool SetField(int col_id, const Property& value);

   private:
    label_t label_;
    vid_t cur_;

    vid_t& num_;
    UpdateTransaction* txn_;
  };

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

  vertex_iterator GetVertexIterator(label_t label);

  vid_t GetVertexNum(label_t label) const;

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

  PropertyGraph& GetGraph() const { return graph_; }

  /**
   * @brief Check if the vertex with the given label and oid exists.
   * @param label The label of the vertex.
   * @param oid The oid of the vertex.
   * @return true if the vertex exists, false otherwise.
   */
  bool HasVertex(label_t label, const Property& oid) const;

  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label, int prop_id) const {
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
  inline Status BatchAddVertices(
      label_t v_label_id, std::shared_ptr<IRecordBatchSupplier> supplier) {
    return graph_.BatchAddVertices(v_label_id, supplier);
  }

  // Also executed in batch mode
  inline Status BatchAddEdges(label_t src_label, label_t dst_label,
                              label_t edge_label,
                              std::shared_ptr<IRecordBatchSupplier> supplier) {
    return graph_.BatchAddEdges(src_label, dst_label, edge_label,
                                std::move(supplier));
  }

  // Also executed in batch mode
  inline Status BatchDeleteVertices(label_t v_label_id,
                                    const std::vector<vid_t>& vids) {
    return graph_.BatchDeleteVertices(v_label_id, vids);
  }

  // Also executed in batch mode
  inline Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::tuple<vid_t, vid_t>>& edges) {
    return graph_.BatchDeleteEdges(src_v_label_id, dst_v_label_id,
                                   edge_label_id, edges);
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

  bool oid_to_lid(label_t label, const Property& oid, vid_t& lid) const;

  Property lid_to_oid(label_t label, vid_t lid) const;

  bool IsValidLid(label_t label, vid_t lid) const;

  void release();

  void applyVerticesUpdates();

  void applyEdgesUpdates();

  Property own_property_memory(const Property& prop);

  bool insert_vertex_with_resize_;

  PropertyGraph& graph_;
  Allocator& alloc_;
  IWalWriter& logger_;
  IVersionManager& vm_;
  timestamp_t timestamp_;

  grape::InArchive arc_;
  int op_num_;

  size_t vertex_label_num_;
  size_t edge_label_num_;

  std::vector<std::shared_ptr<IdIndexerBase<vid_t>>> added_vertices_;
  std::vector<vid_t> added_vertices_base_;
  std::vector<vid_t> vertex_nums_;
  std::vector<ska::flat_hash_map<vid_t, vid_t>> vertex_offsets_;
  std::vector<Table> extra_vertex_properties_;

  std::vector<ska::flat_hash_map<vid_t, std::vector<vid_t>>> added_edges_;
  std::vector<ska::flat_hash_map<
      vid_t, ska::flat_hash_map<
                 vid_t, std::vector<std::tuple<Property, int32_t, size_t>>>>>
      updated_edge_data_;

  std::vector<std::string> sv_vec_;
};

}  // namespace gs

#endif  // INCLUDE_NEUG_TRANSACTION_UPDATE_TRANSACTION_H_
