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
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/allocators.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/in_archive.h"

namespace neug {

class PropertyGraph;
class IWalWriter;
class IVersionManager;
class Schema;

/**
 * @brief Transaction for inserting new vertices and edges into the graph.
 *
 * InsertTransaction handles the insertion of new graph elements with ACID
 * guarantees. It maintains a Write-Ahead Log (WAL) for durability and tracks
 * added vertices to resolve edge insertions that reference newly added
 * vertices.
 *
 * **Key Features:**
 * - Write-Ahead Logging for durability
 * - Vertex insertion with property validation
 * - Edge insertion with vertex existence checking
 * - Automatic vertex resolution for new edges
 * - Transaction commit/abort semantics
 *
 * **Implementation Details:**
 * - Uses OutArchive for serializing operations to WAL
 * - Maintains added_vertices_ map to track new vertices
 * - Destructor calls Abort() for cleanup
 * - Validates property types against schema
 *
 * @since v0.1.0
 */
class InsertTransaction {
 public:
  /**
   * @brief Construct an InsertTransaction.
   *
   * @param session Reference to the database session
   * @param graph Reference to the property graph (mutable for insertions)
   * @param alloc Reference to memory allocator
   * @param logger Reference to WAL writer
   * @param vm Reference to version manager
   * @param timestamp Transaction timestamp
   *
   * Implementation: Stores references and initializes WAL archive with
   * WalHeader.
   *
   * @since v0.1.0
   */
  InsertTransaction(PropertyGraph& graph, Allocator& alloc, IWalWriter& logger,
                    IVersionManager& vm, timestamp_t timestamp);

  /**
   * @brief Destructor that calls Abort().
   *
   * Implementation: Calls Abort() to ensure proper cleanup and release
   * resources.
   *
   * @since v0.1.0
   */
  ~InsertTransaction();

  /**
   * @brief Add a new vertex to the transaction.
   *
   * Validates properties against schema and serializes the vertex insertion to
   * WAL. Tracks the added vertex for later edge resolution.
   *
   * @param label Vertex label/type
   * @param id Vertex primary key value
   * @param props Vector of property values matching schema order
   * @return true if vertex added successfully, false if validation fails
   *
   * Implementation: Validates property count against schema, serializes
   * operation to arc_ with op_type=0, adds vertex to added_vertices_ tracking
   * map.
   *
   * @since v0.1.0
   */
  bool AddVertex(label_t label, const Property& id,
                 const std::vector<Property>& props, vid_t& vid);

  /**
   * @brief Add a new edge to the transaction.
   *
   * Checks for existence of source and destination vertices (including newly
   * added ones), then serializes the edge insertion to WAL.
   *
   * @param src_label Source vertex label
   * @param src Source vertex ID
   * @param dst_label Destination vertex label
   * @param dst Destination vertex ID
   * @param edge_label Edge label/type
   * @param prop Edge property value
   * @return true if edge added successfully, false if vertices don't exist
   *
   * Implementation: Uses graph.get_lid() and added_vertices_ to find vertices,
   * serializes operation to arc_ with op_type=1.
   *
   * @since v0.1.0
   */
  bool AddEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
               label_t edge_label, const std::vector<Property>& properties);

  /**
   * @brief Commit the transaction.
   *
   * Writes the WAL data to persistent storage and releases the timestamp.
   * Returns early if no operations were performed.
   *
   * @return true if commit successful
   *
   * Implementation: Checks if any operations in arc_, writes WAL via logger_,
   * calls vm_.release_insert_timestamp(), then calls clear().
   *
   * @since v0.1.0
   */
  bool Commit();

  void Abort();

  timestamp_t timestamp() const;

  static void IngestWal(PropertyGraph& graph, uint32_t timestamp, char* data,
                        size_t length, Allocator& alloc);

  const Schema& schema() const;

  bool GetVertexIndex(label_t label, const Property& oid, vid_t& lid) const;

  Property GetVertexId(label_t label, vid_t lid) const;

  PropertyGraph& graph() { return graph_; }

 private:
  void create_id_indexer_if_not_exists(label_t label);

  void clear();

  static bool get_vertex_with_retries(PropertyGraph& graph, label_t label,
                                      const Property& oid, vid_t& lid,
                                      timestamp_t timestamp);
  InArchive arc_;

  std::vector<std::unique_ptr<neug::IdIndexerBase<vid_t>>> added_vertices_;
  std::vector<vid_t> added_vertices_base_;
  std::vector<vid_t> vertex_nums_;

  PropertyGraph& graph_;

  Allocator& alloc_;
  IWalWriter& logger_;
  IVersionManager& vm_;
  timestamp_t timestamp_;
};

class StorageTPInsertInterface : public StorageInsertInterface {
 public:
  explicit StorageTPInsertInterface(InsertTransaction& txn) : txn_(txn) {}
  ~StorageTPInsertInterface() {}

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

  inline const Schema& schema() const override { return txn_.schema(); }

  bool GetVertexIndex(label_t label, const Property& id,
                      vid_t& index) const override {
    return txn_.GetVertexIndex(label, id, index);
  }

  Status BatchAddVertices(
      label_t v_label_id,
      std::shared_ptr<IRecordBatchSupplier> supplier) override;

  Status BatchAddEdges(label_t src_label, label_t dst_label, label_t edge_label,
                       std::shared_ptr<IRecordBatchSupplier> supplier) override;

 private:
  InsertTransaction& txn_;
};

}  // namespace neug
