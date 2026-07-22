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

#include <cassert>
#include <optional>

#include "neug/common/types/container_types.h"
#include "neug/common/types/value.h"
#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/property/types.h"

namespace neug {

namespace graph_interface_impl {

using neug::label_t;
using neug::timestamp_t;
using neug::vid_t;

template <typename T>
class VertexArray {
 public:
  VertexArray() : data_() {}
  VertexArray(const VertexSet& keys, const T& val) : data_(keys.size(), val) {}
  ~VertexArray() {}

  inline void Init(const VertexSet& keys, const T& val) {
    data_.resize(keys.size(), val);
  }

  inline typename vector_t<T>::reference operator[](vid_t v) {
    return data_[v];
  }
  inline typename vector_t<T>::const_reference operator[](vid_t v) const {
    return data_[v];
  }

 private:
  vector_t<T> data_;
};

}  // namespace graph_interface_impl

/**
 * @brief Base interface for graph storage operations.
 *
 * IStorageInterface defines the common interface for all storage access
 * patterns. Derived classes implement specific access modes (read, insert,
 * update).
 *
 * @see StorageReadInterface For read-only access
 * @see StorageInsertInterface For insert operations
 * @see StorageUpdateInterface For full read/write access
 *
 * @since v0.1.0
 */
class IStorageInterface {
 public:
  virtual ~IStorageInterface() {}

  /** @brief Check if this interface supports read operations. */
  virtual bool readable() const = 0;

  /** @brief Check if this interface supports write operations. */
  virtual bool writable() const = 0;

  /** @brief Get the graph schema. */
  virtual const Schema& schema() const = 0;

  /**
   * @brief Get internal vertex index from external ID.
   *
   * @param label Vertex label
   * @param id External vertex ID (primary key value)
   * @param index Output parameter for internal vertex index
   * @return true if vertex found, false otherwise
   */
  virtual bool GetVertexIndex(label_t label, const Value& id,
                              vid_t& index) const = 0;
};

/**
 * @brief Read-only interface for graph traversal and property access.
 *
 * StorageReadInterface provides efficient read-only access to graph data
 * for query execution. It supports:
 * - Vertex property access
 * - Edge traversal (outgoing/incoming)
 * - Vertex ID lookups
 *
 * **Usage Example:**
 * @code{.cpp}
 * // Create read interface with timestamp
 * neug::StorageReadInterface reader(graph, timestamp);
 *
 * // Get vertex set for a label
 * auto vertices = reader.GetVertexSet(person_label);
 *
 * // Access vertex properties
 * auto name_col = reader.GetVertexPropColumn<std::string_view>(person_label,
 * "name");
 *
 * // Traverse edges
 * auto out_view = reader.GetGenericOutgoingGraphView(person_label,
 * person_label, knows_label);
 * @endcode
 *
 * **Timestamp Semantics:**
 * The read_ts parameter determines which version of data is visible.
 * Only vertices/edges with timestamp <= read_ts are visible.
 *
 * @note This interface is used internally by query execution.
 * @note Thread-safe for concurrent read access.
 *
 * @see StorageInsertInterface For insert operations
 * @see StorageUpdateInterface For update operations
 *
 * @since v0.1.0
 */
class StorageReadInterface : virtual public IStorageInterface {
 public:
  /// Typed property column accessor
  template <typename PROP_T>
  using vertex_column_t = TypedRefColumn<PROP_T>;

  /// Set of vertex IDs for a label
  using vertex_set_t = neug::VertexSet;

  /// Array indexed by vertex ID
  template <typename T>
  using vertex_array_t = graph_interface_impl::VertexArray<T>;

  /// Invalid vertex ID constant
  static constexpr vid_t kInvalidVid = std::numeric_limits<vid_t>::max();

  /**
   * @brief Construct a read interface from a GraphView reference.
   *
   * @param view GraphView for reading graph data (must outlive this object)
   * @param read_ts Timestamp for MVCC visibility
   */
  StorageReadInterface(const GraphView& view, timestamp_t read_ts)
      : view_(view), read_ts_(read_ts) {}

  ~StorageReadInterface() {}
  bool readable() const override { return true; }
  bool writable() const override { return false; }

  /**
   * @brief Get a typed property column for vertices.
   *
   * Returns a typed column accessor for efficient bulk access to vertex
   * properties. Use this for performance-critical property access.
   *
   * @param label Vertex label
   * @param prop_name Property name
   * @return Shared pointer to column accessor
   *
   * @since v0.1.0
   */
  virtual std::shared_ptr<RefColumnBase> GetVertexPropColumn(
      label_t label, const std::string& prop_name) const {
    return view_.GetVertexPropertyColumn(label, prop_name);
  }

  /**
   * @brief Get all vertices of a specific label.
   *
   * Returns a VertexSet containing all visible vertices of the given label.
   * The set respects MVCC visibility based on read_ts.
   *
   * **Usage Example:**
   * @code{.cpp}
   * VertexSet persons = reader.GetVertexSet(person_label);
   * for (vid_t v : persons) {
   *     // Process each person vertex
   * }
   * @endcode
   *
   * @param label Vertex label
   * @return VertexSet containing all visible vertex IDs
   *
   * @since v0.1.0
   */
  VertexSet GetVertexSet(label_t label) const {
    return view_.GetVertexSet(label, read_ts_);
  }

  bool GetVertexIndex(label_t label, const Value& id,
                      vid_t& index) const override {
    return view_.get_lid(label, id, index, read_ts_);
  }

  /**
   * @brief Check if a vertex is valid (not deleted) at current timestamp.
   *
   * @param label Vertex label
   * @param index Internal vertex ID
   * @return true if vertex exists and is visible
   *
   * @since v0.1.0
   */
  inline bool IsValidVertex(label_t label, vid_t index) const {
    return view_.IsValidLid(label, index, read_ts_);
  }

  /**
   * @brief Get the external ID (primary key) of a vertex.
   *
   * @param label Vertex label
   * @param index Internal vertex ID
   * @return Value containing the primary key value
   *
   * @since v0.1.0
   */
  inline Value GetVertexId(label_t label, vid_t index) const {
    return view_.GetOid(label, index, read_ts_);
  }

  /**
   * @brief Get a single property value for a vertex.
   *
   * **Usage Example:**
   * @code{.cpp}
   * Value age = reader.GetVertexProperty(person_label, vid,
   * age_prop_id); int64_t age_val = age.GetValue<int64_t>();
   * @endcode
   *
   * @param label Vertex label
   * @param index Internal vertex ID
   * @param prop_id Property column index
   * @return Value containing the value
   *
   * @since v0.1.0
   */
  inline Value GetVertexProperty(label_t label, vid_t index,
                                 int prop_id) const {
    auto col = view_.GetVertexPropertyColumn(label, prop_id);
    return col ? col->get_any(index) : Value();
  }

  /**
   * @brief Get outgoing edge view for traversal.
   *
   * **Usage Example:**
   * @code{.cpp}
   * // Get outgoing KNOWS edges from Person to Person
   * CsrView view = reader.GetGenericOutgoingGraphView(
   *     person_label, person_label, knows_label);
   *
   * // Traverse neighbors of vertex v
   * for (auto it = view.get_edges(v).begin(); it != view.get_edges(v).end();
   * ++it) { vid_t neighbor = *it;
   * }
   * @endcode
   *
   * @param v_label Source vertex label
   * @param neighbor_label Destination vertex label
   * @param edge_label Edge label
   * @return CsrView for edge traversal
   *
   * @see CsrView For traversal operations
   *
   * @since v0.1.0
   */
  CsrView GetGenericOutgoingGraphView(label_t v_label, label_t neighbor_label,
                                      label_t edge_label) const {
    return view_.GetGenericOutgoingView(v_label, neighbor_label, edge_label,
                                        read_ts_);
  }

  /**
   * @brief Get incoming edge view for traversal.
   *
   * @param v_label Destination vertex label (receives edges)
   * @param neighbor_label Source vertex label (edges come from)
   * @param edge_label Edge label
   * @return CsrView for reverse edge traversal
   *
   * @since v0.1.0
   */
  CsrView GetGenericIncomingGraphView(label_t v_label, label_t neighbor_label,
                                      label_t edge_label) const {
    return view_.GetGenericIncomingView(neighbor_label, v_label, edge_label,
                                        read_ts_);
  }

  /**
   * @brief Get edge property accessor by column index.
   *
   * @param src_label Source vertex label
   * @param dst_label Destination vertex label
   * @param edge_label Edge label
   * @param prop_id Property column index
   * @return EdgeDataAccessor for property access
   *
   * @since v0.1.0
   */
  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label, int prop_id) const {
    return view_.GetEdgeDataAccessor(src_label, dst_label, edge_label, prop_id);
  }

  /**
   * @brief Get edge property accessor by property name.
   *
   * **Usage Example:**
   * @code{.cpp}
   * // Get accessor for "weight" property
   * EdgeDataAccessor weight = reader.GetEdgeDataAccessor(
   *     person_label, person_label, knows_label, "weight");
   *
   * // Access property during traversal
   * CsrView view = reader.GetGenericOutgoingGraphView(...);
   * for (auto it = view.get_edges(v).begin(); ...; ++it) {
   *     double w = weight.get_typed_data<double>(it);
   * }
   * @endcode
   *
   * @param src_label Source vertex label
   * @param dst_label Destination vertex label
   * @param edge_label Edge label
   * @param prop_name Property name
   * @return EdgeDataAccessor for property access
   *
   * @since v0.1.0
   */
  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label,
                                       const std::string& prop_name) const {
    return view_.GetEdgeDataAccessor(src_label, dst_label, edge_label,
                                     prop_name);
  }

  const Schema& schema() const override { return view_.schema(); }

 protected:
  const GraphView& view_;
  timestamp_t read_ts_;
};

/**
 * @brief Write-only interface for inserting vertices and edges.
 *
 * StorageInsertInterface provides methods for adding new data to the graph.
 * It supports both single-record and batch insert operations.
 *
 * **Usage Example:**
 * @code{.cpp}
 * // Add single vertex
 * vid_t vid;
 * std::vector<neug::Property> props = {Property(30)};  // age
 * inserter.AddVertex(person_label, Property("alice"), props, vid);
 *
 * // Add edge between vertices
 * const void* edge_prop = nullptr;
 * inserter.AddEdge(person_label, src_vid, person_label, dst_vid, knows_label,
 *                  {}, edge_prop);
 * @endcode
 *
 * @note This interface is write-only; use StorageReadInterface for reads.
 * @note For combined read/write, use StorageUpdateInterface.
 *
 * @see StorageReadInterface For read operations
 * @see StorageUpdateInterface For combined read/write
 *
 * @since v0.1.0
 */
class StorageInsertInterface : virtual public IStorageInterface {
 public:
  explicit StorageInsertInterface() {}
  virtual ~StorageInsertInterface() {}

  bool readable() const override { return false; }
  bool writable() const override { return true; }

  /**
   * @brief Add a single vertex to the graph.
   *
   * @param label Vertex label
   * @param id Primary key value
   * @param props Property values (excluding primary key)
   * @param vid Output: assigned internal vertex ID on success
   * @return Status::OK() on success, or an error Status if validation fails
   *         (e.g. property count/type mismatch, capacity failure).
   */
  Status AddVertex(label_t label, const Value& id,
                   const std::vector<Value>& props, vid_t& vid) {
    auto st = AddVertexImpl(label, id, props, vid);
    if (st.ok()) {
      MarkVertexDirty(label);
    }
    return st;
  }

  /**
   * @brief Add a single edge to the graph.
   *
   * @param src_label Source vertex label
   * @param src Source vertex internal ID
   * @param dst_label Destination vertex label
   * @param dst Destination vertex internal ID
   * @param edge_label Edge label
   * @param properties Edge property values
   * @param prop Output: pointer to the inserted edge property storage. For an
   *             insert transaction the edge property is not actually inserted
   *             into the graph until commit, so this is set to nullptr.
   * @return Status::OK() on success, or an error Status if validation fails
   *         (e.g. missing source/destination vertex, property mismatch).
   */
  Status AddEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
                 label_t edge_label, const std::vector<Value>& properties,
                 const void*& prop) {
    auto st = AddEdgeImpl(src_label, src, dst_label, dst, edge_label,
                          properties, prop);
    if (st.ok()) {
      MarkEdgeDirty(src_label, dst_label, edge_label);
    }
    return st;
  }

  /**
   * @brief Batch insert vertices from a record supplier.
   *
   * @param v_label_id Vertex label for all records
   * @param supplier Record batch data source
   * @return Status indicating success or failure
   */
  Status BatchAddVertices(label_t v_label_id,
                          std::shared_ptr<IDataChunkSupplier> supplier) {
    auto st = BatchAddVerticesImpl(v_label_id, std::move(supplier));
    if (st.ok()) {
      MarkVertexDirty(v_label_id);
    }
    return st;
  }

  /**
   * @brief Batch insert edges from a record supplier.
   *
   * @param src_label Source vertex label
   * @param dst_label Destination vertex label
   * @param edge_label Edge label
   * @param supplier Record batch data source
   * @return Status indicating success or failure
   */
  Status BatchAddEdges(label_t src_label, label_t dst_label, label_t edge_label,
                       std::shared_ptr<IDataChunkSupplier> supplier) {
    auto st = BatchAddEdgesImpl(src_label, dst_label, edge_label,
                                std::move(supplier));
    if (st.ok()) {
      MarkEdgeDirty(src_label, dst_label, edge_label);
    }
    return st;
  }

 protected:
  virtual void MarkVertexDirty(label_t label) = 0;
  virtual void MarkEdgeDirty(label_t src, label_t dst, label_t edge) = 0;

  virtual Status AddVertexImpl(label_t label, const Value& id,
                               const std::vector<Value>& props, vid_t& vid) = 0;
  virtual Status AddEdgeImpl(label_t src_label, vid_t src, label_t dst_label,
                             vid_t dst, label_t edge_label,
                             const std::vector<Value>& properties,
                             const void*& prop) = 0;
  virtual Status BatchAddVerticesImpl(
      label_t v_label_id, std::shared_ptr<IDataChunkSupplier> supplier) = 0;
  virtual Status BatchAddEdgesImpl(
      label_t src_label, label_t dst_label, label_t edge_label,
      std::shared_ptr<IDataChunkSupplier> supplier) = 0;
};

/**
 * @brief Full read/write interface for graph mutations.
 *
 * StorageUpdateInterface combines read and write capabilities, enabling:
 * - All read operations from StorageReadInterface
 * - All insert operations from StorageInsertInterface
 * - Property updates on vertices and edges
 * - Deletion of vertices and edges
 * - Schema modifications (DDL operations)
 *
 * **Usage Example:**
 * @code{.cpp}
 * // Update vertex property
 * updater.UpdateVertexProperty(person_label, vid, age_col_id, Property(31));
 *
 * // Delete vertices
 * std::vector<vid_t> to_delete = {vid1, vid2, vid3};
 * updater.BatchDeleteVertices(person_label, to_delete);
 *
 * // Create new vertex type
 * updater.CreateVertexType("Company",
 *     {{DataTypeId::kInt64, "id", Property()},
 *      {DataTypeId::kVarchar, "name", Property()}},
 *     {"id"}, true);
 * @endcode
 *
 * @note This is the most comprehensive storage interface.
 * @note Used by update transactions for Cypher write operations.
 *
 * @see StorageReadInterface For read-only access
 * @see StorageInsertInterface For insert-only access
 *
 * @since v0.1.0
 */
class StorageUpdateInterface : public StorageReadInterface,
                               public StorageInsertInterface {
 public:
  /**
   * @brief Construct an update interface.
   *
   * Reads are inherited from StorageReadInterface, borrowing the caller-owned
   * `view` (which must outlive this object — typically the owning
   * UpdateTransaction's view).
   *
   * @param view GraphView owned by the caller
   * @param ts Timestamp for MVCC visibility
   */
  StorageUpdateInterface(const GraphView& view, timestamp_t ts)
      : StorageReadInterface(view, ts), StorageInsertInterface() {}
  virtual ~StorageUpdateInterface() {}

  bool readable() const override { return true; }
  bool writable() const override { return true; }

  /**
   * @brief Update a vertex property value.
   *
   * @param label Vertex label
   * @param lid Internal vertex ID
   * @param col_id Property column index
   * @param value New property value
   */
  Status UpdateVertexProperty(label_t label, vid_t lid, int col_id,
                              const Value& value) {
    auto st = UpdateVertexPropertyImpl(label, lid, col_id, value);
    if (st.ok()) {
      MarkVertexDirty(label);
    }
    return st;
  }

  /**
   * @brief Update an edge property value.
   *
   * @param src_label Source vertex label
   * @param src Source vertex ID
   * @param dst_label Destination vertex label
   * @param dst Destination vertex ID
   * @param edge_label Edge label
   * @param oe_offset Outgoing edge offset
   * @param ie_offset Incoming edge offset
   * @param col_id Property column index
   * @param value New property value
   */
  Status UpdateEdgeProperty(label_t src_label, vid_t src, label_t dst_label,
                            vid_t dst, label_t edge_label, int32_t oe_offset,
                            int32_t ie_offset, int32_t col_id,
                            const Value& value) {
    auto st = UpdateEdgePropertyImpl(src_label, src, dst_label, dst, edge_label,
                                     oe_offset, ie_offset, col_id, value);
    if (st.ok()) {
      MarkEdgeDirty(src_label, dst_label, edge_label);
    }
    return st;
  }

  /**
   * @brief Delete a single vertex and its associated edges.
   */
  Status DeleteVertex(label_t label, vid_t lid) {
    auto st = DeleteVertexImpl(label, lid);
    if (st.ok()) {
      MarkVertexDirty(label);
      markIncidentEdgeTablesDirty(label);
    }
    return st;
  }

  /**
   * @brief Delete a single edge by offset.
   */
  Status DeleteEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
                    label_t edge_label, int32_t oe_offset, int32_t ie_offset) {
    auto st = DeleteEdgeImpl(src_label, src, dst_label, dst, edge_label,
                             oe_offset, ie_offset);
    if (st.ok()) {
      MarkEdgeDirty(src_label, dst_label, edge_label);
    }
    return st;
  }

  /**
   * @brief Delete all edges between two vertices with a given label.
   */
  Status DeleteEdges(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
                     label_t edge_label) {
    auto st = DeleteEdgesImpl(src_label, src, dst_label, dst, edge_label);
    if (st.ok()) {
      MarkEdgeDirty(src_label, dst_label, edge_label);
    }
    return st;
  }

  /**
   * @brief Delete multiple vertices by their internal IDs.
   *
   * @param v_label_id Vertex label
   * @param vids Vector of internal vertex IDs to delete
   * @return Status indicating success or failure
   */
  Status BatchDeleteVertices(label_t v_label_id,
                             const std::vector<vid_t>& vids) {
    auto st = BatchDeleteVerticesImpl(v_label_id, vids);
    if (st.ok()) {
      MarkVertexDirty(v_label_id);
      markIncidentEdgeTablesDirty(v_label_id);
    }
    return st;
  }

  /**
   * @brief Delete multiple edges by source-destination pairs.
   *
   * @param src_v_label_id Source vertex label
   * @param dst_v_label_id Destination vertex label
   * @param edge_label_id Edge label
   * @param edges Vector of (source_vid, destination_vid) pairs
   * @return Status indicating success or failure
   */
  Status BatchDeleteEdges(label_t src_v_label_id, label_t dst_v_label_id,
                          label_t edge_label_id,
                          const std::vector<std::tuple<vid_t, vid_t>>& edges) {
    auto st = BatchDeleteEdgesImpl(src_v_label_id, dst_v_label_id,
                                   edge_label_id, edges);
    if (st.ok()) {
      MarkEdgeDirty(src_v_label_id, dst_v_label_id, edge_label_id);
    }
    return st;
  }

  /// @brief Delete edges by offset (for internal use)
  Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
      const std::vector<std::pair<vid_t, int32_t>>& ie_edges) {
    auto st = BatchDeleteEdgesImpl(src_v_label_id, dst_v_label_id,
                                   edge_label_id, oe_edges, ie_edges);
    if (st.ok()) {
      MarkEdgeDirty(src_v_label_id, dst_v_label_id, edge_label_id);
    }
    return st;
  }

  /**
   * @brief Create a new vertex type.
   *
   * @param config CreateVertexTypeParam (includes type name and properties)
   */
  Status CreateVertexType(const CreateVertexTypeParam& config) {
    auto st = CreateVertexTypeImpl(config);
    if (st.ok()) {
      MarkSchemaDirty();
    }
    return st;
  }

  /**
   * @brief Create a new edge type.
   *
   * @param config CreateEdgeTypeParam (includes src/dst/edge names and
   *               properties)
   */
  Status CreateEdgeType(const CreateEdgeTypeParam& config) {
    auto st = CreateEdgeTypeImpl(config);
    if (st.ok()) {
      MarkSchemaDirty();
    }
    return st;
  }

  /**
   * @brief Add properties to an existing vertex type.
   *
   * Callers (DDL operators) resolve the type name to @p label before entry.
   * @p config carries the property payload only.
   *
   * @param label Vertex label id
   * @param config Property payload (names and default values)
   */
  Status AddVertexProperties(label_t label,
                             const AddVertexPropertiesParam& config) {
    auto st = AddVertexPropertiesImpl(label, config);
    if (st.ok()) {
      MarkSchemaDirty();
      MarkVertexDirty(label);
    }
    return st;
  }

  /**
   * @brief Add properties to an existing edge type.
   *
   * @param src Source vertex label id
   * @param dst Destination vertex label id
   * @param edge Edge label id
   * @param config Property payload (names and default values)
   */
  Status AddEdgeProperties(label_t src, label_t dst, label_t edge,
                           const AddEdgePropertiesParam& config) {
    auto st = AddEdgePropertiesImpl(src, dst, edge, config);
    if (st.ok()) {
      MarkSchemaDirty();
      MarkEdgeDirty(src, dst, edge);
    }
    return st;
  }

  /**
   * @brief Rename properties of a vertex type.
   *
   * @param label Vertex label id
   * @param config Rename mappings (old name → new name)
   */
  Status RenameVertexProperties(label_t label,
                                const RenameVertexPropertiesParam& config) {
    auto st = RenameVertexPropertiesImpl(label, config);
    if (st.ok()) {
      MarkSchemaDirty();
      MarkVertexDirty(label);
    }
    return st;
  }

  /**
   * @brief Rename properties of an edge type.
   *
   * @param src Source vertex label id
   * @param dst Destination vertex label id
   * @param edge Edge label id
   * @param config Rename mappings (old name → new name)
   */
  Status RenameEdgeProperties(label_t src, label_t dst, label_t edge,
                              const RenameEdgePropertiesParam& config) {
    auto st = RenameEdgePropertiesImpl(src, dst, edge, config);
    if (st.ok()) {
      MarkSchemaDirty();
      MarkEdgeDirty(src, dst, edge);
    }
    return st;
  }

  /**
   * @brief Delete properties from a vertex type.
   *
   * @param label Vertex label id
   * @param config Property names to delete
   */
  Status DeleteVertexProperties(label_t label,
                                const DeleteVertexPropertiesParam& config) {
    auto st = DeleteVertexPropertiesImpl(label, config);
    if (st.ok()) {
      MarkSchemaDirty();
      MarkVertexDirty(label);
    }
    return st;
  }

  /**
   * @brief Delete properties from an edge type.
   *
   * @param src Source vertex label id
   * @param dst Destination vertex label id
   * @param edge Edge label id
   * @param config Property names to delete
   */
  Status DeleteEdgeProperties(label_t src, label_t dst, label_t edge,
                              const DeleteEdgePropertiesParam& config) {
    auto st = DeleteEdgePropertiesImpl(src, dst, edge, config);
    if (st.ok()) {
      MarkSchemaDirty();
      MarkEdgeDirty(src, dst, edge);
    }
    return st;
  }

  /**
   * @brief Delete a vertex type.
   *
   * Callers resolve the type name to @p label before entry.
   *
   * @param label Vertex label id
   */
  Status DeleteVertexType(label_t label) {
    auto st = DeleteVertexTypeImpl(label);
    if (st.ok()) {
      MarkSchemaDirty();
    }
    return st;
  }

  /**
   * @brief Delete an edge type.
   *
   * Callers resolve type names to label ids before entry.
   *
   * @param src Source vertex label id
   * @param dst Destination vertex label id
   * @param edge Edge label id
   */
  Status DeleteEdgeType(label_t src, label_t dst, label_t edge) {
    auto st = DeleteEdgeTypeImpl(src, dst, edge);
    if (st.ok()) {
      MarkSchemaDirty();
    }
    return st;
  }

  /**
   * @brief Create a checkpoint of the current graph state.
   */
  virtual void CreateCheckpoint() = 0;

 protected:
  virtual void MarkSchemaDirty() = 0;

  virtual Status UpdateVertexPropertyImpl(label_t label, vid_t lid, int col_id,
                                          const Value& value) = 0;
  virtual Status UpdateEdgePropertyImpl(label_t src_label, vid_t src,
                                        label_t dst_label, vid_t dst,
                                        label_t edge_label, int32_t oe_offset,
                                        int32_t ie_offset, int32_t col_id,
                                        const Value& value) = 0;
  virtual Status DeleteVertexImpl(label_t label, vid_t lid) = 0;
  virtual Status DeleteEdgeImpl(label_t src_label, vid_t src, label_t dst_label,
                                vid_t dst, label_t edge_label,
                                int32_t oe_offset, int32_t ie_offset) = 0;
  virtual Status DeleteEdgesImpl(label_t src_label, vid_t src,
                                 label_t dst_label, vid_t dst,
                                 label_t edge_label) = 0;
  virtual Status BatchDeleteVerticesImpl(label_t v_label_id,
                                         const std::vector<vid_t>& vids) = 0;
  virtual Status BatchDeleteEdgesImpl(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::tuple<vid_t, vid_t>>& edges) = 0;
  virtual Status BatchDeleteEdgesImpl(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
      const std::vector<std::pair<vid_t, int32_t>>& ie_edges) = 0;
  virtual Status CreateVertexTypeImpl(const CreateVertexTypeParam& config) = 0;
  virtual Status CreateEdgeTypeImpl(const CreateEdgeTypeParam& config) = 0;
  virtual Status AddVertexPropertiesImpl(
      label_t label, const AddVertexPropertiesParam& config) = 0;
  virtual Status AddEdgePropertiesImpl(
      label_t src, label_t dst, label_t edge,
      const AddEdgePropertiesParam& config) = 0;
  virtual Status RenameVertexPropertiesImpl(
      label_t label, const RenameVertexPropertiesParam& config) = 0;
  virtual Status RenameEdgePropertiesImpl(
      label_t src, label_t dst, label_t edge,
      const RenameEdgePropertiesParam& config) = 0;
  virtual Status DeleteVertexPropertiesImpl(
      label_t label, const DeleteVertexPropertiesParam& config) = 0;
  virtual Status DeleteEdgePropertiesImpl(
      label_t src, label_t dst, label_t edge,
      const DeleteEdgePropertiesParam& config) = 0;
  virtual Status DeleteVertexTypeImpl(label_t label) = 0;
  virtual Status DeleteEdgeTypeImpl(label_t src, label_t dst, label_t edge) = 0;

 private:
  void markIncidentEdgeTablesDirty(label_t label) {
    for (const auto& [_, es] : schema().get_all_edge_schemas()) {
      if (es->src_label_id == label || es->dst_label_id == label) {
        MarkEdgeDirty(es->src_label_id, es->dst_label_id, es->edge_label_id);
      }
    }
  }
};

class StorageAPUpdateInterface : public StorageUpdateInterface {
 public:
  explicit StorageAPUpdateInterface(PropertyGraph& graph, GraphView& view,
                                    timestamp_t timestamp,
                                    neug::Allocator& alloc)
      : StorageUpdateInterface(view, timestamp),
        graph_(graph),
        mut_view_(view),
        alloc_(alloc),
        timestamp_(timestamp) {}
  ~StorageAPUpdateInterface() {}

  void CreateCheckpoint() override;

 protected:
  void MarkVertexDirty(label_t label) override {
    graph_.MarkVertexDirty(label);
  }
  void MarkEdgeDirty(label_t src, label_t dst, label_t edge) override {
    graph_.MarkEdgeDirty(src, dst, edge);
  }
  void MarkSchemaDirty() override { graph_.MarkSchemaDirty(); }

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
  Status DeleteEdgeImpl(label_t src_label, vid_t src, label_t dst_label,
                        vid_t dst, label_t edge_label, int32_t oe_offset,
                        int32_t ie_offset) override;
  Status DeleteEdgesImpl(label_t src_label, vid_t src, label_t dst_label,
                         vid_t dst, label_t edge_label) override;
  Status BatchAddVerticesImpl(
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

 private:
  PropertyGraph& graph_;
  GraphView& mut_view_;
  neug::Allocator& alloc_;
  timestamp_t timestamp_;
};

}  // namespace neug
