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

  inline typename std::vector<T>::reference operator[](vid_t v) {
    return data_[v];
  }
  inline typename std::vector<T>::const_reference operator[](vid_t v) const {
    return data_[v];
  }

 private:
  std::vector<T> data_;
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
  virtual bool GetVertexIndex(label_t label, const Property& id,
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
   * @brief Construct a read interface for a specific timestamp.
   *
   * @param graph Reference to the PropertyGraph
   * @param read_ts Timestamp for MVCC visibility
   */
  explicit StorageReadInterface(const PropertyGraph& graph, timestamp_t read_ts)
      : graph_(graph), read_ts_(read_ts) {}
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

  std::shared_ptr<RefColumnBase> GetVertexPropColumn(
      label_t label, const std::string& prop_name) const {
    return graph_.GetVertexPropertyColumn(label, prop_name);
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
    return graph_.GetVertexSet(label, read_ts_);
  }

  bool GetVertexIndex(label_t label, const Property& id,
                      vid_t& index) const override {
    return graph_.get_lid(label, id, index, read_ts_);
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
    return graph_.IsValidLid(label, index, read_ts_);
  }

  /**
   * @brief Get the external ID (primary key) of a vertex.
   *
   * @param label Vertex label
   * @param index Internal vertex ID
   * @return Property containing the primary key value
   *
   * @since v0.1.0
   */
  inline Property GetVertexId(label_t label, vid_t index) const {
    return graph_.GetOid(label, index, read_ts_);
  }

  /**
   * @brief Get a single property value for a vertex.
   *
   * **Usage Example:**
   * @code{.cpp}
   * Property age = reader.GetVertexProperty(person_label, vid, age_prop_id);
   * int64_t age_val = age.as_int64();
   * @endcode
   *
   * @param label Vertex label
   * @param index Internal vertex ID
   * @param prop_id Property column index
   * @return Property containing the value
   *
   * @since v0.1.0
   */
  inline Property GetVertexProperty(label_t label, vid_t index,
                                    int prop_id) const {
    return graph_.get_vertex_table(label)
        .get_property_column(prop_id)
        ->get_prop(index);
  }

  /**
   * @brief Get outgoing edge view for traversal.
   *
   * **Usage Example:**
   * @code{.cpp}
   * // Get outgoing KNOWS edges from Person to Person
   * GenericView view = reader.GetGenericOutgoingGraphView(
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
   * @return GenericView for edge traversal
   *
   * @see GenericView For traversal operations
   *
   * @since v0.1.0
   */
  GenericView GetGenericOutgoingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    return graph_.GetGenericOutgoingGraphView(v_label, neighbor_label,
                                              edge_label, read_ts_);
  }

  /**
   * @brief Get incoming edge view for traversal.
   *
   * @param v_label Destination vertex label (receives edges)
   * @param neighbor_label Source vertex label (edges come from)
   * @param edge_label Edge label
   * @return GenericView for reverse edge traversal
   *
   * @since v0.1.0
   */
  GenericView GetGenericIncomingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    return graph_.GetGenericIncomingGraphView(v_label, neighbor_label,
                                              edge_label, read_ts_);
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
    return graph_.GetEdgeDataAccessor(src_label, dst_label, edge_label,
                                      prop_id);
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
   * GenericView view = reader.GetGenericOutgoingGraphView(...);
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
    return graph_.GetEdgeDataAccessor(src_label, dst_label, edge_label,
                                      prop_name);
  }

  const Schema& schema() const override { return graph_.schema(); }

 private:
  const PropertyGraph& graph_;
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
 * inserter.AddEdge(person_label, src_vid, person_label, dst_vid, knows_label,
 * {});
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
   * @param vid Output: assigned internal vertex ID
   * @return true if vertex added successfully
   */
  virtual bool AddVertex(label_t label, const Property& id,
                         const std::vector<Property>& props, vid_t& vid) = 0;

  /**
   * @brief Add a single edge to the graph.
   *
   * @param src_label Source vertex label
   * @param src Source vertex internal ID
   * @param dst_label Destination vertex label
   * @param dst Destination vertex internal ID
   * @param edge_label Edge label
   * @param properties Edge property values
   * @return true if edge added successfully
   */
  virtual bool AddEdge(label_t src_label, vid_t src, label_t dst_label,
                       vid_t dst, label_t edge_label,
                       const std::vector<Property>& properties) = 0;

  /**
   * @brief Batch insert vertices from a record supplier.
   *
   * @param v_label_id Vertex label for all records
   * @param supplier Record batch data source
   * @return Status indicating success or failure
   */
  virtual Status BatchAddVertices(
      label_t v_label_id, std::shared_ptr<IRecordBatchSupplier> supplier) = 0;

  /**
   * @brief Batch insert edges from a record supplier.
   *
   * @param src_label Source vertex label
   * @param dst_label Destination vertex label
   * @param edge_label Edge label
   * @param supplier Record batch data source
   * @return Status indicating success or failure
   */
  virtual Status BatchAddEdges(
      label_t src_label, label_t dst_label, label_t edge_label,
      std::shared_ptr<IRecordBatchSupplier> supplier) = 0;
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
   * @brief Construct an update interface with read timestamp.
   *
   * @param graph Reference to the PropertyGraph
   * @param read_ts Timestamp for MVCC visibility
   */
  explicit StorageUpdateInterface(const neug::PropertyGraph& graph,
                                  timestamp_t read_ts)
      : StorageReadInterface(graph, read_ts), StorageInsertInterface() {}
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
  virtual void UpdateVertexProperty(label_t label, vid_t lid, int col_id,
                                    const Property& value) = 0;

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
  virtual void UpdateEdgeProperty(label_t src_label, vid_t src,
                                  label_t dst_label, vid_t dst,
                                  label_t edge_label, int32_t oe_offset,
                                  int32_t ie_offset, int32_t col_id,
                                  const Property& value) = 0;

  virtual bool AddVertex(label_t label, const Property& id,
                         const std::vector<Property>& props,
                         vid_t& vid) override = 0;
  virtual bool AddEdge(label_t src_label, vid_t src, label_t dst_label,
                       vid_t dst, label_t edge_label,
                       const std::vector<Property>& properties) override = 0;

  /**
   * @brief Delete multiple vertices by their internal IDs.
   *
   * @param v_label_id Vertex label
   * @param vids Vector of internal vertex IDs to delete
   * @return Status indicating success or failure
   */
  virtual Status BatchDeleteVertices(label_t v_label_id,
                                     const std::vector<vid_t>& vids) = 0;

  /**
   * @brief Delete multiple edges by source-destination pairs.
   *
   * @param src_v_label_id Source vertex label
   * @param dst_v_label_id Destination vertex label
   * @param edge_label_id Edge label
   * @param edges Vector of (source_vid, destination_vid) pairs
   * @return Status indicating success or failure
   */
  virtual Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::tuple<vid_t, vid_t>>& edges) = 0;

  /// @brief Delete edges by offset (for internal use)
  virtual Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
      const std::vector<std::pair<vid_t, int32_t>>& ie_edges) = 0;

  virtual Status CreateVertexType(
      const std::string& name,
      const std::vector<std::tuple<DataType, std::string, Property>>&
          properties,
      const std::vector<std::string>& primary_key_names,
      bool error_on_conflict) = 0;

  virtual Status CreateEdgeType(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::tuple<DataType, std::string, Property>>&
          properties,
      bool error_on_conflict, EdgeStrategy oe_edge_strategy,
      EdgeStrategy ie_edge_strategy) = 0;

  virtual Status AddVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::tuple<DataType, std::string, Property>>&
          add_properties,
      bool error_on_conflict) = 0;

  virtual Status AddEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::tuple<DataType, std::string, Property>>&
          add_properties,
      bool error_on_conflict) = 0;

  virtual Status RenameVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict) = 0;

  virtual Status RenameEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict) = 0;

  virtual Status DeleteVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::string>& delete_properties,
      bool error_on_conflict) = 0;

  virtual Status DeleteEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::string>& delete_properties,
      bool error_on_conflict) = 0;

  virtual Status DeleteVertexType(const std::string& vertex_type_name,
                                  bool error_on_conflict = true) = 0;

  virtual Status DeleteEdgeType(const std::string& src_type,
                                const std::string& dst_type,
                                const std::string& edge_type,
                                bool error_on_conflict) = 0;

  virtual void CreateCheckpoint() = 0;
};

class StorageAPUpdateInterface : public StorageUpdateInterface {
 public:
  explicit StorageAPUpdateInterface(PropertyGraph& graph, timestamp_t timestamp,
                                    neug::Allocator& alloc)
      : StorageUpdateInterface(graph, timestamp),
        graph_(graph),
        alloc_(alloc),
        timestamp_(timestamp) {}
  ~StorageAPUpdateInterface() {}

  void UpdateVertexProperty(label_t label, vid_t lid, int col_id,
                            const Property& value) override;
  void UpdateEdgeProperty(label_t src_label, vid_t src, label_t dst_label,
                          vid_t dst, label_t edge_label, int32_t oe_offset,
                          int32_t ie_offset, int32_t col_id,
                          const Property& value) override;
  bool AddVertex(label_t label, const Property& id,
                 const std::vector<Property>& props, vid_t& vid) override;
  bool AddEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
               label_t edge_label,
               const std::vector<Property>& properties) override;
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
  PropertyGraph& graph_;
  neug::Allocator& alloc_;
  timestamp_t timestamp_;
};

}  // namespace neug
