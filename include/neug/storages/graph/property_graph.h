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

#include <glog/logging.h>
#include <stddef.h>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "neug/storages/allocators.h"
#include "neug/storages/csr/generic_view.h"
#include "neug/storages/graph/edge_table.h"
#include "neug/storages/graph/operation_params.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/graph/vertex_table.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace neug {

namespace execution {
class EdgeRecord;
}

/**
 * @brief Core property graph storage engine for vertices, edges, and schema.
 *
 * PropertyGraph is the **fundamental storage layer** for all graph data in
 * NeuG. It provides low-level access to graph structures, schema management,
 * and persistence capabilities. Most users interact with graphs through
 * higher-level APIs (NeugDB, Connection), but PropertyGraph offers direct
 * access for performance-critical applications.
 *
 * **Usage Example:**
 * @code{.cpp}
 * // Access via NeugDB
 * neug::NeugDB db;
 * db.Open("/path/to/graph");
 * const neug::PropertyGraph& graph = db.graph();
 *
 * // Get schema information
 * const neug::Schema& schema = graph.schema();
 * label_t person_label = schema.get_vertex_label_id("Person");
 *
 * // Get vertex count
 * vid_t vertex_count = graph.VertexNum(person_label);
 *
 * // Access vertex properties
 * auto name_column = graph.GetVertexPropertyColumn(person_label, "name");
 * @endcode
 *
 * **Storage Architecture:**
 * - **VertexTable**: Per-label vertex storage with properties and primary keys
 * - **EdgeTable**: CSR (Compressed Sparse Row) format for efficient traversal
 * - **Schema**: Type definitions, property schemas, and constraints
 *
 * **Memory Levels:**
 * - Level 0: Sync with disk (lowest memory, highest I/O)
 * - Level 1: Memory-mapped virtual memory (default)
 * - Level 2: Prefer hugepages for better TLB performance
 * - Level 3: Force hugepages (highest performance, most memory)
 *
 * **Persistence:**
 * - Snapshot-based persistence to work_dir
 * - Compaction support for removing deleted data
 * - Schema stored in `graph.yaml`
 *
 * @note For query execution, use Connection::Query() instead of direct
 * PropertyGraph access.
 * @note PropertyGraph is not thread-safe for writes. Use transactions for
 * concurrent access.
 *
 * @see Schema For schema management
 * @see VertexTable For vertex storage details
 * @see EdgeTable For edge storage details
 *
 * @since v0.1.0
 */
class PropertyGraph {
 public:
  /**
   * @brief Construct PropertyGraph with default settings.
   *
   * Implementation: Initializes vertex_label_total_count_=0,
   * edge_label_total_count_=0, memory_level_=kInMemory
   *
   * @since v0.1.0
   */
  PropertyGraph();

  /**
   * @brief Destructor that reserves space and cleans up resources.
   * Implementation: Calls Clear() to release resources and reset state.
   * @since v0.1.0
   */
  ~PropertyGraph();

  /**
   * @brief Open the property graph from persistent storage.
   *
   * @param work_dir Working directory containing graph data files
   * @param memory_level Memory usage level (controls performance vs memory
   * tradeoff)
   *
   * Implementation: Sets work_dir_ and memory_level_, loads schema from
   * work_dir, then loads vertex and edge data from snapshot files.
   *
   * @since v0.1.0
   */
  void Open(const std::string& work_dir, MemoryLevel memory_level);

  void Open(const Schema& schema, const std::string& work_dir,
            MemoryLevel memory_level);

  void Compact(bool compact_csr, float reserve_ratio, timestamp_t ts);

  /**
   * @brief Dump the current graph state to persistent storage.
   * @param reopen If true, reopens the graph after dumping (default: true)
   */
  void Dump(bool reopen = true);

  /**
   * @brief Dump schema information to a file.
   *
   * @since v0.1.0
   */
  void DumpSchema();

  /**
   * @brief Get read-only access to the schema.
   *
   * @return const Schema& Reference to the graph schema
   *
   * @since v0.1.0
   */
  const Schema& schema() const;

  /**
   * @brief Get mutable access to the schema.
   *
   * @return Schema& Mutable reference to the graph schema
   *
   * @since v0.1.0
   */
  Schema& mutable_schema();

  /**
   * @brief Clear all graph data and reset to empty state.
   *
   * Implementation: Clears vertex_tables_, edge_tables_, resets label counts
   * to 0, and calls schema_.Clear().
   *
   * @since v0.1.0
   */
  void Clear();

  /**
   * @brief Create a new vertex type in the graph schema.
   *
   * Defines a new vertex label with its properties and primary key.
   *
   * **Usage Example:**
   * @code{.cpp}
   * CreateVertexTypeParamBuilder builder;
   * auto config = builder.VertexLabel("Person")
   *                   .AddProperty(DataType::kInt64, "id", Property())
   *                   .AddProperty(DataType::kVarchar, "name", Property())
   *                   .AddProperty(DataType::kInt32, "age", Property())
   *                   .AddPrimaryKeyName("id")
   *                   .Build();
   * graph.CreateVertexType(config, true);
   * @endcode
   *
   * @param config Vertex type creation config, including type name,
   *        properties, and primary keys
   * @param error_on_conflict If true, returns error if type exists;
   *        if false, silently skips creation
   *
   * @return Status indicating success or failure
   *
   * @since v0.1.0
   */
  Status CreateVertexType(const CreateVertexTypeParam& config,
                          bool error_on_conflict = true);

  /**
   * @brief Create a new edge type in the graph schema.
   *
   * Defines a new edge label connecting source and destination vertex types.
   *
   * **Usage Example:**
   * @code{.cpp}
   * CreateEdgeTypeParamBuilder builder;
   * auto config = builder.SrcLabel("Person")
   *                   .DstLabel("Person")
   *                   .EdgeLabel("KNOWS")
   *                   .AddProperty(DataType::kInt64, "since", Property())
   *                   .AddProperty(DataType::kDouble, "weight", Property())
   *                   .OEEdgeStrategy(EdgeStrategy::kMultiple)
   *                   .IEEdgeStrategy(EdgeStrategy::kMultiple)
   *                   .Build();
   * graph.CreateEdgeType(config, true);
   * @endcode
   *
   * @param config Edge type creation config, including source/destination,
   *        edge label, properties, and edge strategies
   * @param error_on_conflict If true, returns error if type exists
   *
   * @return Status indicating success or failure
   *
   * @since v0.1.0
   */
  Status CreateEdgeType(const CreateEdgeTypeParam& config,
                        bool error_on_conflict = true);

  /**
   * @brief Delete a vertex type physically from the graph storage, could not be
   * reverted.
   * @param vertex_type_name Name of the vertex type to delete
   * @return Status Status indicating success or failure
   */
  Status DeleteVertexType(const std::string& vertex_type_name,
                          bool error_on_conflict = true);

  Status DeleteVertexType(label_t label, bool error_on_conflict = true);

  Status DeleteEdgeType(const std::string& src_vertex_type,
                        const std::string& dst_vertex_type,
                        const std::string& edge_type_name,
                        bool error_on_conflict = true);

  Status DeleteEdgeType(label_t src_label, label_t dst_label,
                        label_t edge_label, bool error_on_conflict = true);

  Status AddVertexProperties(const AddVertexPropertiesParam& config,
                             bool error_on_conflict = true);

  Status AddEdgeProperties(const AddEdgePropertiesParam& config,
                           bool error_on_conflict = true);

  Status RenameVertexProperties(const RenameVertexPropertiesParam& config,
                                bool error_on_conflict = true);

  Status RenameEdgeProperties(const RenameEdgePropertiesParam& config,
                              bool error_on_conflict = true);

  Status DeleteVertexProperties(const DeleteVertexPropertiesParam& config,
                                bool error_on_conflict = true);

  Status DeleteEdgeProperties(const DeleteEdgePropertiesParam& config,
                              bool error_on_conflict = true);

  Status EnsureCapacity(label_t v_label, size_t capacity);

  Status EnsureCapacity(label_t src_label, label_t dst_label,
                        label_t edge_label, size_t capacity);

  Status EnsureCapacity(label_t src_label, label_t dst_label,
                        label_t edge_label, size_t src_v_cap, size_t dst_v_cap,
                        size_t capacity);

  Status BatchAddVertices(label_t v_label_id,
                          std::shared_ptr<IRecordBatchSupplier> supplier);

  Status BatchAddEdges(label_t src_label, label_t dst_label, label_t edge_label,
                       std::shared_ptr<IRecordBatchSupplier> supplier);

  Status BatchDeleteVertices(label_t v_label_id,
                             const std::vector<vid_t>& vids);

  /**
   * @brief Delete vertex and its associated edges.
   * @param label Vertex label id.
   * @param oid Vertex original id.
   * @param ts Timestamp of the deletion.
   * @return true if deletion is successful, false otherwise.
   * @note We always delete vertex in detach mode.
   */
  Status DeleteVertex(label_t v_label, const Property& oid, timestamp_t ts);

  Status DeleteVertex(label_t v_label, vid_t lid, timestamp_t ts);

  Status DeleteEdge(label_t src_label, vid_t src_lid, label_t dst_label,
                    vid_t dst_lid, label_t edge_label, int32_t oe_offset,
                    int32_t ie_offset, timestamp_t ts);

  Status BatchDeleteEdges(
      label_t src_v_label, label_t dst_v_label, label_t edge_label,
      const std::vector<std::tuple<vid_t, vid_t>>& edges_vec);

  Status BatchDeleteEdges(
      label_t src_v_label, label_t dst_v_label, label_t edge_label,
      const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
      const std::vector<std::pair<vid_t, int32_t>>& ie_edges);

  inline VertexTable& get_vertex_table(label_t vertex_label) {
    return vertex_tables_[vertex_label];
  }

  inline const VertexTable& get_vertex_table(label_t vertex_label) const {
    return vertex_tables_[vertex_label];
  }

  inline EdgeTable& get_edge_table(label_t src_label, label_t dst_label,
                                   label_t edge_label) {
    size_t index =
        schema_.generate_edge_label(src_label, dst_label, edge_label);
    if (edge_tables_.count(index) == 0) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Edge table for edge label triplet not found");
    }
    return edge_tables_.at(index);
  }

  inline const EdgeTable& get_edge_table(label_t src_label, label_t dst_label,
                                         label_t edge_label) const {
    size_t index =
        schema_.generate_edge_label(src_label, dst_label, edge_label);
    if (edge_tables_.count(index) == 0) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Edge table for edge label triplet not found");
    }
    return edge_tables_.at(index);
  }

  vid_t LidNum(label_t vertex_label) const;

  vid_t VertexNum(label_t vertex_label, timestamp_t ts = MAX_TIMESTAMP) const;

  bool IsValidLid(label_t vertex_label, vid_t lid, timestamp_t ts) const;

  size_t EdgeNum(label_t src_label, label_t edge_label,
                 label_t dst_label) const;

  bool get_lid(label_t label, const Property& oid, vid_t& lid,
               timestamp_t ts) const;

  Property GetOid(label_t label, vid_t lid, timestamp_t ts) const;

  Status AddVertex(label_t label, const Property& id,
                   const std::vector<Property>& props, vid_t& vid,
                   timestamp_t ts, bool insert_safe = false);

  int32_t AddEdge(label_t src_label, vid_t src_lid, label_t dst_label,
                  vid_t dst_lid, label_t edge_label,
                  const std::vector<Property>& properties, timestamp_t ts,
                  Allocator& alloc, bool insert_safe = false);

  Status UpdateVertexProperty(label_t v_label, vid_t vid, int32_t prop_id,
                              const Property& value, timestamp_t ts);

  Status UpdateEdgeProperty(label_t src_label, vid_t src_lid, label_t dst_label,
                            vid_t dst_lid, label_t e_label, int32_t oe_offset,
                            int32_t ie_offset, int32_t col_id,
                            const Property& new_prop, timestamp_t ts);

  /**
   * @brief Get a view for traversing outgoing edges.
   *
   * Returns a GenericView for efficiently iterating over outgoing edges
   * from vertices of type v_label to vertices of type neighbor_label.
   *
   * **Usage Example:**
   * @code{.cpp}
   * // Get view for Person -[KNOWS]-> Person edges
   * label_t person = schema.get_vertex_label_id("Person");
   * label_t knows = schema.get_edge_label_id("KNOWS");
   *
   * GenericView view = graph.GetGenericOutgoingGraphView(
   *     person, person, knows, read_ts);
   *
   * // Traverse from vertex v
   * NbrList neighbors = view.get_edges(v);
   * for (auto it = neighbors.begin(); it != neighbors.end(); ++it) {
   *     vid_t friend_id = *it;
   *     // Process neighbor...
   * }
   * @endcode
   *
   * @param v_label Source vertex label
   * @param neighbor_label Destination vertex label
   * @param edge_label Edge label connecting them
   * @param ts Read timestamp for MVCC (default: latest)
   *
   * @return GenericView for outgoing edge traversal
   *
   * @throws std::invalid_argument if edge triplet doesn't exist
   *
   * @see GenericView For traversal operations
   * @see GetGenericIncomingGraphView For reverse traversal
   *
   * @since v0.1.0
   */
  GenericView GetGenericOutgoingGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label,
      timestamp_t ts = std::numeric_limits<timestamp_t>::max()) const {
    size_t index =
        schema_.generate_edge_label(v_label, neighbor_label, edge_label);
    if (edge_tables_.count(index) == 0) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Edge table for edge label triplet not found");
    }
    return edge_tables_.at(index).get_outgoing_view(ts);
  }

  /**
   * @brief Get a view for traversing incoming edges.
   *
   * Returns a GenericView for efficiently iterating over incoming edges
   * to vertices of type v_label from vertices of type neighbor_label.
   *
   * **Usage Example:**
   * @code{.cpp}
   * // Get view for Person <-[KNOWS]- Person edges (reverse direction)
   * GenericView view = graph.GetGenericIncomingGraphView(
   *     person, person, knows, read_ts);
   *
   * // Find who follows vertex v (incoming edges)
   * NbrList followers = view.get_edges(v);
   * for (auto it = followers.begin(); it != followers.end(); ++it) {
   *     vid_t follower_id = *it;
   * }
   * @endcode
   *
   * @param v_label Destination vertex label (this vertex type receives edges)
   * @param neighbor_label Source vertex label (edges come from this type)
   * @param edge_label Edge label connecting them
   * @param ts Read timestamp for MVCC (default: latest)
   *
   * @return GenericView for incoming edge traversal
   *
   * @throws std::invalid_argument if edge triplet doesn't exist
   *
   * @see GenericView For traversal operations
   * @see GetGenericOutgoingGraphView For forward traversal
   *
   * @since v0.1.0
   */
  GenericView GetGenericIncomingGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label,
      timestamp_t ts = std::numeric_limits<timestamp_t>::max()) const {
    size_t index =
        schema_.generate_edge_label(neighbor_label, v_label, edge_label);
    if (edge_tables_.count(index) == 0) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Edge table for edge label triplet not found");
    }
    return edge_tables_.at(index).get_incoming_view(ts);
  }

  /**
   * @brief Get accessor for edge property by column index.
   *
   * Returns an EdgeDataAccessor for reading edge property values.
   *
   * @param src_label Source vertex label
   * @param dst_label Destination vertex label
   * @param edge_label Edge label
   * @param prop_id Property column index (0-based)
   *
   * @return EdgeDataAccessor for the specified property
   *
   * @throws std::invalid_argument if edge triplet doesn't exist
   *
   * @see EdgeDataAccessor For accessing property values
   *
   * @since v0.1.0
   */
  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label, int prop_id) const {
    size_t index =
        schema_.generate_edge_label(src_label, dst_label, edge_label);
    if (edge_tables_.count(index) == 0) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Edge table for edge label triplet not found");
    }
    return edge_tables_.at(index).get_edge_data_accessor(prop_id);
  }

  /**
   * @brief Get accessor for edge property by name.
   *
   * **Usage Example:**
   * @code{.cpp}
   * // Get accessor for "weight" property on KNOWS edges
   * EdgeDataAccessor weight_accessor = graph.GetEdgeDataAccessor(
   *     person, person, knows, "weight");
   *
   * // Use with edge iteration
   * GenericView view = graph.GetGenericOutgoingGraphView(...);
   * for (auto it = view.get_edges(v).begin(); ...; ++it) {
   *     double weight = weight_accessor.get_typed_data<double>(it);
   * }
   * @endcode
   *
   * @param src_label Source vertex label
   * @param dst_label Destination vertex label
   * @param edge_label Edge label
   * @param prop Property name
   *
   * @return EdgeDataAccessor for the specified property
   *
   * @throws std::invalid_argument if edge triplet or property doesn't exist
   *
   * @since v0.1.0
   */
  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label,
                                       const std::string& prop) const {
    size_t index =
        schema_.generate_edge_label(src_label, dst_label, edge_label);
    auto edge_table_it = edge_tables_.find(index);
    if (edge_table_it == edge_tables_.end()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Edge table for edge label triplet not found");
    }
    return edge_table_it->second.get_edge_data_accessor(prop);
  }

  void loadSchema(const std::string& filename);
  inline std::shared_ptr<ColumnBase> GetVertexPropertyColumn(
      uint8_t label, int32_t col_id) const {
    return vertex_tables_[label].get_property_column(col_id);
  }

  inline std::shared_ptr<RefColumnBase> GetVertexPropertyColumn(
      uint8_t label, const std::string& prop) const {
    return vertex_tables_[label].GetPropertyColumn(prop);
  }

  inline VertexSet GetVertexSet(label_t label,
                                timestamp_t ts = MAX_TIMESTAMP) const {
    return vertex_tables_[label].GetVertexSet(ts);
  }

  std::string get_statistics_json() const;

  inline std::string get_schema_yaml_path() const {
    return work_dir_ + "/graph.yaml";
  }

  inline std::string work_dir() const { return work_dir_; }

 private:
  Status delete_vertex_properties_check(const std::string& vertex_type_name,
                                        const std::vector<std::string>& props,
                                        bool error_on_conflict,
                                        std::vector<std::string>& valid_props);
  Status delete_edge_properties_check(const std::string& src_type_name,
                                      const std::string& dst_type_name,
                                      const std::string& edge_type_name,
                                      const std::vector<std::string>& props,
                                      bool error_on_conflict,
                                      std::vector<std::string>& valid_props);

  Status edge_triplet_check(const std::string& src_type_name,
                            const std::string& dst_type_name,
                            const std::string& edge_type_name);

  // Check whether the edge triplet exists, maybe marked as deleted
  Status edge_triplet_exist(const std::string& src_type_name,
                            const std::string& dst_type_name,
                            const std::string& edge_type_name);

  Status vertex_label_check(const std::string& vertex_type_name);

  void compact_schema();

  std::string work_dir_;
  Schema schema_;
  std::vector<std::shared_ptr<std::mutex>> v_mutex_;
  std::vector<VertexTable> vertex_tables_;
  std::unordered_map<uint32_t, EdgeTable> edge_tables_;

  size_t vertex_label_total_count_, edge_label_total_count_;
  MemoryLevel memory_level_;
};

}  // namespace neug
