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
#include <utility>
#include <vector>

#include "neug/execution/common/types/value.h"
#include "neug/storages/allocators.h"
#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/csr/csr_view.h"
#include "neug/storages/graph/edge_table.h"
#include "neug/storages/graph/operation_params.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/graph/vertex_table.h"
#include "neug/utils/exception/exception.h"
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
   * @brief Open the graph from the given Checkpoint using the Module interface.
   *
   * Reads a CheckpointManifest from @p ckp, then opens each module (Schema,
   * VertexTable, EdgeTable) via Module::Open.  If the checkpoint contains no
   * meta the graph starts empty.
   */
  void Open(std::shared_ptr<Checkpoint> ckp, MemoryLevel memory_level);

  void Compact(bool compact_csr, float reserve_ratio, timestamp_t ts);

  /**
   * @brief Dump the current graph state to persistent storage.
   * @param reopen If true, reopens the graph after dumping (default: true)
   */
  void Dump(std::shared_ptr<Checkpoint> ckp, bool reopen = true);

  /**
   * @brief Dump using the graph's own internal Checkpoint.
   * Convenience overload for callers that don't hold a Checkpoint reference.
   */
  void Dump(bool reopen = true) {
    assert(ckp_ && "ckp_ must be set before calling Dump()");
    Dump(ckp_, reopen);
  }

  Checkpoint& checkpoint() {
    assert(ckp_);
    return *ckp_;
  }

  const Checkpoint& checkpoint() const {
    assert(ckp_);
    return *ckp_;
  }

  std::shared_ptr<Checkpoint> checkpoint_ptr() const { return ckp_; }

  MemoryLevel memory_level() const { return memory_level_; }

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
   * Properties are specified as (name, default_value) pairs; the DataType
   * is derived from each Value's type().
   *
   * **Usage Example:**
   * @code{.cpp}
   * CreateVertexTypeParamBuilder builder;
   * auto config = builder.VertexLabel("Person")
   *                   .AddProperty("id", Value::INT64(0))
   *                   .AddProperty("name", Value::STRING(""))
   *                   .AddProperty("age", Value::INT32(0))
   *                   .AddPrimaryKeyName("id")
   *                   .Build();
   * graph.CreateVertexType(config);
   * @endcode
   *
   * @param config Vertex type creation config, including type name,
   *        properties (name + default value), and primary keys
   *
   * @return Status indicating success or failure
   *
   * @since v0.1.0
   */
  Status CreateVertexType(const CreateVertexTypeParam& config);

  /**
   * @brief Create a new edge type in the graph schema.
   *
   * Defines a new edge label connecting source and destination vertex types.
   * Properties are specified as (name, default_value) pairs; the DataType
   * is derived from each Value's type().
   *
   * **Usage Example:**
   * @code{.cpp}
   * CreateEdgeTypeParamBuilder builder;
   * auto config = builder.SrcLabel("Person")
   *                   .DstLabel("Person")
   *                   .EdgeLabel("KNOWS")
   *                   .AddProperty("since", Value::INT64(0))
   *                   .AddProperty("weight", Value::DOUBLE(0.0))
   *                   .OEEdgeStrategy(EdgeStrategy::kMultiple)
   *                   .IEEdgeStrategy(EdgeStrategy::kMultiple)
   *                   .Build();
   * graph.CreateEdgeType(config);
   * @endcode
   *
   * @param config Edge type creation config, including source/destination,
   *        edge label, properties (name + default value), and edge strategies
   *
   * @return Status indicating success or failure. Returns
   *         ERR_SCHEMA_MISMATCH if the type already exists.
   *
   * @since v0.1.0
   */
  Status CreateEdgeType(const CreateEdgeTypeParam& config);

  /**
   * @brief Delete a vertex type physically from the graph storage, could not be
   * reverted.
   * @param vertex_type_name Name of the vertex type to delete
   * @return Status Status indicating success or failure
   */
  Status DeleteVertexType(const std::string& vertex_type_name);

  Status DeleteVertexType(label_t label);

  Status DeleteEdgeType(const std::string& src_vertex_type,
                        const std::string& dst_vertex_type,
                        const std::string& edge_type_name);

  Status DeleteEdgeType(label_t src_label, label_t dst_label,
                        label_t edge_label);

  /**
   * @brief Add properties to an existing vertex type.
   *
   * Each property is a (name, default_value) pair; the DataType is derived
   * from each Value's type().
   *
   * @param config Config specifying the vertex label and new properties
   * @return Status indicating success or failure. Returns
   *         ERR_SCHEMA_MISMATCH if a property already exists.
   */
  Status AddVertexProperties(const AddVertexPropertiesParam& config);

  /**
   * @brief Add properties to an existing edge type.
   *
   * Each property is a (name, default_value) pair; the DataType is derived
   * from each Value's type().
   *
   * @param config Config specifying the edge triplet and new properties
   * @return Status indicating success or failure. Returns
   *         ERR_SCHEMA_MISMATCH if a property already exists.
   */
  Status AddEdgeProperties(const AddEdgePropertiesParam& config);

  Status RenameVertexProperties(const RenameVertexPropertiesParam& config);

  Status RenameEdgeProperties(const RenameEdgePropertiesParam& config);

  Status DeleteVertexProperties(const DeleteVertexPropertiesParam& config);

  Status DeleteEdgeProperties(const DeleteEdgePropertiesParam& config);

  Status EnsureCapacity(label_t v_label, size_t capacity);

  Status EnsureCapacity(label_t src_label, label_t dst_label,
                        label_t edge_label, size_t capacity);

  Status EnsureCapacity(label_t src_label, label_t dst_label,
                        label_t edge_label, size_t src_v_cap, size_t dst_v_cap,
                        size_t capacity);

  Status BatchAddVertices(label_t v_label_id,
                          std::shared_ptr<IDataChunkSupplier> supplier);

  Status BatchAddEdges(label_t src_label, label_t dst_label, label_t edge_label,
                       std::shared_ptr<IDataChunkSupplier> supplier);

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
  Status DeleteVertex(label_t v_label, const execution::Value& oid,
                      timestamp_t ts);
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
    schema_.ensure_vertex_label_valid(vertex_label);
    return vertex_tables_[vertex_label];
  }

  inline const VertexTable& get_vertex_table(label_t vertex_label) const {
    schema_.ensure_vertex_label_valid(vertex_label);
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

  inline bool HasEdgeTable(uint32_t index) const {
    return edge_tables_.count(index) != 0;
  }

  inline EdgeTable& get_edge_table_by_index(uint32_t index) {
    return edge_tables_.at(index);
  }

  vid_t LidNum(label_t vertex_label) const;

  vid_t VertexNum(label_t vertex_label, timestamp_t ts = MAX_TIMESTAMP) const;

  bool IsValidLid(label_t vertex_label, vid_t lid, timestamp_t ts) const;

  size_t EdgeNum(label_t src_label, label_t edge_label,
                 label_t dst_label) const;

  bool get_lid(label_t label, const execution::Value& oid, vid_t& lid,
               timestamp_t ts) const;

  execution::Value GetOid(label_t label, vid_t lid, timestamp_t ts) const;

  Status AddVertex(label_t label, const execution::Value& id,
                   const std::vector<execution::Value>& props, vid_t& vid,
                   timestamp_t ts, bool insert_safe = false);

  Status AddEdge(label_t src_label, vid_t src_lid, label_t dst_label,
                 vid_t dst_lid, label_t edge_label,
                 const std::vector<execution::Value>& properties,
                 timestamp_t ts, Allocator& alloc, int32_t& oe_offset,
                 const void*& prop, bool insert_safe = false);

  Status UpdateVertexProperty(label_t v_label, vid_t vid, int32_t prop_id,
                              const execution::Value& value, timestamp_t ts);

  Status UpdateEdgeProperty(label_t src_label, vid_t src_lid, label_t dst_label,
                            vid_t dst_lid, label_t e_label, int32_t oe_offset,
                            int32_t ie_offset, int32_t col_id,
                            const execution::Value& new_prop, timestamp_t ts);

  /**
   * @brief Get a view for traversing outgoing edges.
   *
   * Returns a CsrView for efficiently iterating over outgoing edges
   * from vertices of type v_label to vertices of type neighbor_label.
   *
   * **Usage Example:**
   * @code{.cpp}
   * // Get view for Person -[KNOWS]-> Person edges
   * label_t person = schema.get_vertex_label_id("Person");
   * label_t knows = schema.get_edge_label_id("KNOWS");
   *
   * CsrView view = graph.GetGenericOutgoingGraphView(
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
   * @return CsrView for outgoing edge traversal
   *
   * @throws std::invalid_argument if edge triplet doesn't exist
   *
   * @see CsrView For traversal operations
   * @see GetGenericIncomingGraphView For reverse traversal
   *
   * @since v0.1.0
   */
  CsrView GetGenericOutgoingGraphView(
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
   * Returns a CsrView for efficiently iterating over incoming edges
   * to vertices of type v_label from vertices of type neighbor_label.
   *
   * **Usage Example:**
   * @code{.cpp}
   * // Get view for Person <-[KNOWS]- Person edges (reverse direction)
   * CsrView view = graph.GetGenericIncomingGraphView(
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
   * @return CsrView for incoming edge traversal
   *
   * @throws std::invalid_argument if edge triplet doesn't exist
   *
   * @see CsrView For traversal operations
   * @see GetGenericOutgoingGraphView For forward traversal
   *
   * @since v0.1.0
   */
  CsrView GetGenericIncomingGraphView(
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
   * CsrView view = graph.GetGenericOutgoingGraphView(...);
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
  inline std::shared_ptr<RefColumnBase> GetVertexPropertyColumn(
      uint8_t label, int32_t col_id) const {
    schema_.ensure_vertex_label_valid(label);
    auto props = schema_.get_vertex_properties(label);
    if (col_id < 0 || static_cast<size_t>(col_id) >= props.size()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Vertex property column id out of range: " + std::to_string(col_id) +
          " (label has " + std::to_string(props.size()) + " properties)");
    }
    return vertex_tables_[label].GetPropertyColumn(col_id);
  }

  inline std::shared_ptr<RefColumnBase> GetVertexPropertyColumn(
      uint8_t label, const std::string& prop) const {
    schema_.ensure_vertex_label_valid(label);
    return vertex_tables_[label].GetPropertyColumn(prop);
  }

  inline VertexSet GetVertexSet(label_t label,
                                timestamp_t ts = MAX_TIMESTAMP) const {
    schema_.ensure_vertex_label_valid(label);
    return vertex_tables_[label].GetVertexSet(ts);
  }

  std::string get_statistics_json() const;

  inline std::string work_dir() const { return ckp_->path(); }

  std::shared_ptr<PropertyGraph> Clone() const;

 private:
  Status delete_vertex_properties_check(const std::string& vertex_type_name,
                                        const std::vector<std::string>& props,
                                        std::vector<std::string>& valid_props);
  Status delete_edge_properties_check(const std::string& src_type_name,
                                      const std::string& dst_type_name,
                                      const std::string& edge_type_name,
                                      const std::vector<std::string>& props,
                                      std::vector<std::string>& valid_props);

  Status vertex_label_check(const std::string& vertex_type_name) const;
  Status vertex_label_check(label_t label) const;

  Status edge_triplet_check(const std::string& src_type_name,
                            const std::string& dst_type_name,
                            const std::string& edge_type_name) const;
  Status edge_triplet_check(label_t src_label, label_t dst_label,
                            label_t edge_label) const;

  void compact_schema();

  std::shared_ptr<Checkpoint> ckp_;
  Schema schema_;
  std::vector<std::shared_ptr<std::mutex>> v_mutex_;
  std::vector<VertexTable> vertex_tables_;
  std::unordered_map<uint32_t, EdgeTable> edge_tables_;

  size_t vertex_label_total_count_, edge_label_total_count_;
  MemoryLevel memory_level_;

  friend class GraphView;
};

}  // namespace neug
