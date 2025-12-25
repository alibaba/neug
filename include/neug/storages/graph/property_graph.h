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

#include "neug/storages/csr/generic_view.h"
#include "neug/storages/graph/edge_table.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/graph/vertex_table.h"
#include "neug/utils/allocators.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace gs {

namespace runtime {
class EdgeRecord;
}

/**
 * @brief Core property graph storage engine managing vertices, edges, and
 * schema.
 *
 * PropertyGraph provides the fundamental storage layer for graph data in
 * NeuG. It manages vertex and edge tables, schema information, and provides
 * persistence through file-based storage with memory management
 * optimizations.
 *
 * **Key Components:**
 * - Vertex tables for storing vertex data and properties
 * - Edge tables using various CSR (Compressed Sparse Row) formats
 * - Schema management for vertex/edge types and properties
 * - Memory management with configurable memory levels
 * - Persistence with snapshot and compaction support
 *
 * **Implementation Details:**
 * - vertex_tables_ stores vertex data indexed by label
 * - edge_tables_ stores edge data in CSR format
 * - schema_ manages type definitions and property schemas
 * - work_dir_ stores the working directory for persistence
 * - memory_level_ controls memory usage vs performance tradeoff
 *
 * @since v0.1.0
 */
class PropertyGraph {
 public:
  /**
   * @brief Construct PropertyGraph with default settings.
   *
   * Implementation: Initializes vertex_label_num_=0, edge_label_num_=0,
   * memory_level_=1.
   *
   * @since v0.1.0
   */
  PropertyGraph();

  /**
   * @brief Destructor that reserves space and cleans up resources.
   *
   * Implementation: Calculates degree lists for vertices, reserves space in
   * vertex and edge tables to optimize memory layout before destruction.
   *
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
  void Open(const std::string& work_dir, int memory_level);

  void Open(const Schema& schema, const std::string& work_dir,
            int memory_level);

  void Compact(bool compact_csr, float reserve_ratio, timestamp_t ts);

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

  // When error_on_conflict is true, it will return an error if the
  // vertex type or edge type already exists.
  // When error_on_conflict is false, it will skip the creation if the
  // vertex type or edge type already exists.
  Status CreateVertexType(
      const std::string& vertex_type_name,
      const std::vector<std::tuple<DataTypeId, std::string, Property>>&
          properties,
      const std::vector<std::string>& primary_key_names,
      bool error_on_conflict = true);

  Status CreateEdgeType(
      const std::string& src_vertex_type, const std::string& dst_vertex_type,
      const std::string& edge_type_name,
      const std::vector<std::tuple<DataTypeId, std::string, Property>>&
          properties,
      bool error_on_conflict = true,
      EdgeStrategy oe_strategy = EdgeStrategy::kMultiple,
      EdgeStrategy ie_strategy = EdgeStrategy::kMultiple);

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

  Status AddVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::tuple<DataTypeId, std::string, Property>>&
          add_properties,
      bool error_on_conflict = true);

  Status AddEdgeProperties(
      const std::string& src_type_name, const std::string& dst_type_name,
      const std::string& edge_type_name,
      const std::vector<std::tuple<DataTypeId, std::string, Property>>&
          add_properties,
      bool error_on_conflict = true);

  Status RenameVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict = true);

  Status RenameEdgeProperties(
      const std::string& src_type_name, const std::string& dst_type_name,
      const std::string& edge_type_name,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict = true);

  Status DeleteVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::string>& delete_properties,
      bool error_on_conflict = true);

  Status DeleteEdgeProperties(const std::string& src_type_name,
                              const std::string& dst_type_name,
                              const std::string& edge_type_name,
                              const std::vector<std::string>& delete_properties,
                              bool error_on_conflict = true);

  Status Reserve(label_t v_label, vid_t vertex_reserve_size);

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
                   timestamp_t ts);

  int32_t AddEdge(label_t src_label, vid_t src_lid, label_t dst_label,
                  vid_t dst_lid, label_t edge_label,
                  const std::vector<Property>& properties, timestamp_t ts,
                  Allocator& alloc);

  Status UpdateVertexProperty(label_t v_label, vid_t vid, int32_t prop_id,
                              const Property& value, timestamp_t ts);

  Status UpdateEdgeProperty(label_t src_label, vid_t src_lid, label_t dst_label,
                            vid_t dst_lid, label_t e_label, int32_t oe_offset,
                            int32_t ie_offset, int32_t col_id,
                            const Property& new_prop, timestamp_t ts);

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

  inline std::string statisticsFilePath() const {
    return work_dir_ + "/statistics.json";
  }

  std::string get_statistics_json() const;

  inline std::string get_schema_yaml_path() const {
    return work_dir_ + "/graph.yaml";
  }

  inline std::string work_dir() const { return work_dir_; }

  void generateStatistics() const;

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

  size_t vertex_label_num_, edge_label_num_;
  int memory_level_;
};

}  // namespace gs
