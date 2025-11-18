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

#ifndef STORAGES_RT_MUTABLE_GRAPH_MUTABLE_PROPERTY_FRAGMENT_H_
#define STORAGES_RT_MUTABLE_GRAPH_MUTABLE_PROPERTY_FRAGMENT_H_

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

namespace grape {
class OutArchive;
}  // namespace grape

namespace gs {

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
  static constexpr const float DEFAULT_RESERVE_RATIO = 1.2;
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

  void Compact(bool reset_timestamp, bool compact_csr, float reserve_ratio,
               timestamp_t ts);

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
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          properties,
      const std::vector<std::string>& primary_key_names,
      bool error_on_conflict = true);

  Status CreateEdgeType(
      const std::string& src_vertex_type, const std::string& dst_vertex_type,
      const std::string& edge_type_name,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
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
                          bool error_on_conflict = true, bool is_soft = false);

  /**
   * @brief Revert a logical deletion of a vertex type.
   * @param label_id Expect label_id not label_name
   * @return Status Status indicating success or failure
   */
  Status RevertDeleteVertexType(label_t label_id,
                                bool error_on_conflict = true);

  Status DeleteEdgeType(const std::string& src_vertex_type,
                        const std::string& dst_vertex_type,
                        const std::string& edge_type_name,
                        bool error_on_conflict = true, bool is_soft = false);

  // Could only revert soft delete
  Status RevertDeleteEdgeType(const std::string& src_vertex_type,
                              const std::string& dst_vertex_type,
                              const std::string& edge_type_name,
                              bool error_on_conflict = true);

  Status RevertDeleteEdgeType(label_t src_label, label_t dst_label,
                              label_t edge_label,
                              bool error_on_conflict = true);

  Status AddVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          add_properties,
      bool error_on_conflict = true);

  Status AddEdgeProperties(
      const std::string& src_type_name, const std::string& dst_type_name,
      const std::string& edge_type_name,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
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
      bool error_on_conflict = true, bool is_soft = false);

  Status RevertDeleteVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::string>& delete_properties,
      bool error_on_conflict = true);

  Status DeleteEdgeProperties(const std::string& src_type_name,
                              const std::string& dst_type_name,
                              const std::string& edge_type_name,
                              const std::vector<std::string>& delete_properties,
                              bool error_on_conflict = true,
                              bool is_soft = false);

  Status RevertDeleteEdgeProperties(
      const std::string& src_type_name, const std::string& dst_type_name,
      const std::string& edge_type_name,
      const std::vector<std::string>& delete_properties,
      bool error_on_conflict = true);

  Status RevertDeleteEdgeProperties(
      label_t src_label, label_t dst_label, label_t edge_label,
      const std::vector<std::string>& delete_properties,
      bool error_on_conflict = true);

  Status Reserve(label_t v_label, vid_t vertex_reserve_size);

  Status BatchAddVertices(label_t v_label_id,
                          std::shared_ptr<IRecordBatchSupplier> supplier);

  Status BatchAddEdges(label_t src_label, label_t dst_label, label_t edge_label,
                       std::shared_ptr<IRecordBatchSupplier> supplier);

  Status BatchDeleteVertices(const label_t& v_label_id,
                             const std::vector<vid_t>& vids);

  Status DeleteVertex(label_t v_label, const Property& oid, timestamp_t ts);

  Status DeleteVertex(label_t v_label, vid_t lid, timestamp_t ts);

  Status BatchDeleteEdges(
      const label_t& src_v_label, const label_t& dst_v_label,
      const label_t& edge_label,
      const std::vector<std::tuple<vid_t, vid_t>>& edges_vec);

  inline VertexTable& get_vertex_table(label_t vertex_label) {
    return vertex_tables_[vertex_label];
  }

  inline const VertexTable& get_vertex_table(label_t vertex_label) const {
    return vertex_tables_[vertex_label];
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

  Status AddEdge(label_t src_label, vid_t src_lid, label_t dst_label,
                 vid_t dst_lid, label_t edge_label,
                 const std::vector<Property>& properties, timestamp_t ts,
                 Allocator& alloc);

  GenericView GetGenericOutgoingGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label,
      timestamp_t ts = std::numeric_limits<timestamp_t>::max()) const {
    size_t index =
        schema_.generate_edge_label(v_label, neighbor_label, edge_label);
    return edge_tables_.at(index).get_outgoing_view(ts);
  }

  GenericView GetGenericIncomingGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label,
      timestamp_t ts = std::numeric_limits<timestamp_t>::max()) const {
    size_t index =
        schema_.generate_edge_label(neighbor_label, v_label, edge_label);
    return edge_tables_.at(index).get_incoming_view(ts);
  }

  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label, int prop_id) const {
    size_t index =
        schema_.generate_edge_label(src_label, dst_label, edge_label);
    return edge_tables_.at(index).get_edge_data_accessor(prop_id);
  }

  void loadSchema(const std::string& filename);

  inline std::shared_ptr<ColumnBase> GetVertexPropertyColumn(
      uint8_t label, const std::string& prop) const {
    return vertex_tables_[label].get_property_column(prop);
  }

  inline std::shared_ptr<RefColumnBase> GetVertexIdColumn(uint8_t label) const {
    return vertex_tables_[label].GetVertexIdColumn();
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
  std::string work_dir_;
  Schema schema_;
  std::vector<std::shared_ptr<std::mutex>> v_mutex_;
  std::vector<VertexTable> vertex_tables_;
  std::unordered_map<uint32_t, EdgeTable> edge_tables_;

  size_t vertex_label_num_, edge_label_num_;
  int memory_level_;
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_MUTABLE_PROPERTY_FRAGMENT_H_
