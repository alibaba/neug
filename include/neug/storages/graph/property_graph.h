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

#include <arrow/api.h>
#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <glog/logging.h>
#include <stddef.h>
#include <algorithm>
#include <atomic>
#include <cstdint>

#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "libgrape-lite/grape/utils/concurrent_queue.h"
#include "neug/execution/common/utils/bitset.h"
#include "neug/storages/csr/dual_csr.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/file_names.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/loader/abstract_arrow_fragment_loader.h"
#include "neug/storages/loader/basic_fragment_loader.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/allocators.h"
#include "neug/utils/arrow_utils.h"
#include "neug/utils/id_indexer.h"
#include "neug/utils/indexers.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

#include "neug/storages/graph/edge_table.h"
#include "neug/storages/graph/vertex_table.h"

namespace grape {
class OutArchive;
struct EmptyType;
}  // namespace grape

namespace gs {
class CsrBase;
class CsrConstEdgeIterBase;
class CsrEdgeIterBase;
template <typename EDATA_T>
class TypedMutableCsrBase;

class PropertyGraph {
 public:
  static constexpr const float DEFAULT_RESERVE_RATIO = 1.2;
  PropertyGraph();

  ~PropertyGraph();

  void IngestEdge(label_t src_label, vid_t src_lid, label_t dst_label,
                  vid_t dst_lid, label_t edge_label, timestamp_t ts,
                  grape::OutArchive& arc, Allocator& alloc);

  void UpdateEdge(label_t src_label, vid_t src_lid, label_t dst_label,
                  vid_t dst_lid, label_t edge_label, timestamp_t ts,
                  const Any& arc, Allocator& alloc);

  void Open(const std::string& work_dir, int memory_level);

  void Compact(bool reset_timestamp, bool compact_csr, float reserve_ratio,
               timestamp_t ts);

  void Dump(const std::string& work_dir, uint32_t version);

  void DumpSchema(const std::string& filename);

  const Schema& schema() const;

  Schema& mutable_schema();

  void Clear();

  // When error_on_conflict is true, it will return an error if the
  // vertex type or edge type already exists.
  // When error_on_conflict is false, it will skip the creation if the
  // vertex type or edge type already exists.
  Status create_vertex_type(
      const std::string& vertex_type_name,
      const std::vector<std::tuple<PropertyType, std::string, Any>>& properties,
      const std::vector<std::string>& primary_key_names,
      bool error_on_conflict = true);

  Status create_edge_type(
      const std::string& src_vertex_type, const std::string& dst_vertex_type,
      const std::string& edge_type_name,
      const std::vector<std::tuple<PropertyType, std::string, Any>>& properties,
      bool error_on_conflict = true,
      EdgeStrategy oe_strategy = EdgeStrategy::kMultiple,
      EdgeStrategy ie_strategy = EdgeStrategy::kMultiple);

  Status add_vertex_properties(
      const std::string& vertex_type_name,
      const std::vector<std::tuple<PropertyType, std::string, Any>>&
          add_properties,
      bool error_on_conflict = true);

  Status add_edge_properties(
      const std::string& src_type_name, const std::string& dst_type_name,
      const std::string& edge_type_name,
      const std::vector<std::tuple<PropertyType, std::string, Any>>&
          add_properties,
      bool error_on_conflict = true);

  Status rename_vertex_properties(
      const std::string& vertex_type_name,
      const std::vector<std::tuple<std::string, std::string>>&
          rename_properties,
      bool error_on_conflict = true);

  Status rename_edge_properties(
      const std::string& src_type_name, const std::string& dst_type_name,
      const std::string& edge_type_name,
      const std::vector<std::tuple<std::string, std::string>>&
          rename_properties,
      bool error_on_conflict = true);

  Status delete_vertex_properties(
      const std::string& vertex_type_name,
      const std::vector<std::string>& delete_properties,
      bool error_on_conflict = true);

  Status delete_edge_properties(
      const std::string& src_type_name, const std::string& dst_type_name,
      const std::string& edge_type_name,
      const std::vector<std::string>& delete_properties,
      bool error_on_conflict = true);

  Status delete_vertex_type(const std::string& vertex_type_name, bool is_detach,
                            bool error_on_conflict);

  Status delete_edge_type(const std::string& src_vertex_type,
                          const std::string& dst_vertex_type,
                          const std::string& edge_type, bool error_on_conflict);

  Status batch_add_vertices(label_t v_label_id, std::vector<Any>&& ids,
                            std::unique_ptr<Table>&& table, timestamp_t ts);

  Status batch_add_edges(
      label_t src_label_id, label_t dst_label_id, label_t edge_label_id,
      std::vector<std::tuple<vid_t, vid_t, size_t>>&& edges_vec,
      std::unique_ptr<Table>&& table);

  Status batch_delete_vertices(const label_t& v_label_id,
                               const std::vector<vid_t>& vids);

  Status batch_delete_edges(const label_t& src_v_label,
                            const label_t& dst_v_label,
                            const label_t& edge_label,
                            std::vector<std::tuple<vid_t, vid_t>>& edges_vec);

  inline Table& get_vertex_table(label_t vertex_label) {
    return vertex_tables_[vertex_label].get_properties_table();
  }

  inline const Table& get_vertex_table(label_t vertex_label) const {
    return vertex_tables_[vertex_label].get_properties_table();
  }

  vid_t lid_num(label_t vertex_label) const;

  vid_t vertex_num(label_t vertex_label, timestamp_t ts = MAX_TIMESTAMP) const;

  bool is_valid_lid(label_t vertex_label, vid_t lid, timestamp_t ts) const;

  size_t edge_num(label_t src_label, label_t edge_label,
                  label_t dst_label) const;

  bool get_lid(label_t label, const Any& oid, vid_t& lid, timestamp_t ts) const;

  Any get_oid(label_t label, vid_t lid, timestamp_t ts) const;

  vid_t add_vertex(label_t label, const Any& id, timestamp_t ts);

  vid_t add_vertex_safe(label_t label, const Any& id, timestamp_t ts);

  std::shared_ptr<CsrConstEdgeIterBase> get_outgoing_edges(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const;

  std::shared_ptr<CsrConstEdgeIterBase> get_incoming_edges(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const;

  std::shared_ptr<CsrEdgeIterBase> get_outgoing_edges_mut(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label);

  std::shared_ptr<CsrEdgeIterBase> get_incoming_edges_mut(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label);

  inline CsrBase* get_oe_csr(label_t label, label_t neighbor_label,
                             label_t edge_label) {
    size_t index =
        schema_.generate_edge_label(label, neighbor_label, edge_label);
    if (edge_tables_.find(index) == edge_tables_.end()) {
      LOG(ERROR) << "Edge csr not found for label: " << label
                 << ", neighbor_label: " << neighbor_label
                 << ", edge_label: " << edge_label;
      return nullptr;
    }
    return edge_tables_.at(index).GetOutCsr();
  }

  inline const CsrBase* get_oe_csr(label_t label, label_t neighbor_label,
                                   label_t edge_label) const {
    size_t index =
        schema_.generate_edge_label(label, neighbor_label, edge_label);
    if (edge_tables_.find(index) == edge_tables_.end()) {
      LOG(ERROR) << "Edge csr not found for label: " << label
                 << ", neighbor_label: " << neighbor_label
                 << ", edge_label: " << edge_label;
      return nullptr;
    }
    return edge_tables_.at(index).GetOutCsr();
  }

  inline CsrBase* get_ie_csr(label_t label, label_t neighbor_label,
                             label_t edge_label) {
    size_t index =
        schema_.generate_edge_label(neighbor_label, label, edge_label);
    if (edge_tables_.find(index) == edge_tables_.end()) {
      LOG(ERROR) << "Edge csr not found for label: " << label
                 << ", neighbor_label: " << neighbor_label
                 << ", edge_label: " << edge_label;
      return nullptr;
    }
    return edge_tables_.at(index).GetInCsr();
  }

  inline const CsrBase* get_ie_csr(label_t label, label_t neighbor_label,
                                   label_t edge_label) const {
    size_t index =
        schema_.generate_edge_label(neighbor_label, label, edge_label);
    if (edge_tables_.find(index) == edge_tables_.end()) {
      LOG(ERROR) << "Edge csr not found for label: " << (int32_t) label
                 << ", neighbor_label: " << (int32_t) neighbor_label
                 << ", edge_label: " << (int32_t) edge_label;
      return nullptr;
    }
    return edge_tables_.at(index).GetInCsr();
  }

  inline bool vertex_table_modified(label_t label) const {
    return vertex_tables_[label].vertex_table_modified();
  }

  void loadSchema(const std::string& filename);

  inline const mmap_array<timestamp_t>& get_vertex_timestamps(
      label_t label) const {
    return vertex_tables_[label].get_vertex_timestamps();
  }

  inline std::shared_ptr<ColumnBase> get_vertex_property_column(
      uint8_t label, const std::string& prop) const {
    return vertex_tables_[label].get_property_column(prop);
  }

  inline std::shared_ptr<RefColumnBase> get_vertex_id_column(
      uint8_t label) const {
    return vertex_tables_[label].get_vertex_id_column();
  }

  inline std::string statisticsFilePath() const {
    return work_dir_ + "/statistics.json";
  }

  std::string get_statistics_json() const;

  inline std::string get_schema_yaml_path() const {
    return work_dir_ + "/graph.yaml";
  }

  void generateStatistics() const;

 public:
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
