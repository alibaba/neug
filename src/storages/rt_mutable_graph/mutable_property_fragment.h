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

#include <thread>
#include <tuple>
#include <vector>

#include "src/storages/rt_mutable_graph/schema.h"

#include "src/storages/rt_mutable_graph/csr/mutable_csr.h"
#include "src/storages/rt_mutable_graph/dual_csr.h"
#include "src/storages/rt_mutable_graph/loader/loader_utils.h"
#include "src/storages/rt_mutable_graph/types.h"
#include "src/utils/arrow_utils.h"
#include "src/utils/indexers.h"
#include "src/utils/property/table.h"
#include "src/utils/yaml_utils.h"
#include "third_party/libgrape-lite/grape/io/local_io_adaptor.h"
#include "third_party/libgrape-lite/grape/serialization/out_archive.h"

namespace gs {

class MutablePropertyFragment {
 public:
  MutablePropertyFragment();

  ~MutablePropertyFragment();

  void IngestEdge(label_t src_label, vid_t src_lid, label_t dst_label,
                  vid_t dst_lid, label_t edge_label, timestamp_t ts,
                  grape::OutArchive& arc, Allocator& alloc);

  void UpdateEdge(label_t src_label, vid_t src_lid, label_t dst_label,
                  vid_t dst_lid, label_t edge_label, timestamp_t ts,
                  const Any& arc, Allocator& alloc);

  void Open(const std::string& work_dir, int memory_level);

  void Compact(uint32_t version);

  void Warmup(int thread_num);

  void Dump(const std::string& work_dir, uint32_t version);

  void DumpSchema(const std::string& filename);

  const Schema& schema() const;

  Schema& mutable_schema();

  void Clear();

  Status create_vertex_type(
      const std::string& vertex_type_name,
      const std::vector<std::tuple<PropertyType, std::string, Any>>& properties,
      const std::vector<std::string>& primary_key_names);

  Status create_edge_type(
      const std::string& src_vertex_type, const std::string& dst_vertex_type,
      const std::string& edge_type_name,
      const std::vector<std::tuple<PropertyType, std::string, Any>>& properties,
      EdgeStrategy oe_strategy = EdgeStrategy::kMultiple,
      EdgeStrategy ie_strategy = EdgeStrategy::kMultiple);

  Status update_vertex_type(
      const std::string& vertex_type_name,
      const std::vector<std::tuple<PropertyType, std::string, Any>>&
          add_properties,
      const std::vector<std::tuple<std::string, Any>>& update_properties,
      const std::vector<std::tuple<PropertyType, std::string, Any>>&
          delete_properties,
      bool skip_exists);

  Status update_edge_type(
      const std::string& src_type_name, const std::string& dst_type_name,
      const std::string& edge_type_name,
      const std::vector<std::tuple<PropertyType, std::string, Any>>&
          add_properties,
      const std::vector<std::tuple<std::string, Any>>& update_properties,
      const std::vector<std::tuple<PropertyType, std::string, Any>>&
          delete_properties,
      bool skip_exists);

  Status delete_vertex_type(const std::string& vertex_type_name, bool is_detach,
                            bool skip_exists);

  Status delete_edge_type(const std::string& src_vertex_type,
                          const std::string& dst_vertex_type,
                          const std::string& edge_type, bool skip_exists);

  template <typename PK_T>
  Status batch_load_vertices(const label_t& vertex_type_id,
                             std::vector<std::shared_ptr<IRecordBatchSupplier>>&
                                 record_batch_supplier_vec) {
    LOG(INFO) << "Batch load vertices";
    return Status::OK();
  }

  template <typename SRC_PK_T, typename DST_PK_T, typename EDATA_T>
  Status batch_load_edges(label_t src_label_id, label_t dst_label_id,
                          label_t edge_label_id,
                          std::vector<std::shared_ptr<IRecordBatchSupplier>>&
                              record_batch_supplier_vec) {
    LOG(INFO) << "Batch load edges";
    return Status::OK();
  }

  template <typename SRC_PK_T, typename DST_PK_T, typename EDATA_T>
  Status batch_load_edges(const std::string& src_vertex_type,
                          const std::string& dst_vertex_type,
                          const std::string& edge_type_name,
                          std::vector<std::shared_ptr<IRecordBatchSupplier>>&
                              record_batch_supplier_vec) {
    auto src_label_id = schema_.get_vertex_label_id(src_vertex_type);
    auto dst_label_id = schema_.get_vertex_label_id(dst_vertex_type);
    auto edge_label_id = schema_.get_edge_label_id(edge_type_name);
    return batch_load_edges<SRC_PK_T, DST_PK_T, EDATA_T>(
        src_label_id, dst_label_id, edge_label_id, record_batch_supplier_vec);
  }

  inline Table& get_vertex_table(label_t vertex_label) {
    return vertex_data_[vertex_label];
  }

  inline const Table& get_vertex_table(label_t vertex_label) const {
    return vertex_data_[vertex_label];
  }

  vid_t vertex_num(label_t vertex_label) const;

  size_t edge_num(label_t src_label, label_t edge_label,
                  label_t dst_label) const;

  bool get_lid(label_t label, const Any& oid, vid_t& lid) const;

  Any get_oid(label_t label, vid_t lid) const;

  vid_t add_vertex(label_t label, const Any& id);
  std::shared_ptr<CsrConstEdgeIterBase> get_outgoing_edges(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const;

  std::shared_ptr<CsrConstEdgeIterBase> get_incoming_edges(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const;

  std::shared_ptr<CsrEdgeIterBase> get_outgoing_edges_mut(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label);

  std::shared_ptr<CsrEdgeIterBase> get_incoming_edges_mut(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label);

  CsrConstEdgeIterBase* get_outgoing_edges_raw(label_t label, vid_t u,
                                               label_t neighbor_label,
                                               label_t edge_label) const;

  CsrConstEdgeIterBase* get_incoming_edges_raw(label_t label, vid_t u,
                                               label_t neighbor_label,
                                               label_t edge_label) const;

  inline CsrBase* get_oe_csr(label_t label, label_t neighbor_label,
                             label_t edge_label) {
    size_t index = label * vertex_label_num_ * edge_label_num_ +
                   neighbor_label * edge_label_num_ + edge_label;
    return oe_[index];
  }

  inline const CsrBase* get_oe_csr(label_t label, label_t neighbor_label,
                                   label_t edge_label) const {
    size_t index = label * vertex_label_num_ * edge_label_num_ +
                   neighbor_label * edge_label_num_ + edge_label;
    return oe_[index];
  }

  inline CsrBase* get_ie_csr(label_t label, label_t neighbor_label,
                             label_t edge_label) {
    size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                   label * edge_label_num_ + edge_label;
    return ie_[index];
  }

  inline const CsrBase* get_ie_csr(label_t label, label_t neighbor_label,
                                   label_t edge_label) const {
    size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                   label * edge_label_num_ + edge_label;
    return ie_[index];
  }

  void loadSchema(const std::string& filename);
  inline std::shared_ptr<ColumnBase> get_vertex_property_column(
      uint8_t label, const std::string& prop) const {
    return vertex_data_[label].get_column(prop);
  }

  inline std::shared_ptr<RefColumnBase> get_vertex_id_column(
      uint8_t label) const {
    if (lf_indexers_[label].get_type() == PropertyType::kInt64) {
      return std::make_shared<TypedRefColumn<int64_t>>(
          dynamic_cast<const TypedColumn<int64_t>&>(
              lf_indexers_[label].get_keys()));
    } else if (lf_indexers_[label].get_type() == PropertyType::kInt32) {
      return std::make_shared<TypedRefColumn<int32_t>>(
          dynamic_cast<const TypedColumn<int32_t>&>(
              lf_indexers_[label].get_keys()));
    } else if (lf_indexers_[label].get_type() == PropertyType::kUInt64) {
      return std::make_shared<TypedRefColumn<uint64_t>>(
          dynamic_cast<const TypedColumn<uint64_t>&>(
              lf_indexers_[label].get_keys()));
    } else if (lf_indexers_[label].get_type() == PropertyType::kUInt32) {
      return std::make_shared<TypedRefColumn<uint32_t>>(
          dynamic_cast<const TypedColumn<uint32_t>&>(
              lf_indexers_[label].get_keys()));
    } else if (lf_indexers_[label].get_type() == PropertyType::kStringView) {
      return std::make_shared<TypedRefColumn<std::string_view>>(
          dynamic_cast<const TypedColumn<std::string_view>&>(
              lf_indexers_[label].get_keys()));
    } else {
      LOG(ERROR) << "Unsupported vertex id type: "
                 << lf_indexers_[label].get_type();
      return nullptr;
    }
  }

  inline std::string statisticsFilePath() const {
    return work_dir_ + "/statistics.json";
  }

  std::string get_statistics_json() const;

  inline std::string get_schema_yaml_path() const {
    return work_dir_ + "/graph.yaml";
  }

  void generateStatistics() const;

  std::string work_dir_;
  Schema schema_;
  std::vector<IndexerType> lf_indexers_;
  std::vector<CsrBase*> ie_, oe_;
  std::vector<DualCsrBase*> dual_csr_list_;
  std::vector<Table> vertex_data_;

  size_t vertex_label_num_, edge_label_num_;
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_MUTABLE_PROPERTY_FRAGMENT_H_
