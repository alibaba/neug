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

#ifndef STORAGES_RT_MUTABLE_GRAPH_EDGE_TABLE_H_
#define STORAGES_RT_MUTABLE_GRAPH_EDGE_TABLE_H_

#include "libgrape-lite/grape/serialization/in_archive.h"
#include "neug/storages/csr/dual_csr.h"
#include "neug/utils/allocators.h"
#include "neug/utils/property/table.h"

namespace gs {
class EdgeTable {
 public:
  EdgeTable(const std::string& src_label_name, const std::string& dst_label_name,
            const std::string& edge_label_name, EdgeStrategy oe_strategy, EdgeStrategy ie_strategy,
            const std::vector<std::string>& prop_names, const std::vector<PropertyType>& prop_types,
            bool oe_mutable, bool ie_mutable);

  EdgeTable(EdgeTable&& edge_table);

  EdgeTable(const EdgeTable&) = delete;

  void BatchInit(const std::string& work_dir, const std::vector<int>& oe_degree,
                 const std::vector<int>& ie_degree, bool in_memory = false);

  void Open(const std::string& snapshot_dir, const std::string& work_dir, int memory_level,
            size_t src_vertex_cap = 0, size_t dst_vertex_cap = 0);

  void BatchAddEdges(std::vector<std::tuple<vid_t, vid_t, size_t>>&& edges,
                     std::unique_ptr<Table>&& table);

  void Dump(const std::string& new_snapshot_dir);

  CsrBase* GetInCsr() { return dual_csr_->GetInCsr(); }
  CsrBase* GetOutCsr() { return dual_csr_->GetOutCsr(); }
  const CsrBase* GetInCsr() const { return dual_csr_->GetInCsr(); }
  const CsrBase* GetOutCsr() const { return dual_csr_->GetOutCsr(); }

  void IngestEdge(vid_t src, vid_t dst, grape::OutArchive& oarc, timestamp_t ts, Allocator& alloc);

  void SortByEdgeData(timestamp_t ts);

  void UpdateEdge(vid_t src, vid_t dst, const Any& data, timestamp_t ts, Allocator& alloc);

  void BatchDeleteVertices(bool is_src, const std::vector<vid_t>& vids);

  void BatchDeleteEdge(const std::vector<std::tuple<vid_t, vid_t>>& parsed_edges_vec);

  void Close() {
    dual_csr_->Close();
    table_->close();
  }

  ~EdgeTable() {
    if (dual_csr_) {
      dual_csr_->Close();
      delete dual_csr_;
      dual_csr_ = nullptr;
    }
    if (table_) {
      table_->close();
    }
  }

  void Reserve(vid_t src_vertex_num, vid_t dst_vertex_num);

  size_t EdgeNum() const { return dual_csr_->EdgeNum(); }

  DualCsrBase* GetDualCsr() { return dual_csr_; }

  // TODO: remove files in work_dir
  void Drop() {
    if (dual_csr_) {
      dual_csr_->Drop();
      delete dual_csr_;
      dual_csr_ = nullptr;
    }
    table_->drop();
  }

  void RenameProperties(const std::vector<std::string>& old_names,
                        const std::vector<std::string>& new_names);

  void AddProperties(const std::vector<std::string>&, const std::vector<PropertyType>&);

  void DeleteProperties(const std::vector<std::string>& col_names);

  inline Table& get_properties_table() { return *table_; }
  inline const Table& get_properties_table() const { return *table_; }

  std::string work_dir() const { return work_dir_; }

 private:
  static DualCsrBase* create_dual_csr(EdgeStrategy oes, EdgeStrategy ies,
                                      const std::vector<PropertyType>& properties, bool oe_mutable,
                                      bool ie_mutable, const std::vector<std::string>& prop_names,
                                      Table& table, std::atomic<size_t>& offset);

  inline bool has_complex_property() const {
    if (prop_names_.empty()) {
      return false;
    }
    return (prop_types_.size() > 1 ||
            prop_types_[0].type_enum == impl::PropertyTypeImpl::kVarChar ||
            prop_types_[0].type_enum == impl::PropertyTypeImpl::kStringView);
  }

  void drop_and_create_dual_csr();

  template <typename EDATA_T>
  void BatchPutEdgeUtil(const std::vector<std::vector<vid_t>>& parsed_edges_vec,
                        const std::vector<Any>& prop_values,
                        std::vector<std::tuple<vid_t, vid_t, size_t>>&& append_vec,
                        std::unique_ptr<Table>&& table, size_t prev_size) {
    if constexpr (std::is_same_v<EDATA_T, std::string_view>) {
      auto casted_dual_csr = dynamic_cast<DualCsr<std::string_view>*>(dual_csr_);

      std::vector<std::string> str_values;
      for (const auto& prop : prop_values) {
        str_values.emplace_back(std::string(prop.AsStringView()));
      }

      size_t row_id = 0;
      auto typed_column = dynamic_cast<StringColumn*>(table->get_column_by_id(0).get());
      for (size_t i = 0; i < parsed_edges_vec.size(); ++i) {
        for (vid_t dst : parsed_edges_vec[i]) {
          typed_column->set_value_with_resize(row_id, str_values[row_id]);
          casted_dual_csr->BatchPutEdge(i, dst, row_id++);
        }
      }
      for (const auto& [v0, v1, prop] : append_vec) {
        auto sv = table->get_column_by_id(0)->get(prop).AsStringView();
        typed_column->set_value_with_resize(row_id, std::string(sv));
        casted_dual_csr->BatchPutEdge(v0, v1, row_id++);
      }
    } else if constexpr (std::is_same_v<EDATA_T, RecordView>) {
      auto casted_dual_csr = dynamic_cast<DualCsr<RecordView>*>(dual_csr_);
      size_t row_id = prev_size;
      for (size_t i = 0; i < append_vec.size(); ++i) {
        const auto& [v0, v1, index] = append_vec[i];
        table_->insert(row_id, table->get_row(index));
        std::get<2>(append_vec[i]) = row_id++;
      }
      row_id = 0;
      for (size_t i = 0; i < parsed_edges_vec.size(); ++i) {
        for (vid_t dst : parsed_edges_vec[i]) {
          RecordView val;
          ConvertAny<RecordView>::to(prop_values[row_id++], val);
          casted_dual_csr->BatchPutEdge(i, dst, val.offset);
        }
      }
      for (const auto& [v0, v1, prop] : append_vec) {
        casted_dual_csr->BatchPutEdge(v0, v1, prop);
      }
    } else if constexpr (std::is_same_v<EDATA_T, grape::EmptyType>) {
      auto casted_dual_csr = dynamic_cast<DualCsr<grape::EmptyType>*>(dual_csr_);
      for (size_t i = 0; i < parsed_edges_vec.size(); ++i) {
        for (vid_t dst : parsed_edges_vec[i]) {
          casted_dual_csr->BatchPutEdge(i, dst, grape::EmptyType());
        }
      }
      for (const auto& [v0, v1, prop] : append_vec) {
        casted_dual_csr->BatchPutEdge(v0, v1, grape::EmptyType());
      }
    } else {
      auto casted_dual_csr = dynamic_cast<DualCsr<EDATA_T>*>(dual_csr_);
      size_t row_num = 0;
      for (size_t i = 0; i < parsed_edges_vec.size(); ++i) {
        for (vid_t dst : parsed_edges_vec[i]) {
          EDATA_T val;
          ConvertAny<EDATA_T>::to(prop_values[row_num++], val);
          casted_dual_csr->BatchPutEdge(i, dst, val);
        }
      }
      for (const auto& [v0, v1, prop] : append_vec) {
        auto val = table->get_column_by_id(0)->get(prop);
        EDATA_T typed_val;
        ConvertAny<EDATA_T>::to(val, typed_val);
        casted_dual_csr->BatchPutEdge(v0, v1, typed_val);
      }
    }
  }

  std::string src_label_name_, dst_label_name_, edge_label_name_;
  EdgeStrategy oe_strategy_, ie_strategy_;
  bool oe_mutable_, ie_mutable_;
  DualCsrBase* dual_csr_;
  std::unique_ptr<Table> table_;
  std::unique_ptr<std::atomic<size_t>> offset_;
  std::vector<std::string> prop_names_;
  std::vector<PropertyType> prop_types_;
  int memory_level_;
  std::string work_dir_;
};
}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_EDGE_TABLE_H_