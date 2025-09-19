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

#include "neug/storages/graph/edge_table.h"

namespace gs {
DualCsrBase* EdgeTable::create_dual_csr(
    EdgeStrategy oes, EdgeStrategy ies,
    const std::vector<PropertyType>& properties, bool oe_mutable,
    bool ie_mutable, const std::vector<std::string>& prop_names, Table& table,
    std::atomic<size_t>& offset) {
  if (properties.empty()) {
    return new DualCsr<grape::EmptyType>(oes, ies, oe_mutable, ie_mutable);
  } else if (properties.size() == 1) {
    if (properties[0] == PropertyType::kBool) {
      return new DualCsr<bool>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kInt32) {
      return new DualCsr<int32_t>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kUInt32) {
      return new DualCsr<uint32_t>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kDate) {
      return new DualCsr<Date>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kInt64) {
      return new DualCsr<int64_t>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kUInt64) {
      return new DualCsr<uint64_t>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kDouble) {
      return new DualCsr<double>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kFloat) {
      return new DualCsr<float>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0].type_enum == impl::PropertyTypeImpl::kVarChar) {
      return new DualCsr<std::string_view>(oes, ies, table, offset, oe_mutable,
                                           ie_mutable);
    } else if (properties[0] == PropertyType::kStringView) {
      return new DualCsr<std::string_view>(oes, ies, table, offset, oe_mutable,
                                           ie_mutable);
    } else if (properties[0] == PropertyType::kDateTime) {
      return new DualCsr<DateTime>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kTimestamp) {
      return new DualCsr<TimeStamp>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kDateTime) {
      return new DualCsr<DateTime>(oes, ies, oe_mutable, ie_mutable);
    }
  } else {
    // TODO: fix me, storage strategy not set
    return new DualCsr<RecordView>(oes, ies, table, offset, oe_mutable,
                                   ie_mutable);
  }
  LOG(FATAL) << "not support edge strategy or edge data type";
  return nullptr;
}

EdgeTable::EdgeTable(const std::string& src_label_name,
                     const std::string& dst_label_name,
                     const std::string& edge_label_name,
                     EdgeStrategy oe_strategy, EdgeStrategy ie_strategy,
                     const std::vector<std::string>& prop_names,
                     const std::vector<PropertyType>& prop_types,
                     bool oe_mutable, bool ie_mutable)
    : src_label_name_(src_label_name),
      dst_label_name_(dst_label_name),
      edge_label_name_(edge_label_name),
      oe_strategy_(oe_strategy),
      ie_strategy_(ie_strategy),
      oe_mutable_(oe_mutable),
      ie_mutable_(ie_mutable),
      prop_names_(prop_names),
      prop_types_(prop_types) {
  table_ = std::make_unique<Table>();
  offset_ = std::make_unique<std::atomic<size_t>>(0);
  dual_csr_ = create_dual_csr(oe_strategy, ie_strategy, prop_types, oe_mutable,
                              ie_mutable, prop_names, *table_, *offset_);
}

EdgeTable::EdgeTable(EdgeTable&& edge_table)
    : src_label_name_(std::move(edge_table.src_label_name_)),
      dst_label_name_(std::move(edge_table.dst_label_name_)),
      edge_label_name_(std::move(edge_table.edge_label_name_)),
      oe_strategy_(edge_table.oe_strategy_),
      ie_strategy_(edge_table.ie_strategy_),
      oe_mutable_(edge_table.oe_mutable_),
      ie_mutable_(edge_table.ie_mutable_),
      dual_csr_(edge_table.dual_csr_),
      table_(std::move(edge_table.table_)),
      offset_(std::move(edge_table.offset_)),
      prop_names_(std::move(edge_table.prop_names_)),
      prop_types_(std::move(edge_table.prop_types_)),
      memory_level_(edge_table.memory_level_),
      work_dir_(edge_table.work_dir_) {
  edge_table.dual_csr_ = nullptr;
}

void EdgeTable::Open(const std::string& work_dir, int memory_level,
                     size_t src_vertex_cap, size_t dst_vertex_cap) {
  memory_level_ = memory_level;
  work_dir_ = work_dir;
  table_->set_work_dir(work_dir_);
  // Here we only set the name and work_dir of the table, the real open
  // operation is not needed before we got csr that needs at least two
  // properties
  table_->set_name(
      edata_prefix(src_label_name_, dst_label_name_, edge_label_name_));
  std::string checkpoint_dir_path = checkpoint_dir(work_dir_);
  if (memory_level_ == 0) {
    dual_csr_->Open(
        oe_prefix(src_label_name_, dst_label_name_, edge_label_name_),
        ie_prefix(src_label_name_, dst_label_name_, edge_label_name_),
        edata_prefix(src_label_name_, dst_label_name_, edge_label_name_),
        checkpoint_dir_path, work_dir, prop_names_, prop_types_);
  } else if (memory_level_ >= 2) {
    dual_csr_->OpenWithHugepages(
        oe_prefix(src_label_name_, dst_label_name_, edge_label_name_),
        ie_prefix(src_label_name_, dst_label_name_, edge_label_name_),
        edata_prefix(src_label_name_, dst_label_name_, edge_label_name_),
        checkpoint_dir_path, prop_names_, prop_types_, src_vertex_cap,
        dst_vertex_cap);
  } else {
    dual_csr_->OpenInMemory(
        oe_prefix(src_label_name_, dst_label_name_, edge_label_name_),
        ie_prefix(src_label_name_, dst_label_name_, edge_label_name_),
        edata_prefix(src_label_name_, dst_label_name_, edge_label_name_),
        work_dir, prop_names_, prop_types_, src_vertex_cap, dst_vertex_cap);
  }
}

void EdgeTable::Dump(const std::string& checkpoint_dir_path) {
  dual_csr_->Dump(
      oe_prefix(src_label_name_, dst_label_name_, edge_label_name_),
      ie_prefix(src_label_name_, dst_label_name_, edge_label_name_),
      edata_prefix(src_label_name_, dst_label_name_, edge_label_name_),
      checkpoint_dir_path);
  table_->dump(edata_prefix(src_label_name_, dst_label_name_, edge_label_name_),
               checkpoint_dir_path);
}

void EdgeTable::SortByEdgeData(timestamp_t ts) {
  dual_csr_->SortByEdgeData(ts);
}

void EdgeTable::UpdateEdge(vid_t src, vid_t dst, const Any& data,
                           timestamp_t ts, Allocator& alloc) {
  dual_csr_->UpdateEdge(src, dst, data, ts, alloc);
}

void EdgeTable::BatchDeleteVertices(bool is_src,
                                    const std::vector<vid_t>& vids) {
  dual_csr_->BatchDeleteVertices(is_src, vids);
}

void EdgeTable::BatchDeleteEdge(
    const std::vector<std::tuple<vid_t, vid_t>>& parsed_edges_vec) {
  dual_csr_->BatchDeleteEdge(parsed_edges_vec);
}

void EdgeTable::Reserve(vid_t src_vertex_num, vid_t dst_vertex_num) {
  dual_csr_->Resize(src_vertex_num, dst_vertex_num);
}

static void collectEdgesWithNoProperty(DualCsrBase* dual_csr,
                                       std::vector<std::vector<vid_t>>& edges,
                                       std::vector<int>& out_degs,
                                       std::vector<int>& in_degs) {
  auto csr = dual_csr->GetOutCsr();
  auto src_vertex_cap = dual_csr->GetOutCsr()->size();
  auto dst_vertex_cap = dual_csr->GetInCsr()->size();
  edges.clear();
  edges.resize(src_vertex_cap);
  out_degs.clear();
  out_degs.resize(src_vertex_cap, 0);
  in_degs.clear();
  in_degs.resize(dst_vertex_cap, 0);
  for (size_t i = 0; i < src_vertex_cap; ++i) {
    auto it = csr->edge_iter(i);
    while (it->is_valid()) {
      vid_t dst = it->get_neighbor();
      edges[i].push_back(dst);
      out_degs[i]++;
      in_degs[dst]++;
      it->next();
    }
  }
}

static void collectEdgesWithProperty(DualCsrBase* dual_csr,
                                     std::vector<std::vector<vid_t>>& edges,
                                     std::vector<int>& out_degs,
                                     std::vector<int>& in_degs,
                                     std::vector<Any>& prop_values) {
  auto csr = dual_csr->GetOutCsr();
  edges.clear();
  auto src_vertex_cap = dual_csr->GetOutCsr()->size();
  auto dst_vertex_cap = dual_csr->GetInCsr()->size();
  edges.resize(src_vertex_cap);
  out_degs.clear();
  out_degs.resize(src_vertex_cap, 0);
  in_degs.clear();
  in_degs.resize(dst_vertex_cap, 0);
  for (size_t i = 0; i < src_vertex_cap; ++i) {
    auto it = csr->edge_iter(i);
    while (it->is_valid()) {
      vid_t dst = it->get_neighbor();
      edges[i].push_back(dst);
      out_degs[i]++;
      in_degs[dst]++;
      prop_values.push_back(it->get_data());
      it->next();
    }
  }
}

template <typename T>
void AppendEdgesUtils(DualCsrBase* dual_csr,
                      const std::vector<std::vector<vid_t>>& edges) {
  auto casted_dual_csr = dynamic_cast<DualCsr<T>*>(dual_csr);
  if constexpr (std::is_same_v<T, RecordView> ||
                std::is_same_v<T, std::string_view>) {
    size_t row_id = 0;
    for (vid_t i = 0; i < edges.size(); ++i) {
      for (vid_t j : edges[i]) {
        casted_dual_csr->BatchPutEdge(i, j, row_id);
        ++row_id;
      }
    }
  } else {
    for (vid_t i = 0; i < edges.size(); ++i) {
      for (vid_t j : edges[i]) {
        casted_dual_csr->BatchPutEdge(i, j, T());
      }
    }
  }
}
void EdgeTable::AddProperties(const std::vector<std::string>& prop_names,
                              const std::vector<PropertyType>& prop_types) {
  CHECK(prop_names.size() == prop_types.size());
  if (prop_names.empty()) {
    return;
  }
  for (const auto& name : prop_names) {
    if (std::find(prop_names_.begin(), prop_names_.end(), name) !=
        prop_names_.end()) {
      LOG(ERROR) << "Column " << name << " already exists.";
      return;
    }
  }
  std::vector<PropertyType> old_prop_types = prop_types_;

  for (size_t i = 0; i < prop_names.size(); ++i) {
    prop_names_.push_back(prop_names[i]);
    prop_types_.push_back(prop_types[i]);
  }
  std::vector<std::vector<vid_t>> edges;
  std::vector<int> out_degs, in_degs;
  if (old_prop_types.size() == 0 && prop_names_.size() == 1) {
    collectEdgesWithNoProperty(dual_csr_, edges, out_degs, in_degs);
    // TODO: delete the files related to the old properties
    drop_and_create_dual_csr();
    if (memory_level_ == 0) {
      BatchInit(work_dir_, out_degs, in_degs, false);
    } else {
      BatchInit(work_dir_, out_degs, in_degs, true);
    }
    if (prop_types_[0].type_enum == impl::PropertyTypeImpl::kStringView ||
        prop_types_[0].type_enum == impl::PropertyTypeImpl::kVarChar) {
      AppendEdgesUtils<std::string_view>(dual_csr_, edges);
    } else if (prop_types_[0] == PropertyType::DateTime()) {
      AppendEdgesUtils<DateTime>(dual_csr_, edges);
    } else if (prop_types_[0] == PropertyType::Date()) {
      AppendEdgesUtils<Date>(dual_csr_, edges);
    } else if (prop_types_[0] == PropertyType::Timestamp()) {
      AppendEdgesUtils<TimeStamp>(dual_csr_, edges);
    } else if (prop_types_[0] == PropertyType::Double()) {
      AppendEdgesUtils<double>(dual_csr_, edges);
    } else if (prop_types_[0] == PropertyType::Int64()) {
      AppendEdgesUtils<int64_t>(dual_csr_, edges);
    } else if (prop_types_[0] == PropertyType::Int32()) {
      AppendEdgesUtils<int32_t>(dual_csr_, edges);
    } else {
      LOG(FATAL) << "Unsupported property type: " << prop_types_[0].ToString();
    }
  } else {
    // Add properties from empty edge
    if (old_prop_types.size() == 0) {
      collectEdgesWithNoProperty(dual_csr_, edges, out_degs, in_degs);
      drop_and_create_dual_csr();
      if (memory_level_ == 0) {
        BatchInit(work_dir_, out_degs, in_degs, false);
      } else {
        BatchInit(work_dir_, out_degs, in_degs, true);
      }
      AppendEdgesUtils<RecordView>(dual_csr_, edges);
      table_->add_columns(prop_names, prop_types, memory_level_);
      table_->resize(EdgeNum());
    } else if (old_prop_types.size() == 1) {
      if ((old_prop_types[0].type_enum !=
           impl::PropertyTypeImpl::kStringView) &&
          (old_prop_types[0].type_enum != impl::PropertyTypeImpl::kVarChar)) {
        table_->add_columns(prop_names_, prop_types_, memory_level_);
        table_->resize(EdgeNum());
        std::vector<Any> prop_values;
        collectEdgesWithProperty(dual_csr_, edges, out_degs, in_degs,
                                 prop_values);
        auto col = table_->get_column_by_id(0);
        for (size_t i = 0; i < prop_values.size(); ++i) {
          col->set_any(i, prop_values[i]);
        }
        drop_and_create_dual_csr();
        if (memory_level_ == 0) {
          BatchInit(work_dir_, out_degs, in_degs, false);
        } else {
          BatchInit(work_dir_, out_degs, in_degs, true);
        }
        AppendEdgesUtils<RecordView>(dual_csr_, edges);
      } else {
        collectEdgesWithNoProperty(dual_csr_, edges, out_degs, in_degs);
        drop_and_create_dual_csr();
        table_->add_columns(prop_names, prop_types, memory_level_);
        if (memory_level_ == 0) {
          BatchInit(work_dir_, out_degs, in_degs, false);
        } else {
          BatchInit(work_dir_, out_degs, in_degs, true);
        }
        AppendEdgesUtils<RecordView>(dual_csr_, edges);
      }
    } else {
      table_->add_columns(prop_names, prop_types, memory_level_);
    }
  }
}

void EdgeTable::BatchInit(const std::string& work_dir,
                          const std::vector<int>& oe_degree,
                          const std::vector<int>& ie_degree, bool in_memory) {
  if (!in_memory) {
    dual_csr_->BatchInit(
        oe_prefix(src_label_name_, dst_label_name_, edge_label_name_),
        ie_prefix(src_label_name_, dst_label_name_, edge_label_name_),
        edata_prefix(src_label_name_, dst_label_name_, edge_label_name_),
        work_dir, oe_degree, ie_degree);
  } else {
    dual_csr_->BatchInitInMemory(
        edata_prefix(src_label_name_, dst_label_name_, edge_label_name_),
        work_dir, oe_degree, ie_degree);
  }
}

void EdgeTable::IngestEdge(vid_t src, vid_t dst, grape::OutArchive& oarc,
                           timestamp_t ts, Allocator& alloc) {
  dual_csr_->IngestEdge(src, dst, oarc, ts, alloc);
}

void EdgeTable::RenameProperties(const std::vector<std::string>& old_names,
                                 const std::vector<std::string>& new_names) {
  CHECK_EQ(old_names.size(), new_names.size());
  for (size_t i = 0; i < old_names.size(); ++i) {
    auto it = std::find(prop_names_.begin(), prop_names_.end(), old_names[i]);
    if (it == prop_names_.end()) {
      LOG(ERROR) << "Column " << old_names[i] << " does not exist.";
      return;
    }
    if (std::find(prop_names_.begin(), prop_names_.end(), new_names[i]) !=
        prop_names_.end()) {
      LOG(ERROR) << "Column " << new_names[i] << " already exists.";
      return;
    }
    size_t index = std::distance(prop_names_.begin(), it);
    prop_names_[index] = new_names[i];
    if (has_complex_property()) {
      table_->rename_column(old_names[i], new_names[i]);
    }
  }
}
template <typename T>
void AppendEdgesExtractPropertyFromRecordView(
    DualCsrBase* dual_csr, const std::vector<std::vector<vid_t>>& edges,
    const std::vector<Any>& prop_values, const ColumnBase& column) {
  auto casted_csr = dynamic_cast<DualCsr<T>*>(dual_csr);
  size_t row_id = 0;
  const auto& casted_column = dynamic_cast<const TypedColumn<T>&>(column);
  for (vid_t i = 0; i < edges.size(); ++i) {
    for (vid_t j : edges[i]) {
      Any val = prop_values[row_id++];
      size_t offset = val.AsRecordView().offset;
      if constexpr (std::is_same_v<T, std::string_view>) {
        casted_csr->BatchPutEdge(i, j, offset);
      } else {
        T value = casted_column.get_view(offset);
        casted_csr->BatchPutEdge(i, j, value);
      }
    }
  }
}
void EdgeTable::DeleteProperties(const std::vector<std::string>& col_names) {
  for (const auto& col : col_names) {
    auto it = std::find(prop_names_.begin(), prop_names_.end(), col);
    if (it == prop_names_.end()) {
      LOG(ERROR) << "Column " << col << " does not exist.";
      return;
    }
  }

  std::vector<PropertyType> old_prop_types = prop_types_;
  std::vector<std::string> old_prop_names = prop_names_;
  for (const auto& col : col_names) {
    auto it = std::find(prop_names_.begin(), prop_names_.end(), col);
    size_t index = std::distance(prop_names_.begin(), it);
    prop_names_.erase(it);
    prop_types_.erase(prop_types_.begin() + index);
  }
  if (prop_types_.size() == old_prop_types.size()) {
    // No properties deleted
    return;
  }
  std::vector<std::vector<vid_t>> edges;
  std::vector<int> out_degs, in_degs;
  if (old_prop_types.size() == 1 &&
      old_prop_types[0].type_enum != impl::PropertyTypeImpl::kStringView &&
      old_prop_types[0].type_enum != impl::PropertyTypeImpl::kVarChar) {
  } else {
    for (const auto& col : col_names) {
      table_->delete_column(col);
    }
  }
  if (prop_names_.empty()) {
    collectEdgesWithNoProperty(dual_csr_, edges, out_degs, in_degs);
    drop_and_create_dual_csr();
    if (memory_level_ == 0) {
      BatchInit(work_dir_, out_degs, in_degs, false);
    } else {
      BatchInit(work_dir_, out_degs, in_degs, true);
    }
    AppendEdgesUtils<grape::EmptyType>(dual_csr_, edges);

  } else if (prop_names_.size() == 1) {
    std::vector<Any> prop_values;
    collectEdgesWithProperty(dual_csr_, edges, out_degs, in_degs, prop_values);
    drop_and_create_dual_csr();
    if (memory_level_ == 0) {
      BatchInit(work_dir_, out_degs, in_degs, false);
    } else {
      BatchInit(work_dir_, out_degs, in_degs, true);
    }
    if (prop_types_[0].type_enum == impl::PropertyTypeImpl::kStringView ||
        prop_types_[0].type_enum == impl::PropertyTypeImpl::kVarChar) {
      AppendEdgesExtractPropertyFromRecordView<std::string_view>(
          dual_csr_, edges, prop_values, *table_->get_column_by_id(0));
    } else if (prop_types_[0] == PropertyType::DateTime()) {
      AppendEdgesExtractPropertyFromRecordView<DateTime>(
          dual_csr_, edges, prop_values, *table_->get_column_by_id(0));
    } else if (prop_types_[0] == PropertyType::Timestamp()) {
      AppendEdgesExtractPropertyFromRecordView<TimeStamp>(
          dual_csr_, edges, prop_values, *table_->get_column_by_id(0));
    } else if (prop_types_[0] == PropertyType::Double()) {
      AppendEdgesExtractPropertyFromRecordView<double>(
          dual_csr_, edges, prop_values, *table_->get_column_by_id(0));
    } else if (prop_types_[0] == PropertyType::Int64()) {
      AppendEdgesExtractPropertyFromRecordView<int64_t>(
          dual_csr_, edges, prop_values, *table_->get_column_by_id(0));
    } else if (prop_types_[0] == PropertyType::Int32()) {
      AppendEdgesExtractPropertyFromRecordView<int32_t>(
          dual_csr_, edges, prop_values, *table_->get_column_by_id(0));
    } else {
      LOG(FATAL) << "Unsupported property type: " << prop_types_[0].ToString();
    }
    if (prop_types_[0].type_enum == impl::PropertyTypeImpl::kStringView ||
        prop_types_[0].type_enum == impl::PropertyTypeImpl::kVarChar) {
    } else {
      table_->delete_column(prop_names_[0]);
    }
  }

  // Remove columns from the table
}

void EdgeTable::BatchAddEdges(
    std::vector<std::tuple<vid_t, vid_t, size_t>>&& edges,
    std::unique_ptr<Table>&& table, size_t src_v_cap, size_t dst_v_cap) {
  std::vector<std::vector<vid_t>> parsed_edges_vec;
  std::vector<int> out_degs, in_degs;
  std::vector<Any> prop_values;
  collectEdgesWithProperty(dual_csr_, parsed_edges_vec, out_degs, in_degs,
                           prop_values);
  dual_csr_->Resize(src_v_cap, dst_v_cap);
  out_degs.resize(src_v_cap, 0);
  in_degs.resize(dst_v_cap, 0);
  for (const auto& [v0, v1, prop] : edges) {
    if (v0 == std::numeric_limits<vid_t>::max() ||
        v1 == std::numeric_limits<vid_t>::max()) {
      continue;
    }
    if (v0 >= out_degs.size()) {
      THROW_INTERNAL_EXCEPTION("src vid " + std::to_string(v0) +
                               " exceeds src vid cap " +
                               std::to_string(out_degs.size()));
    }
    if (v1 >= in_degs.size()) {
      THROW_INTERNAL_EXCEPTION("dst vid " + std::to_string(v1) +
                               " exceeds dst vid cap " +
                               std::to_string(in_degs.size()));
    }
    out_degs[v0]++;
    in_degs[v1]++;
  }
  size_t prev_size = offset_->load();
  if (memory_level_ == 0) {
    BatchInit(work_dir_, out_degs, in_degs, false);
  } else {
    BatchInit(work_dir_, out_degs, in_degs, true);
  }
  if (!has_complex_property()) {
    if (prop_types_.size() == 0) {
      BatchPutEdgeUtil<grape::EmptyType>(parsed_edges_vec, prop_values,
                                         std::move(edges), std::move(table),
                                         prev_size);
    } else if (prop_types_.size() == 1) {
      if (prop_types_[0] == PropertyType::kBool) {
        BatchPutEdgeUtil<bool>(parsed_edges_vec, prop_values, std::move(edges),
                               std::move(table), prev_size);
      } else if (prop_types_[0] == PropertyType::kInt32) {
        BatchPutEdgeUtil<int32_t>(parsed_edges_vec, prop_values,
                                  std::move(edges), std::move(table),
                                  prev_size);
      } else if (prop_types_[0] == PropertyType::kUInt32) {
        BatchPutEdgeUtil<uint32_t>(parsed_edges_vec, prop_values,
                                   std::move(edges), std::move(table),
                                   prev_size);
      } else if (prop_types_[0] == PropertyType::kDate) {
        BatchPutEdgeUtil<Date>(parsed_edges_vec, prop_values, std::move(edges),
                               std::move(table), prev_size);
      } else if (prop_types_[0] == PropertyType::kInt64) {
        BatchPutEdgeUtil<int64_t>(parsed_edges_vec, prop_values,
                                  std::move(edges), std::move(table),
                                  prev_size);
      } else if (prop_types_[0] == PropertyType::kUInt64) {
        BatchPutEdgeUtil<uint64_t>(parsed_edges_vec, prop_values,
                                   std::move(edges), std::move(table),
                                   prev_size);
      } else if (prop_types_[0] == PropertyType::kDouble) {
        BatchPutEdgeUtil<double>(parsed_edges_vec, prop_values,
                                 std::move(edges), std::move(table), prev_size);
      } else if (prop_types_[0] == PropertyType::kFloat) {
        BatchPutEdgeUtil<float>(parsed_edges_vec, prop_values, std::move(edges),
                                std::move(table), prev_size);
      } else if (prop_types_[0] == PropertyType::kTimestamp) {
        BatchPutEdgeUtil<TimeStamp>(parsed_edges_vec, prop_values,
                                    std::move(edges), std::move(table),
                                    prev_size);
      } else if (prop_types_[0] == PropertyType::kDateTime) {
        BatchPutEdgeUtil<DateTime>(parsed_edges_vec, prop_values,
                                   std::move(edges), std::move(table),
                                   prev_size);
      } else {
        LOG(FATAL) << "Unsupported property type: "
                   << prop_types_[0].ToString();
      }
    } else {
      THROW_INTERNAL_EXCEPTION("Got invalid property size: " +
                               std::to_string(prop_types_.size()));
    }
  } else {
    if (prop_types_.size() == 1 &&
        (prop_types_[0].type_enum == impl::PropertyTypeImpl::kStringView ||
         prop_types_[0].type_enum == impl::PropertyTypeImpl::kVarChar)) {
      BatchPutEdgeUtil<std::string_view>(parsed_edges_vec, prop_values,
                                         std::move(edges), std::move(table),
                                         prev_size);
    } else {
      CHECK(prop_types_.size() > 1);

      BatchPutEdgeUtil<RecordView>(parsed_edges_vec, prop_values,
                                   std::move(edges), std::move(table),
                                   prev_size);
    }
  }
}

void EdgeTable::drop_and_create_dual_csr() {
  if (dual_csr_) {
    dual_csr_->Drop();
    delete dual_csr_;
    dual_csr_ = nullptr;
  }
  dual_csr_ =
      create_dual_csr(oe_strategy_, ie_strategy_, prop_types_, oe_mutable_,
                      ie_mutable_, prop_names_, *table_, *offset_);
}

void EdgeTable::Compact(bool reset_timestamp, bool compact_csr,
                        bool sort_on_compaction, float reserve_ratio,
                        timestamp_t ts) {
  if (reset_timestamp) {
    dual_csr_->ResetTimestamp();
  }
  if (compact_csr) {
    dual_csr_->CompactNbr(work_dir_, reserve_ratio);
  }
  if (sort_on_compaction) {
    dual_csr_->SortByEdgeData(ts);
  }
  // TODO(zhanglei): Support updating nbr lids which are deleted.
}

}  // namespace gs