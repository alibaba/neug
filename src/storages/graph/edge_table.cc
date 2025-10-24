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

#include <arrow/api.h>
#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/array_primitive.h>
#include <glog/logging.h>
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <ostream>
#include <string_view>
#include <utility>

#include "libgrape-lite/grape/serialization/in_archive.h"
#include "libgrape-lite/grape/types.h"

#include "neug/storages/csr/immutable_csr.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/file_names.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/arrow_utils.h"

namespace gs {

std::tuple<std::vector<vid_t>, std::vector<vid_t>, std::vector<bool>>
filterInvalidEdges(const std::vector<vid_t>& src_lid,
                   const std::vector<vid_t>& dst_lid) {
  assert(src_lid.size() == dst_lid.size());
  std::vector<vid_t> filtered_src, filtered_dst;
  std::vector<bool> valid_flags;
  filtered_src.reserve(src_lid.size());
  filtered_dst.reserve(dst_lid.size());
  valid_flags.reserve(src_lid.size());
  for (size_t i = 0; i < src_lid.size(); ++i) {
    if (src_lid[i] != std::numeric_limits<vid_t>::max() &&
        dst_lid[i] != std::numeric_limits<vid_t>::max()) {
      filtered_src.push_back(src_lid[i]);
      filtered_dst.push_back(dst_lid[i]);
      valid_flags.push_back(true);
    } else {
      valid_flags.push_back(false);
    }
  }
  return std::make_tuple(std::move(filtered_src), std::move(filtered_dst),
                         std::move(valid_flags));
}

template <typename EDATA_T>
void batch_put_edges_with_default_edata_impl(const std::vector<vid_t>& src_lid,
                                             const std::vector<vid_t>& dst_lid,
                                             CsrBase* out_csr) {
  assert(src_lid.size() == dst_lid.size());
  std::vector<EDATA_T> default_data(src_lid.size(), EDATA_T());
  dynamic_cast<TypedCsrBase<EDATA_T>*>(out_csr)->batch_put_edges(
      src_lid, dst_lid, default_data);
}

// TODO(zhanglei): Support default value for non-empty edge data type
void batch_put_edges_with_default_edata(const std::vector<vid_t>& src_lid,
                                        const std::vector<vid_t>& dst_lid,
                                        PropertyType property_type,
                                        CsrBase* out_csr) {
  assert(src_lid.size() == dst_lid.size());
  if (property_type == PropertyType::kEmpty) {
    batch_put_edges_with_default_edata_impl<grape::EmptyType>(src_lid, dst_lid,
                                                              out_csr);
  } else if (property_type == PropertyType::kInt32) {
    batch_put_edges_with_default_edata_impl<int32_t>(src_lid, dst_lid, out_csr);
  } else if (property_type == PropertyType::kUInt32) {
    batch_put_edges_with_default_edata_impl<uint32_t>(src_lid, dst_lid,
                                                      out_csr);
  } else if (property_type == PropertyType::kInt64) {
    batch_put_edges_with_default_edata_impl<int64_t>(src_lid, dst_lid, out_csr);
  } else if (property_type == PropertyType::kUInt64) {
    batch_put_edges_with_default_edata_impl<uint64_t>(src_lid, dst_lid,
                                                      out_csr);
  } else if (property_type == PropertyType::kFloat) {
    batch_put_edges_with_default_edata_impl<float>(src_lid, dst_lid, out_csr);
  } else if (property_type == PropertyType::kDouble) {
    batch_put_edges_with_default_edata_impl<double>(src_lid, dst_lid, out_csr);
  } else if (property_type == PropertyType::kTimestamp) {
    batch_put_edges_with_default_edata_impl<TimeStamp>(src_lid, dst_lid,
                                                       out_csr);
  } else if (property_type == PropertyType::kDate) {
    batch_put_edges_with_default_edata_impl<Date>(src_lid, dst_lid, out_csr);
  } else if (property_type == PropertyType::kDateTime) {
    batch_put_edges_with_default_edata_impl<DateTime>(src_lid, dst_lid,
                                                      out_csr);
  } else if (property_type == PropertyType::kInterval) {
    batch_put_edges_with_default_edata_impl<Interval>(src_lid, dst_lid,
                                                      out_csr);
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("not support edge data type: " +
                                  property_type.ToString());
  }
}

EdgeTableMeta::EdgeTableMeta(const std::string& src_label_name_,
                             const std::string& dst_label_name_,
                             const std::string& edge_label_name_,
                             bool ie_mutable_, bool oe_mutable_,
                             EdgeStrategy ie_strategy_,
                             EdgeStrategy oe_strategy_,
                             const std::vector<PropertyType>& properties_,
                             const std::vector<std::string>& property_names_,
                             const std::vector<StorageStrategy>& strategies_)
    : src_label_name(src_label_name_),
      dst_label_name(dst_label_name_),
      edge_label_name(edge_label_name_),
      ie_mutable(ie_mutable_),
      oe_mutable(oe_mutable_),
      oe_strategy(oe_strategy_),
      ie_strategy(ie_strategy_),
      properties(properties_),
      property_names(property_names_),
      strategies(strategies_) {
  CHECK_EQ(properties.size(), property_names.size());
  CHECK_EQ(properties.size(), strategies.size());
}

bool EdgeTableMeta::is_bundled() const {
  if (properties.empty()) {
    return true;
  } else if (properties.size() == 1 &&
             properties[0].type_enum == impl::PropertyTypeImpl::kStringView) {
    return false;
  } else if (properties.size() > 1) {
    return false;
  } else {
    return true;
  }
}

template <typename T>
std::unique_ptr<CsrBase> create_csr_impl(bool is_mutable,
                                         EdgeStrategy strategy) {
  if (strategy == EdgeStrategy::kSingle) {
    if (is_mutable) {
      return std::unique_ptr<CsrBase>(new SingleMutableCsr<T>());
    } else {
      return std::unique_ptr<CsrBase>(new SingleImmutableCsr<T>());
    }
  } else if (strategy == EdgeStrategy::kMultiple) {
    if (is_mutable) {
      return std::unique_ptr<CsrBase>(new MutableCsr<T>());
    } else {
      return std::unique_ptr<CsrBase>(new ImmutableCsr<T>());
    }
  } else {
    return std::unique_ptr<CsrBase>(new EmptyCsr<T>());
  }
}

static std::unique_ptr<CsrBase> create_csr(bool is_mutable,
                                           EdgeStrategy strategy,
                                           PropertyType property_type) {
  if (property_type == PropertyType::kEmpty) {
    return create_csr_impl<grape::EmptyType>(is_mutable, strategy);
  } else if (property_type == PropertyType::kInt32) {
    return create_csr_impl<int32_t>(is_mutable, strategy);
  } else if (property_type == PropertyType::kUInt32) {
    return create_csr_impl<uint32_t>(is_mutable, strategy);
  } else if (property_type == PropertyType::kInt64) {
    return create_csr_impl<int64_t>(is_mutable, strategy);
  } else if (property_type == PropertyType::kUInt64) {
    return create_csr_impl<uint64_t>(is_mutable, strategy);
  } else if (property_type == PropertyType::kFloat) {
    return create_csr_impl<float>(is_mutable, strategy);
  } else if (property_type == PropertyType::kDouble) {
    return create_csr_impl<double>(is_mutable, strategy);
  } else if (property_type == PropertyType::kTimestamp) {
    return create_csr_impl<TimeStamp>(is_mutable, strategy);
  } else if (property_type == PropertyType::kDate) {
    return create_csr_impl<Date>(is_mutable, strategy);
  } else if (property_type == PropertyType::kDateTime) {
    return create_csr_impl<DateTime>(is_mutable, strategy);
  } else if (property_type == PropertyType::kInterval) {
    return create_csr_impl<Interval>(is_mutable, strategy);
  } else {
    LOG(FATAL) << "not support edge data type";
    return nullptr;
  }
}

EdgeTable::EdgeTable(const EdgeTableMeta& meta) : meta_(meta) {
  table_ = std::make_unique<Table>();

  if (meta_.is_bundled()) {
    auto property_type =
        meta_.properties.empty() ? PropertyType::kEmpty : meta_.properties[0];
    out_csr_ = create_csr(meta_.oe_mutable, meta_.oe_strategy, property_type);
    in_csr_ = create_csr(meta_.ie_mutable, meta_.ie_strategy, property_type);
  } else {
    out_csr_ =
        create_csr(meta_.oe_mutable, meta_.oe_strategy, PropertyType::kUInt64);
    in_csr_ =
        create_csr(meta_.ie_mutable, meta_.ie_strategy, PropertyType::kUInt64);
  }
}

EdgeTable::EdgeTable(EdgeTable&& edge_table)
    : meta_(edge_table.meta_),
      work_dir_(edge_table.work_dir_),
      memory_level_(edge_table.memory_level_) {
  csr_alter_version_ = edge_table.csr_alter_version_.load();
  out_csr_ = std::move(edge_table.out_csr_);
  in_csr_ = std::move(edge_table.in_csr_);
  table_ = std::move(edge_table.table_);
  table_idx_ = edge_table.table_idx_.load();
}

void EdgeTable::Open(const std::string& work_dir) {
  work_dir_ = work_dir;
  memory_level_ = 0;
  auto checkpoint_dir_path = checkpoint_dir(work_dir);
  in_csr_->open(ie_prefix(meta_.src_label_name, meta_.dst_label_name,
                          meta_.edge_label_name),
                checkpoint_dir_path, work_dir);
  out_csr_->open(oe_prefix(meta_.src_label_name, meta_.dst_label_name,
                           meta_.edge_label_name),
                 checkpoint_dir_path, work_dir);
  if (!meta_.is_bundled()) {
    table_->open(edata_prefix(meta_.src_label_name, meta_.dst_label_name,
                              meta_.edge_label_name),
                 work_dir, meta_.property_names, meta_.properties,
                 meta_.strategies);
    table_idx_.store(table_->row_num());
    table_->resize(std::max(table_->row_num() + (table_->row_num() + 4) / 5,
                            static_cast<size_t>(4096)));
  }
}

void EdgeTable::OpenInMemory(const std::string& work_dir, size_t src_v_cap,
                             size_t dst_v_cap) {
  work_dir_ = work_dir;
  memory_level_ = 1;
  auto checkpoint_dir_path = checkpoint_dir(work_dir);
  in_csr_->open_in_memory(
      checkpoint_dir_path + "/" +
          ie_prefix(meta_.src_label_name, meta_.dst_label_name,
                    meta_.edge_label_name),
      dst_v_cap);
  out_csr_->open_in_memory(
      checkpoint_dir_path + "/" +
          oe_prefix(meta_.src_label_name, meta_.dst_label_name,
                    meta_.edge_label_name),
      src_v_cap);
  if (!meta_.is_bundled()) {
    table_->open_in_memory(
        edata_prefix(meta_.src_label_name, meta_.dst_label_name,
                     meta_.edge_label_name),
        work_dir_, meta_.property_names, meta_.properties, meta_.strategies);
    table_idx_.store(table_->row_num());
    table_->resize(
        std::max(table_->row_num() + (table_->row_num() + 4) / 5, 4096ul));
  }
}

void EdgeTable::OpenWithHugepages(const std::string& work_dir, size_t src_v_cap,
                                  size_t dst_v_cap) {
  work_dir_ = work_dir;
  memory_level_ = 2;  // 2 or 3?
  auto checkpoint_dir_path = checkpoint_dir(work_dir);
  in_csr_->open_with_hugepages(
      checkpoint_dir_path + "/" +
          ie_prefix(meta_.src_label_name, meta_.dst_label_name,
                    meta_.edge_label_name),
      dst_v_cap);
  out_csr_->open_with_hugepages(
      checkpoint_dir_path + "/" +
          oe_prefix(meta_.src_label_name, meta_.dst_label_name,
                    meta_.edge_label_name),
      src_v_cap);
  if (!meta_.is_bundled()) {
    table_->open_in_memory(
        edata_prefix(meta_.src_label_name, meta_.dst_label_name,
                     meta_.edge_label_name),
        checkpoint_dir_path, meta_.property_names, meta_.properties,
        meta_.strategies);
    table_idx_.store(table_->row_num());
    table_->resize(
        std::max(table_->row_num() + (table_->row_num() + 4) / 5, 4096ul));
  }
}

void EdgeTable::Dump(const std::string& checkpoint_dir_path) {
  in_csr_->dump(ie_prefix(meta_.src_label_name, meta_.dst_label_name,
                          meta_.edge_label_name),
                checkpoint_dir_path);
  out_csr_->dump(oe_prefix(meta_.src_label_name, meta_.dst_label_name,
                           meta_.edge_label_name),
                 checkpoint_dir_path);
  if (!meta_.is_bundled()) {
    table_->dump(edata_prefix(meta_.src_label_name, meta_.dst_label_name,
                              meta_.edge_label_name),
                 checkpoint_dir_path);
  }
}

void EdgeTable::IngestEdge(vid_t src, vid_t dst, grape::OutArchive& oarc,
                           timestamp_t ts, Allocator& alloc) {
  Prop prop;
  if (!meta_.is_bundled()) {
    size_t row_id = table_idx_.fetch_add(1);
    table_->ingest(row_id, oarc);
    prop.set_uint64(row_id);
    out_csr_->put_generic_edge(src, dst, prop, ts, alloc);
    in_csr_->put_generic_edge(dst, src, prop, ts, alloc);
  } else {
    if (meta_.properties.empty() ||
        meta_.properties[0] == PropertyType::kEmpty) {
      // Expect a zero
      uint32_t num_cols;
      oarc >> num_cols;
      assert(num_cols == 0);
      prop = Prop::empty();
      out_csr_->put_generic_edge(src, dst, prop, ts, alloc);
      in_csr_->put_generic_edge(dst, src, prop, ts, alloc);
    } else {
      uint32_t num_cols;
      oarc >> num_cols;
      assert(num_cols == 1);
      for (uint32_t i = 0; i < num_cols; ++i) {
        int32_t col_id;
        oarc >> col_id;
        if (col_id != 0) {
          LOG(ERROR) << "Bundled edge table only support one property column";
        }
        deserialize_property(oarc, meta_.properties[0], prop);
        out_csr_->put_generic_edge(src, dst, prop, ts, alloc);
        in_csr_->put_generic_edge(dst, src, prop, ts, alloc);
      }
    }
  }
}

void EdgeTable::SortByEdgeData(timestamp_t ts) {
  // TODO
}

void EdgeTable::BatchDeleteVertices(const std::set<vid_t>& src_set,
                                    const std::set<vid_t>& dst_set) {
  out_csr_->batch_delete_vertices(src_set, dst_set);
  in_csr_->batch_delete_vertices(dst_set, src_set);
}

void EdgeTable::BatchDeleteEdges(const std::vector<vid_t>& src_list,
                                 const std::vector<vid_t>& dst_list) {
  out_csr_->batch_delete_edges(src_list, dst_list);
  in_csr_->batch_delete_edges(dst_list, src_list);
}

void EdgeTable::Resize(vid_t src_vertex_num, vid_t dst_vertex_num) {
  out_csr_->resize(src_vertex_num);
  in_csr_->resize(dst_vertex_num);
}

size_t EdgeTable::EdgeNum() const {
  if (out_csr_) {
    return out_csr_->edge_num();
  } else if (in_csr_) {
    return in_csr_->edge_num();
  } else {
    return 0;
  }
}

GenericView EdgeTable::get_outgoing_view(timestamp_t ts) const {
  return out_csr_->get_generic_view(ts);
}

GenericView EdgeTable::get_incoming_view(timestamp_t ts) const {
  return in_csr_->get_generic_view(ts);
}

EdgeDataAccessor EdgeTable::get_edge_data_accessor(int col_id) const {
  if (!meta_.is_bundled()) {
    return EdgeDataAccessor(meta_.properties[col_id],
                            table_->get_column_by_id(col_id).get());
  } else {
    if (meta_.properties.empty()) {
      return EdgeDataAccessor(PropertyType::kEmpty, nullptr);
    } else {
      return EdgeDataAccessor(meta_.properties[0], nullptr);
    }
  }
}

void EdgeTable::AddProperties(const std::vector<std::string>& prop_names,
                              const std::vector<PropertyType>& prop_types) {
  if (prop_names.empty()) {
    return;
  }
  for (const auto& name : prop_names) {
    if (std::find(meta_.property_names.begin(), meta_.property_names.end(),
                  name) != meta_.property_names.end()) {
      LOG(ERROR) << "Column " << name << " already exists.";
      return;
    }
  }
  if (meta_.is_bundled()) {
    if (meta_.properties.empty()) {
      meta_.property_names.clear();
      meta_.properties.clear();
      meta_.strategies.clear();
      meta_.property_names.insert(meta_.property_names.end(),
                                  prop_names.begin(), prop_names.end());
      for (auto pt : prop_types) {
        meta_.properties.push_back(pt);
        meta_.strategies.push_back(StorageStrategy::kMem);
      }
      dropAndCreateNewBundledCSR();
    } else {
      dropAndCreateNewUnbundledCSR(false);
      table_->add_columns(prop_names, prop_types, memory_level_);
      meta_.property_names.insert(meta_.property_names.end(),
                                  prop_names.begin(), prop_names.end());
      for (auto pt : prop_types) {
        meta_.properties.push_back(pt);
        meta_.strategies.push_back(StorageStrategy::kMem);
      }
    }
  } else {
    table_->add_columns(prop_names, prop_types, memory_level_);
    meta_.property_names.insert(meta_.property_names.end(), prop_names.begin(),
                                prop_names.end());
    for (auto pt : prop_types) {
      meta_.properties.push_back(pt);
      meta_.strategies.push_back(StorageStrategy::kMem);
    }
  }
}
void EdgeTable::RenameProperties(const std::vector<std::string>& old_names,
                                 const std::vector<std::string>& new_names) {
  CHECK_EQ(old_names.size(), new_names.size());
  for (size_t i = 0; i < old_names.size(); ++i) {
    auto it = std::find(meta_.property_names.begin(),
                        meta_.property_names.end(), old_names[i]);
    if (it == meta_.property_names.end()) {
      LOG(ERROR) << "Column " << old_names[i] << " does not exist.";
      return;
    }
    if (std::find(meta_.property_names.begin(), meta_.property_names.end(),
                  new_names[i]) != meta_.property_names.end()) {
      LOG(ERROR) << "Column " << new_names[i] << " already exists.";
      return;
    }
    size_t index = std::distance(meta_.property_names.begin(), it);
    meta_.property_names[index] = new_names[i];
    if (!meta_.is_bundled()) {
      table_->rename_column(old_names[i], new_names[i]);
    }
  }
}
void EdgeTable::DeleteProperties(const std::vector<std::string>& col_names) {
  if (meta_.is_bundled()) {
    bool found = false;
    for (auto col : col_names) {
      if (col == meta_.property_names[0]) {
        found = true;
        break;
      }
    }
    if (found) {
      dropAndCreateNewUnbundledCSR(true);
      meta_.property_names.clear();
      meta_.properties.clear();
      meta_.strategies.clear();
    }
  } else {
    for (const auto& col : col_names) {
      auto it = std::find(meta_.property_names.begin(),
                          meta_.property_names.end(), col);
      if (it != meta_.property_names.end()) {
        meta_.property_names.erase(it);
        auto idx = std::distance(meta_.property_names.begin(), it);
        meta_.properties.erase(meta_.properties.begin() + idx);
        meta_.strategies.erase(meta_.strategies.begin() + idx);
        table_->delete_column(col);
        VLOG(1) << "delete column " << col;
      }
    }
  }
}

static void parse_endpoint_column(const IndexerType& indexer,
                                  const std::shared_ptr<arrow::Array>& array,
                                  std::vector<vid_t>& lids) {
  if (array->type()->Equals(arrow::utf8())) {
    auto casted = std::static_pointer_cast<arrow::StringArray>(array);
    for (int64_t i = 0; i < casted->length(); ++i) {
      auto str = casted->GetView(i);
      std::string_view sv(str.data(), str.size());
      auto vid = indexer.get_index(Prop::From(sv));
      lids.push_back(vid);
    }
  } else if (array->type()->Equals(arrow::large_utf8())) {
    auto casted = std::static_pointer_cast<arrow::LargeStringArray>(array);
    for (int64_t i = 0; i < casted->length(); ++i) {
      auto str = casted->GetView(i);
      std::string_view sv(str.data(), str.size());
      auto vid = indexer.get_index(Prop::From(sv));
      lids.push_back(vid);
    }
  } else if (array->type()->Equals(arrow::int64())) {
    auto casted = std::static_pointer_cast<arrow::Int64Array>(array);
    for (int64_t i = 0; i < casted->length(); ++i) {
      auto vid = indexer.get_index(Prop::From(casted->Value(i)));
      lids.push_back(vid);
    }
  } else if (array->type()->Equals(arrow::uint64())) {
    auto casted = std::static_pointer_cast<arrow::UInt64Array>(array);
    for (int64_t i = 0; i < casted->length(); ++i) {
      auto vid = indexer.get_index(Prop::From(casted->Value(i)));
      lids.push_back(vid);
    }
  } else if (array->type()->Equals(arrow::int32())) {
    auto casted = std::static_pointer_cast<arrow::Int32Array>(array);
    for (int64_t i = 0; i < casted->length(); ++i) {
      auto vid = indexer.get_index(Prop::From(casted->Value(i)));
      lids.push_back(vid);
    }
  } else if (array->type()->Equals(arrow::uint32())) {
    auto casted = std::static_pointer_cast<arrow::UInt32Array>(array);
    for (int64_t i = 0; i < casted->length(); ++i) {
      auto vid = indexer.get_index(Prop::From(casted->Value(i)));
      lids.push_back(vid);
    }
  } else {
    LOG(FATAL) << "not support type " << array->type()->ToString();
  }
}

void insert_edges_empty_impl(TypedCsrBase<grape::EmptyType>* out_csr,
                             TypedCsrBase<grape::EmptyType>* in_csr,
                             const std::vector<vid_t>& src_lid,
                             const std::vector<vid_t>& dst_lid) {
  std::vector<grape::EmptyType> empty_data(src_lid.size());
  out_csr->batch_put_edges(src_lid, dst_lid, empty_data);
  in_csr->batch_put_edges(dst_lid, src_lid, empty_data);
}

template <typename EDATA_T, typename ARROW_COL_T>
std::vector<EDATA_T> extract_edge_data(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& data_batches,
    const std::vector<bool>& valid_flags) {
  std::vector<EDATA_T> edge_data;
  assert([&]() {
    int64_t total = 0;
    for (auto rb : data_batches) {
      total += rb->num_rows();
    }
    return total == static_cast<int64_t>(valid_flags.size());
  }());
  edge_data.reserve(std::count(valid_flags.begin(), valid_flags.end(), true));
  size_t cur_index = 0;
  for (auto rb : data_batches) {
    auto array = rb->column(0);
    auto casted = std::static_pointer_cast<ARROW_COL_T>(array);
    for (int64_t i = 0; i < casted->length(); ++i) {
      if (valid_flags[cur_index++]) {
        edge_data.emplace_back(casted->GetView(i));
      }
    }
  }
  return edge_data;
}

template <typename EDATA_T>
void insert_edges_bundled_typed_impl(TypedCsrBase<EDATA_T>* out_csr,
                                     TypedCsrBase<EDATA_T>* in_csr,
                                     const std::vector<vid_t>& src_lid,
                                     const std::vector<vid_t>& dst_lid,
                                     const std::vector<EDATA_T>& edge_data) {
  out_csr->batch_put_edges(src_lid, dst_lid, edge_data);
  in_csr->batch_put_edges(dst_lid, src_lid, edge_data);
}

void insert_edges_separated_impl(TypedCsrBase<uint64_t>* out_csr,
                                 TypedCsrBase<uint64_t>* in_csr,
                                 const std::vector<vid_t>& src_lid,
                                 const std::vector<vid_t>& dst_lid,
                                 size_t offset) {
  std::vector<uint64_t> edge_data(src_lid.size());
  for (size_t i = 0; i < src_lid.size(); ++i) {
    edge_data[i] = offset + i;
  }
  out_csr->batch_put_edges(src_lid, dst_lid, edge_data);
  in_csr->batch_put_edges(dst_lid, src_lid, edge_data);
}

static std::vector<Prop> get_row_from_recordbatch(
    const std::vector<PropertyType>& prop_types,
    const std::vector<std::shared_ptr<arrow::DataType>>& expected_types,
    const std::shared_ptr<arrow::RecordBatch>& rb, int64_t row_idx) {
  std::vector<Prop> row;
  if ((int32_t) expected_types.size() != rb->num_columns()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "property types size not match recordbatch column size");
  }
  for (int i = 0; i < rb->num_columns(); ++i) {
    auto array = rb->column(i);
    if (!array->type()->Equals(expected_types[i])) {
      // Except for large string and string
      if ((expected_types[i]->Equals(arrow::utf8()) &&
           array->type()->Equals(arrow::large_utf8())) ||
          (expected_types[i]->Equals(arrow::large_utf8()) &&
           array->type()->Equals(arrow::utf8()))) {
        // pass
      } else {
        THROW_INVALID_ARGUMENT_EXCEPTION(
            std::string("property type not match recordbatch column type: ") +
            prop_types[i].ToString() + "(" + expected_types[i]->ToString() +
            ") vs " + array->type()->ToString());
      }
    }
    if (array->IsNull(row_idx)) {
      row.push_back(Prop());
      continue;
    } else if (prop_types[i] == PropertyType::kInt32) {
      auto casted = std::static_pointer_cast<arrow::Int32Array>(array);
      row.push_back(Prop::from_int32(casted->Value(row_idx)));
    } else if (prop_types[i] == PropertyType::kInt64) {
      auto casted = std::static_pointer_cast<arrow::Int64Array>(array);
      row.push_back(Prop::from_int64(casted->Value(row_idx)));
    } else if (prop_types[i] == PropertyType::kUInt32) {
      auto casted = std::static_pointer_cast<arrow::UInt32Array>(array);
      row.push_back(Prop::from_uint32(casted->Value(row_idx)));
    } else if (prop_types[i] == PropertyType::kUInt64) {
      auto casted = std::static_pointer_cast<arrow::UInt64Array>(array);
      row.push_back(Prop::from_uint64(casted->Value(row_idx)));
    } else if (prop_types[i] == PropertyType::kFloat) {
      auto casted = std::static_pointer_cast<arrow::FloatArray>(array);
      row.push_back(Prop::from_float(casted->Value(row_idx)));
    } else if (prop_types[i] == PropertyType::kDouble) {
      auto casted = std::static_pointer_cast<arrow::DoubleArray>(array);
      row.push_back(Prop::from_double(casted->Value(row_idx)));
    } else if (prop_types[i].type_enum == impl::PropertyTypeImpl::kStringView) {
      if (array->type()->Equals(arrow::utf8())) {
        auto casted = std::static_pointer_cast<arrow::StringArray>(array);
        auto str = casted->GetView(row_idx);
        row.push_back(Prop::from_string(std::string(str.data(), str.size())));
      } else if (array->type()->Equals(arrow::large_utf8())) {
        auto casted = std::static_pointer_cast<arrow::LargeStringArray>(array);
        auto str = casted->GetView(row_idx);
        row.push_back(Prop::from_string(std::string(str.data(), str.size())));
      } else {
        LOG(FATAL) << "not support type " << array->type()->ToString();
      }
    } else if (prop_types[i] == PropertyType::kDate) {
      auto casted = std::static_pointer_cast<arrow::Date32Array>(array);
      Date d;
      d.from_num_days(casted->Value(row_idx));
      row.push_back(Prop::from_date(d));
    } else if (prop_types[i] == PropertyType::kTimestamp) {
      auto casted = std::static_pointer_cast<arrow::TimestampArray>(array);
      row.push_back(Prop::from_timestamp(TimeStamp(casted->Value(row_idx))));
    } else if (prop_types[i] == PropertyType::kDateTime) {
      auto casted = std::static_pointer_cast<arrow::TimestampArray>(array);
      row.push_back(Prop::from_date_time(DateTime(casted->Value(row_idx))));
    } else if (prop_types[i] == PropertyType::kInterval) {
      auto casted = std::static_pointer_cast<arrow::LargeStringArray>(array);
      row.push_back(Prop::from_interval(Interval(casted->GetView(row_idx))));
    } else {
      LOG(FATAL) << "not support type " << array->type()->ToString();
    }
  }
  return row;
}

void EdgeTable::BatchAddEdges(const IndexerType& src_indexer,
                              const IndexerType& dst_indexer,
                              std::shared_ptr<IRecordBatchSupplier> supplier) {
  in_csr_->resize(dst_indexer.size());
  out_csr_->resize(src_indexer.size());
  std::vector<vid_t> src_lid, dst_lid;
  std::vector<std::shared_ptr<arrow::RecordBatch>> data_batches;
  while (true) {
    auto batch = supplier->GetNextBatch();
    if (batch == nullptr) {
      break;
    }
    auto src_array = batch->column(0);
    auto dst_array = batch->column(1);
    parse_endpoint_column(src_indexer, src_array, src_lid);
    parse_endpoint_column(dst_indexer, dst_array, dst_lid);
    if (batch->num_columns() > 2) {
      batch = batch->RemoveColumn(0).ValueOrDie()->RemoveColumn(0).ValueOrDie();
      data_batches.push_back(batch);
    }
  }
  std::vector<bool> valid_flags;  // true for valid edges
  std::tie(src_lid, dst_lid, valid_flags) =
      filterInvalidEdges(src_lid, dst_lid);

  if (meta_.is_bundled()) {
    if (meta_.properties.empty() ||
        meta_.properties[0] == PropertyType::kEmpty) {
      insert_edges_empty_impl(
          dynamic_cast<TypedCsrBase<grape::EmptyType>*>(out_csr_.get()),
          dynamic_cast<TypedCsrBase<grape::EmptyType>*>(in_csr_.get()), src_lid,
          dst_lid);
    } else if (meta_.properties[0] == PropertyType::kInt32) {
      auto edge_data = extract_edge_data<int32_t, arrow::Int32Array>(
          data_batches, valid_flags);
      insert_edges_bundled_typed_impl(
          dynamic_cast<TypedCsrBase<int32_t>*>(out_csr_.get()),
          dynamic_cast<TypedCsrBase<int32_t>*>(in_csr_.get()), src_lid, dst_lid,
          edge_data);
    } else if (meta_.properties[0] == PropertyType::kUInt32) {
      auto edge_data = extract_edge_data<uint32_t, arrow::UInt32Array>(
          data_batches, valid_flags);
      insert_edges_bundled_typed_impl(
          dynamic_cast<TypedCsrBase<uint32_t>*>(out_csr_.get()),
          dynamic_cast<TypedCsrBase<uint32_t>*>(in_csr_.get()), src_lid,
          dst_lid, edge_data);
    } else if (meta_.properties[0] == PropertyType::kInt64) {
      auto edge_data = extract_edge_data<int64_t, arrow::Int64Array>(
          data_batches, valid_flags);
      insert_edges_bundled_typed_impl(
          dynamic_cast<TypedCsrBase<int64_t>*>(out_csr_.get()),
          dynamic_cast<TypedCsrBase<int64_t>*>(in_csr_.get()), src_lid, dst_lid,
          edge_data);
    } else if (meta_.properties[0] == PropertyType::kUInt64) {
      auto edge_data = extract_edge_data<uint64_t, arrow::UInt64Array>(
          data_batches, valid_flags);
      insert_edges_bundled_typed_impl(
          dynamic_cast<TypedCsrBase<uint64_t>*>(out_csr_.get()),
          dynamic_cast<TypedCsrBase<uint64_t>*>(in_csr_.get()), src_lid,
          dst_lid, edge_data);
    } else if (meta_.properties[0] == PropertyType::kTimestamp) {
      auto edge_data = extract_edge_data<TimeStamp, arrow::TimestampArray>(
          data_batches, valid_flags);
      insert_edges_bundled_typed_impl(
          dynamic_cast<TypedCsrBase<TimeStamp>*>(out_csr_.get()),
          dynamic_cast<TypedCsrBase<TimeStamp>*>(in_csr_.get()), src_lid,
          dst_lid, edge_data);
    } else if (meta_.properties[0] == PropertyType::kDouble) {
      auto edge_data = extract_edge_data<double, arrow::DoubleArray>(
          data_batches, valid_flags);
      insert_edges_bundled_typed_impl(
          dynamic_cast<TypedCsrBase<double>*>(out_csr_.get()),
          dynamic_cast<TypedCsrBase<double>*>(in_csr_.get()), src_lid, dst_lid,
          edge_data);
    } else if (meta_.properties[0] == PropertyType::kFloat) {
      auto edge_data = extract_edge_data<float, arrow::FloatArray>(data_batches,
                                                                   valid_flags);
      insert_edges_bundled_typed_impl(
          dynamic_cast<TypedCsrBase<float>*>(out_csr_.get()),
          dynamic_cast<TypedCsrBase<float>*>(in_csr_.get()), src_lid, dst_lid,
          edge_data);
    } else if (meta_.properties[0] == PropertyType::kDate) {
      auto edge_data = extract_edge_data<Date, arrow::Date32Array>(data_batches,
                                                                   valid_flags);
      insert_edges_bundled_typed_impl(
          dynamic_cast<TypedCsrBase<Date>*>(out_csr_.get()),
          dynamic_cast<TypedCsrBase<Date>*>(in_csr_.get()), src_lid, dst_lid,
          edge_data);
    } else if (meta_.properties[0] == PropertyType::kDateTime) {
      auto edge_data = extract_edge_data<DateTime, arrow::TimestampArray>(
          data_batches, valid_flags);
      insert_edges_bundled_typed_impl(
          dynamic_cast<TypedCsrBase<DateTime>*>(out_csr_.get()),
          dynamic_cast<TypedCsrBase<DateTime>*>(in_csr_.get()), src_lid,
          dst_lid, edge_data);
    } else if (meta_.properties[0] == PropertyType::kInterval) {
      auto edge_data = extract_edge_data<Interval, arrow::LargeStringArray>(
          data_batches, valid_flags);
      insert_edges_bundled_typed_impl(
          dynamic_cast<TypedCsrBase<Interval>*>(out_csr_.get()),
          dynamic_cast<TypedCsrBase<Interval>*>(in_csr_.get()), src_lid,
          dst_lid, edge_data);
    } else {
      LOG(FATAL) << "not support edge data type: "
                 << meta_.properties[0].ToString();
    }
  } else {
    size_t offset = table_idx_.fetch_add(src_lid.size());
    insert_edges_separated_impl(
        dynamic_cast<TypedCsrBase<uint64_t>*>(out_csr_.get()),
        dynamic_cast<TypedCsrBase<uint64_t>*>(in_csr_.get()), src_lid, dst_lid,
        offset);
    table_->resize(table_idx_.load());
    std::vector<std::shared_ptr<arrow::DataType>> expected_types;
    for (auto pt : meta_.properties) {
      expected_types.emplace_back(PropertyTypeToArrowType(pt));
    }
    // assert the totoal number of rows in data_batches equals to
    // src_lid.size()
    assert([&]() {
      int64_t total = 0;
      for (auto rb : data_batches) {
        total += rb->num_rows();
      }
      return total == static_cast<int64_t>(valid_flags.size());
    }());
    size_t cur_index = 0;
    for (auto rb : data_batches) {
      for (int64_t i = 0; i < rb->num_rows(); ++i) {
        assert(cur_index < valid_flags.size());
        if (valid_flags[cur_index++]) {
          auto row =
              get_row_from_recordbatch(meta_.properties, expected_types, rb, i);
          table_->insert(offset++, row);
        }
      }
    }
  }
}

void EdgeTable::Compact(bool reset_timestamp, bool compact_csr,
                        bool sort_on_compaction, float reserve_ratio,
                        timestamp_t ts) {
  if (sort_on_compaction) {
    out_csr_->batch_sort_by_edge_data(ts);
    in_csr_->batch_sort_by_edge_data(ts);
  }
  if (reset_timestamp) {
    out_csr_->reset_timestamp();
    in_csr_->reset_timestamp();
  }
  // TODO(zhanglei): Resize the CSR with reserve_ratio
}

void EdgeTable::dropAndCreateNewBundledCSR() {
  auto suffix = get_next_csr_path_suffix();
  std::string next_oe_csr_path =
      tmp_dir(work_dir_) + "/" +
      oe_prefix(meta_.src_label_name, meta_.dst_label_name,
                meta_.edge_label_name) +
      suffix;
  std::string next_ie_csr_path =
      tmp_dir(work_dir_) + "/" +
      ie_prefix(meta_.src_label_name, meta_.dst_label_name,
                meta_.edge_label_name) +
      suffix;

  auto edges = out_csr_->batch_export(nullptr);
  std::unique_ptr<CsrBase> new_out_csr, new_in_csr;
  assert(meta_.properties.size() == 1);
  new_out_csr =
      create_csr(meta_.oe_mutable, meta_.oe_strategy, meta_.properties[0]);
  new_in_csr =
      create_csr(meta_.ie_mutable, meta_.ie_strategy, meta_.properties[0]);

  new_out_csr->open_in_memory(next_oe_csr_path, out_csr_->size());
  new_in_csr->open_in_memory(next_ie_csr_path, in_csr_->size());
  new_out_csr->resize(out_csr_->size());
  new_in_csr->resize(in_csr_->size());

  batch_put_edges_with_default_edata(std::get<0>(edges), std::get<1>(edges),
                                     meta_.properties[0], new_out_csr.get());
  batch_put_edges_with_default_edata(std::get<1>(edges), std::get<0>(edges),
                                     meta_.properties[0], new_in_csr.get());

  out_csr_->close();
  in_csr_->close();
  out_csr_ = std::move(new_out_csr);
  in_csr_ = std::move(new_in_csr);
}

void EdgeTable::dropAndCreateNewUnbundledCSR(bool delete_property) {
  auto suffix = get_next_csr_path_suffix();
  std::string next_oe_csr_path =
      tmp_dir(work_dir_) + "/" +
      oe_prefix(meta_.src_label_name, meta_.dst_label_name,
                meta_.edge_label_name) +
      suffix;
  std::string next_ie_csr_path =
      tmp_dir(work_dir_) + "/" +
      ie_prefix(meta_.src_label_name, meta_.dst_label_name,
                meta_.edge_label_name) +
      suffix;
  std::string next_table_prefix = edata_prefix(
      meta_.src_label_name, meta_.dst_label_name, meta_.edge_label_name);
  // In this method, the edge table must be bundled, so the table must be
  // opened opened. In open_in_memory method, table will try to read the
  // existing table file from checkpoint_dir, but it must not exist.
  if (!delete_property) {
    table_->open_in_memory(next_table_prefix, work_dir_, meta_.property_names,
                           meta_.properties, meta_.strategies);
  }

  std::shared_ptr<ColumnBase> prev_data_col = nullptr;

  if (!delete_property) {
    if (!meta_.properties.empty()) {
      prev_data_col = table_->get_column_by_id(0);
    }
  }

  auto edges = out_csr_->batch_export(prev_data_col);
  std::vector<uint64_t> row_ids;
  for (size_t i = 0; i < std::get<0>(edges).size(); ++i) {
    row_ids.push_back(i);
  }
  std::unique_ptr<CsrBase> new_out_csr, new_in_csr;
  if (delete_property) {
    new_out_csr =
        create_csr(meta_.oe_mutable, meta_.oe_strategy, PropertyType::kEmpty);
    new_in_csr =
        create_csr(meta_.ie_mutable, meta_.ie_strategy, PropertyType::kEmpty);
  } else {
    new_out_csr =
        create_csr(meta_.oe_mutable, meta_.oe_strategy, PropertyType::kUInt64);
    new_in_csr =
        create_csr(meta_.ie_mutable, meta_.ie_strategy, PropertyType::kUInt64);
  }

  new_out_csr->open_in_memory(next_oe_csr_path, out_csr_->size());
  new_in_csr->open_in_memory(next_ie_csr_path, in_csr_->size());
  new_out_csr->resize(out_csr_->size());
  new_in_csr->resize(in_csr_->size());
  if (delete_property) {
    dynamic_cast<TypedCsrBase<grape::EmptyType>*>(new_out_csr.get())
        ->batch_put_edges(std::get<0>(edges), std::get<1>(edges), {});
    dynamic_cast<TypedCsrBase<grape::EmptyType>*>(new_in_csr.get())
        ->batch_put_edges(std::get<1>(edges), std::get<0>(edges), {});
  } else {
    dynamic_cast<TypedCsrBase<uint64_t>*>(new_out_csr.get())
        ->batch_put_edges(std::get<0>(edges), std::get<1>(edges), row_ids);
    dynamic_cast<TypedCsrBase<uint64_t>*>(new_in_csr.get())
        ->batch_put_edges(std::get<1>(edges), std::get<0>(edges), row_ids);
  }
  out_csr_->close();
  in_csr_->close();
  out_csr_ = std::move(new_out_csr);
  in_csr_ = std::move(new_in_csr);
}

std::string EdgeTable::get_next_csr_path_suffix() {
  return std::string("_v_") + std::to_string(csr_alter_version_.fetch_add(1));
}

}  // namespace gs
