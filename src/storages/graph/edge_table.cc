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

#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/module/module_broker.h"
#include "neug/storages/module/module_factory.h"

#include <glog/logging.h>
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <ostream>
#include <string_view>
#include <utility>

#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/csr/csr_view_utils.h"
#include "neug/storages/csr/immutable_csr.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/storages/module/type_name.h"
#include "neug/storages/module_descriptor.h"
#include "neug/utils/io/file/file_utils.h"
#include "neug/utils/property/types.h"

namespace neug {

void filterInvalidEdges(std::vector<vid_t>& src_lid,
                        std::vector<vid_t>& dst_lid,
                        std::vector<bool>& valid_flags) {
  assert(src_lid.size() == dst_lid.size());

  valid_flags.reserve(src_lid.size());
  size_t valid_count = 0;
  for (size_t i = 0; i < src_lid.size(); ++i) {
    if (src_lid[i] != std::numeric_limits<vid_t>::max() &&
        dst_lid[i] != std::numeric_limits<vid_t>::max()) {
      src_lid[valid_count] = src_lid[i];
      dst_lid[valid_count] = dst_lid[i];
      ++valid_count;
      valid_flags.push_back(true);
    } else {
      valid_flags.push_back(false);
    }
  }
  src_lid.resize(valid_count);
  dst_lid.resize(valid_count);
}

template <typename EDATA_T>
void batch_put_edges_with_default_edata_impl(const std::vector<vid_t>& src_lid,
                                             const std::vector<vid_t>& dst_lid,
                                             const EDATA_T& default_data,
                                             CsrBase* out_csr) {
  assert(src_lid.size() == dst_lid.size());
  std::vector<EDATA_T> default_datas(src_lid.size(), default_data);
  dynamic_cast<TypedCsrBase<EDATA_T>*>(out_csr)->batch_put_edges(
      src_lid, dst_lid, default_datas);
}

void batch_put_edges_with_default_edata(const std::vector<vid_t>& src_lid,
                                        const std::vector<vid_t>& dst_lid,
                                        DataTypeId property_type,
                                        const execution::Value& default_value,
                                        CsrBase* out_csr) {
  assert(src_lid.size() == dst_lid.size());
  switch (property_type) {
#define TYPE_DISPATCHER(enum_val, type)                             \
  case DataTypeId::enum_val:                                        \
    batch_put_edges_with_default_edata_impl<type>(                  \
        src_lid, dst_lid, default_value.GetValue<type>(), out_csr); \
    break;
    FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kEmpty:
    batch_put_edges_with_default_edata_impl<EmptyType>(src_lid, dst_lid,
                                                       EmptyType(), out_csr);
    break;
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("not support edge data type: " +
                                  std::to_string(property_type));
  }
}

void batch_put_edges_to_bundled_csr(
    const std::vector<vid_t>& src_lid, const std::vector<vid_t>& dst_lid,
    DataTypeId property_type, const std::vector<execution::Value>& edge_data,
    CsrBase* out_csr) {
  switch (property_type) {
#define TYPE_DISPATCHER(enum_val, type)                          \
  case DataTypeId::enum_val: {                                   \
    std::vector<type> typed_data;                                \
    typed_data.reserve(edge_data.size());                        \
    for (const auto& v : edge_data) {                            \
      typed_data.emplace_back(v.GetValue<type>());               \
    }                                                            \
    dynamic_cast<TypedCsrBase<type>*>(out_csr)->batch_put_edges( \
        src_lid, dst_lid, typed_data);                           \
    break;                                                       \
  }
    TYPE_DISPATCHER(kBoolean, bool);
    TYPE_DISPATCHER(kInt32, int32_t);
    TYPE_DISPATCHER(kUInt32, uint32_t);
    TYPE_DISPATCHER(kInt64, int64_t);
    TYPE_DISPATCHER(kUInt64, uint64_t);
    TYPE_DISPATCHER(kFloat, float);
    TYPE_DISPATCHER(kDouble, double);
    TYPE_DISPATCHER(kDate, Date);
    TYPE_DISPATCHER(kTimestampMs, DateTime);
    TYPE_DISPATCHER(kInterval, Interval);
#undef TYPE_DISPATCHER
  case DataTypeId::kEmpty: {
    dynamic_cast<TypedCsrBase<EmptyType>*>(out_csr)->batch_put_edges(
        src_lid, dst_lid, {});
    break;
  }
  case DataTypeId::kVarchar: {
    THROW_NOT_SUPPORTED_EXCEPTION("not support edge data type: " +
                                  std::to_string(property_type));
    break;
  }
  default:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unsupported edge property type: " +
        std::to_string(static_cast<int>(property_type)));
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
                                           DataTypeId property_type) {
  switch (property_type) {
#define TYPE_DISPATCHER(enum_val, type) \
  case DataTypeId::enum_val:            \
    return create_csr_impl<type>(is_mutable, strategy);
    FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kEmpty: {
    return create_csr_impl<EmptyType>(is_mutable, strategy);
  }
  default: {
    THROW_NOT_SUPPORTED_EXCEPTION("not support edge data type: " +
                                  std::to_string(property_type));
    return nullptr;
  }
  }
}

static void parse_endpoint_column(
    const IndexerType& indexer,
    const std::shared_ptr<execution::IContextColumn>& col,
    std::vector<vid_t>& lids) {
  for (size_t i = 0; i < col->size(); ++i) {
    auto val = col->get_elem(i);
    auto vid = indexer.get_index(val);
    lids.push_back(vid);
  }
}

void insert_edges_empty_impl(TypedCsrBase<EmptyType>* out_csr,
                             TypedCsrBase<EmptyType>* in_csr,
                             const std::vector<vid_t>& src_lid,
                             const std::vector<vid_t>& dst_lid) {
  std::vector<EmptyType> empty_data(src_lid.size());
  out_csr->batch_put_edges(src_lid, dst_lid, empty_data);
  in_csr->batch_put_edges(dst_lid, src_lid, empty_data);
}

template <typename EDATA_T>
void insert_edges_bundled_typed_impl(
    TypedCsrBase<EDATA_T>* out_csr, TypedCsrBase<EDATA_T>* in_csr,
    const std::vector<vid_t>& src_lid, const std::vector<vid_t>& dst_lid,
    const std::vector<std::shared_ptr<execution::IContextColumn>>& data_cols,
    const std::vector<bool>& valid_flags) {
  std::vector<EDATA_T> edge_data;
  edge_data.reserve(src_lid.size());
  size_t cur_index = 0;
  for (auto& col : data_cols) {
    for (size_t i = 0; i < col->size(); ++i) {
      if (valid_flags[cur_index++]) {
        auto val = col->get_elem(i);
        edge_data.push_back(val.template GetValue<EDATA_T>());
      }
    }
  }
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

static std::vector<execution::Value> get_row_from_data_chunks(
    const std::vector<std::shared_ptr<execution::IContextColumn>>& prop_cols,
    size_t row_idx) {
  std::vector<execution::Value> row;
  row.reserve(prop_cols.size());
  for (auto& col : prop_cols) {
    row.push_back(col->get_elem(row_idx));
  }
  return row;
}

void batch_add_unbundled_edges_impl(
    const std::vector<vid_t>& src_lid_list,
    const std::vector<vid_t>& dst_lid_list, TypedCsrBase<uint64_t>* out_csr,
    TypedCsrBase<uint64_t>* in_csr, Table* table_,
    std::atomic<uint64_t>& table_idx_, std::atomic<uint64_t>& capacity_,
    const std::vector<DataType>& prop_types,
    const std::vector<std::shared_ptr<execution::DataChunk>>& data_chunks,
    const std::vector<bool>& valid_flags) {
  size_t offset = table_idx_.fetch_add(src_lid_list.size());
  insert_edges_separated_impl(out_csr, in_csr, src_lid_list, dst_lid_list,
                              offset);
  size_t cur_index = 0;
  for (auto& chunk : data_chunks) {
    size_t num_rows = chunk->row_num();
    // Build per-column accessors for this chunk.
    std::vector<std::shared_ptr<execution::IContextColumn>> prop_cols;
    prop_cols.reserve(chunk->col_num());
    for (auto& c : chunk->columns) {
      if (c)
        prop_cols.push_back(c);
    }
    for (size_t i = 0; i < num_rows; ++i) {
      assert(cur_index < valid_flags.size());
      if (valid_flags[cur_index++]) {
        auto row = get_row_from_data_chunks(prop_cols, i);
        table_->insert(offset++, row, true);
      }
    }
  }
}

void batch_add_bundled_edges_impl(
    CsrBase* out_csr, CsrBase* in_csr, std::shared_ptr<const EdgeSchema> meta,
    const std::vector<vid_t>& src_lid_list,
    const std::vector<vid_t>& dst_lid_list,
    const std::vector<std::shared_ptr<execution::IContextColumn>>& data_cols,
    const std::vector<bool>& valid_flags) {
  const auto& prop_types = meta->properties;
  if (prop_types.empty() || prop_types[0].id() == DataTypeId::kEmpty) {
    insert_edges_empty_impl(dynamic_cast<TypedCsrBase<EmptyType>*>(out_csr),
                            dynamic_cast<TypedCsrBase<EmptyType>*>(in_csr),
                            src_lid_list, dst_lid_list);
    return;
  }
  switch (prop_types[0].id()) {
#define TYPE_DISPATCHER(enum_val, type)                                        \
  case DataTypeId::enum_val:                                                   \
    insert_edges_bundled_typed_impl<type>(                                     \
        dynamic_cast<TypedCsrBase<type>*>(out_csr),                            \
        dynamic_cast<TypedCsrBase<type>*>(in_csr), src_lid_list, dst_lid_list, \
        data_cols, valid_flags);                                               \
    break;
    FOR_EACH_DATA_TYPE_PRIMITIVE(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kDate:
    insert_edges_bundled_typed_impl<Date>(
        dynamic_cast<TypedCsrBase<Date>*>(out_csr),
        dynamic_cast<TypedCsrBase<Date>*>(in_csr), src_lid_list, dst_lid_list,
        data_cols, valid_flags);
    break;
  case DataTypeId::kTimestampMs:
    insert_edges_bundled_typed_impl<DateTime>(
        dynamic_cast<TypedCsrBase<DateTime>*>(out_csr),
        dynamic_cast<TypedCsrBase<DateTime>*>(in_csr), src_lid_list,
        dst_lid_list, data_cols, valid_flags);
    break;
  case DataTypeId::kInterval:
    insert_edges_bundled_typed_impl<Interval>(
        dynamic_cast<TypedCsrBase<Interval>*>(out_csr),
        dynamic_cast<TypedCsrBase<Interval>*>(in_csr), src_lid_list,
        dst_lid_list, data_cols, valid_flags);
    break;
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("not support edge data type: " +
                                  std::to_string(prop_types[0].id()));
  }
}

void EdgeTable::Init(std::shared_ptr<Checkpoint> ckp, MemoryLevel level) {
  CHECK(meta_ != nullptr) << "EdgeTable::Init requires schema";

  ckp_ = std::move(ckp);
  memory_level_ = level;
  const ModuleDescriptor empty{};
  if (meta_->is_bundled()) {
    auto property_type = meta_->properties.empty() ? DataTypeId::kEmpty
                                                   : meta_->properties[0].id();
    out_csr_ = create_csr(meta_->oe_mutable, meta_->oe_strategy, property_type);
    in_csr_ = create_csr(meta_->ie_mutable, meta_->ie_strategy, property_type);
  } else {
    out_csr_ =
        create_csr(meta_->oe_mutable, meta_->oe_strategy, DataTypeId::kUInt64);
    in_csr_ =
        create_csr(meta_->ie_mutable, meta_->ie_strategy, DataTypeId::kUInt64);
  }
  in_csr_->Open(*ckp_, empty, level);
  out_csr_->Open(*ckp_, empty, level);
  if (meta_->is_bundled()) {
    table_ = std::make_unique<Table>();
  } else {
    table_ = std::make_unique<Table>(meta_->property_names, meta_->properties);
  }
  table_->Init(*ckp_, level);
}

std::string expectedCsrType(const EdgeSchema& meta, bool is_in) {
  DataTypeId edata_type;
  if (meta.is_bundled()) {
    edata_type =
        meta.properties.empty() ? DataTypeId::kEmpty : meta.properties[0].id();
  } else {
    edata_type = DataTypeId::kUInt64;
  }
  EdgeStrategy strategy = is_in ? meta.ie_strategy : meta.oe_strategy;
  bool is_mutable = is_in ? meta.ie_mutable : meta.oe_mutable;
  return module_naming::CsrTypeName(edata_type, strategy, is_mutable);
}

void validateCsrSlot(std::shared_ptr<const EdgeSchema> meta, const CsrBase* csr,
                     bool is_in) {
  const char* fn_name = is_in ? "SetInCsr" : "SetOutCsr";
  CHECK(csr != nullptr) << "EdgeTable::" << fn_name << ": csr must not be null";
  auto expected = expectedCsrType(*meta, is_in);
  auto actual = csr->ModuleTypeName();
  if (expected != actual) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        std::string("EdgeTable::") + fn_name + ": CSR type mismatch for edge " +
        meta->src_label_name + "-[" + meta->edge_label_name + "]->" +
        meta->dst_label_name + "; expected '" + expected + "', got '" + actual +
        "'");
  }
}

void EdgeTable::SetInCsr(std::unique_ptr<CsrBase> csr) {
  validateCsrSlot(meta_, csr.get(), /*is_in=*/true);
  in_csr_ = std::move(csr);
}

void EdgeTable::SetOutCsr(std::unique_ptr<CsrBase> csr) {
  validateCsrSlot(meta_, csr.get(), /*is_in=*/false);
  out_csr_ = std::move(csr);
}

EdgeTable::EdgeTable(EdgeTable&& edge_table)
    : ckp_(std::move(edge_table.ckp_)),
      meta_(edge_table.meta_),
      memory_level_(edge_table.memory_level_) {
  out_csr_ = std::move(edge_table.out_csr_);
  in_csr_ = std::move(edge_table.in_csr_);
  table_ = std::move(edge_table.table_);
  table_idx_ = edge_table.table_idx_.load();
  capacity_ = edge_table.capacity_.load();
}

void EdgeTable::Swap(EdgeTable& edge_table) {
  std::swap(ckp_, edge_table.ckp_);
  std::swap(meta_, edge_table.meta_);
  std::swap(memory_level_, edge_table.memory_level_);
  out_csr_.swap(edge_table.out_csr_);
  in_csr_.swap(edge_table.in_csr_);
  table_.swap(edge_table.table_);
  auto t_idx = table_idx_.load();
  table_idx_.store(edge_table.table_idx_.load());
  edge_table.table_idx_.store(t_idx);
  auto cap = capacity_.load();
  capacity_.store(edge_table.capacity_.load());
  edge_table.capacity_.store(cap);
}

EdgeTable EdgeTable::Clone() const {
  EdgeTable cow_clone(meta_);
  cow_clone.ckp_ = ckp_;
  cow_clone.memory_level_ = memory_level_;
  cow_clone.out_csr_ = std::unique_ptr<CsrBase>(
      static_cast<CsrBase*>(out_csr_->Clone().release()));
  cow_clone.in_csr_ = std::unique_ptr<CsrBase>(
      static_cast<CsrBase*>(in_csr_->Clone().release()));

  if (table_) {
    cow_clone.table_ = table_->Clone();
  }

  cow_clone.table_idx_ = table_idx_.load();
  cow_clone.capacity_ = capacity_.load();
  return cow_clone;
}

void EdgeTable::DetachOutCsr() {
  CHECK(ckp_ != nullptr) << "Checkpoint is null, cannot detach out CSR";
  out_csr_->Detach(*ckp_, memory_level_);
}

void EdgeTable::DetachInCsr() {
  CHECK(ckp_ != nullptr) << "Checkpoint is null, cannot detach in CSR";
  in_csr_->Detach(*ckp_, memory_level_);
}

void EdgeTable::DetachOutAdjlist(vid_t vid, Allocator& alloc) {
  out_csr_->DetachVertex(vid, alloc);
}

void EdgeTable::DetachInAdjlist(vid_t vid, Allocator& alloc) {
  in_csr_->DetachVertex(vid, alloc);
}

void EdgeTable::SetEdgeSchema(std::shared_ptr<const EdgeSchema> meta) {
  meta_ = meta;
}

void EdgeTable::Close() {
  out_csr_.reset();
  in_csr_.reset();
  if (table_) {
    table_->close();
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

void EdgeTable::BatchDeleteEdges(
    const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
    const std::vector<std::pair<vid_t, int32_t>>& ie_edges) {
  out_csr_->batch_delete_edges(oe_edges);
  in_csr_->batch_delete_edges(ie_edges);
}

void EdgeTable::DeleteEdge(vid_t src_lid, vid_t dst_lid, int32_t oe_offset,
                           int32_t ie_offset, timestamp_t ts) {
  out_csr_->delete_edge(src_lid, oe_offset, ts);
  in_csr_->delete_edge(dst_lid, ie_offset, ts);
}

void EdgeTable::DeleteVertex(bool is_src, vid_t vid, timestamp_t ts) {
  auto oe_view = get_outgoing_view(ts);
  auto ie_view = get_incoming_view(ts);
  if (is_src) {
    auto oe_edges = oe_view.get_edges(vid);
    auto begin_ptr = oe_edges.start_ptr;
    auto stride = oe_edges.cfg.stride;
    for (auto iter = oe_edges.begin(); iter != oe_edges.end(); ++iter) {
      if (iter.get_timestamp() > ts) {
        continue;
      }
      int32_t oe_offset = static_cast<int32_t>(
          (reinterpret_cast<const char*>(iter.get_nbr_ptr()) -
           reinterpret_cast<const char*>(begin_ptr)) /
          stride);
      auto ie_offset = search_other_offset_with_cur_offset(
          oe_view, ie_view, vid, iter.get_vertex(), oe_offset,
          meta_->properties);
      DeleteEdge(vid, iter.get_vertex(), oe_offset, ie_offset, ts);
    }
  } else {
    auto ie_edges = ie_view.get_edges(vid);
    auto begin_ptr = ie_edges.start_ptr;
    auto stride = ie_edges.cfg.stride;
    for (auto iter = ie_edges.begin(); iter != ie_edges.end(); ++iter) {
      if (iter.get_timestamp() > ts) {
        continue;
      }
      int32_t ie_offset = static_cast<int32_t>(
          (reinterpret_cast<const char*>(iter.get_nbr_ptr()) -
           reinterpret_cast<const char*>(begin_ptr)) /
          stride);
      auto oe_offset = search_other_offset_with_cur_offset(
          ie_view, oe_view, vid, iter.get_vertex(), ie_offset,
          meta_->properties);
      DeleteEdge(iter.get_vertex(), vid, oe_offset, ie_offset, ts);
    }
  }
}

void EdgeTable::UpdateEdgeProperty(vid_t src_lid, vid_t dst_lid,
                                   int32_t oe_offset, int32_t ie_offset,
                                   int32_t col_id, const execution::Value& prop,
                                   timestamp_t ts) {
  auto accessor = get_edge_data_accessor(col_id);
  auto oe_edges = out_csr_->get_generic_view(ts).get_edges(src_lid);
  auto oe_iter = oe_edges.begin();
  oe_iter += oe_offset;
  if (oe_iter == oe_edges.end()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("invalid oe offset ");
  }
  accessor.set_data(oe_iter, prop, ts);
  if (meta_->is_bundled()) {
    auto ie_edges = in_csr_->get_generic_view(ts).get_edges(dst_lid);
    auto ie_iter = ie_edges.begin();
    ie_iter += ie_offset;
    if (ie_iter == ie_edges.end()) {
      THROW_INVALID_ARGUMENT_EXCEPTION("invalid ie offset ");
    }
    accessor.set_data(ie_iter, prop, ts);
  }
}

void EdgeTable::EnsureCapacity(size_t capacity) {
  if (!meta_->is_bundled()) {
    if (capacity <= capacity_.load()) {
      return;
    }
    capacity = std::max(capacity, 4096UL);
    table_->resize(capacity, meta_->get_default_property_values());
    capacity_.store(capacity);
  }
}

void EdgeTable::EnsureCapacity(vid_t src_v_cap, vid_t dst_v_cap,
                               size_t capacity) {
  if (src_v_cap > out_csr_->size()) {
    out_csr_->resize(src_v_cap);
  }
  if (dst_v_cap > in_csr_->size()) {
    in_csr_->resize(dst_v_cap);
  }
  EnsureCapacity(capacity);
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

size_t EdgeTable::PropertyNum() const { return table_->col_num(); }

CsrView EdgeTable::get_outgoing_view(timestamp_t ts) const {
  return out_csr_->get_generic_view(ts);
}

CsrView EdgeTable::get_incoming_view(timestamp_t ts) const {
  return in_csr_->get_generic_view(ts);
}

EdgeDataAccessor EdgeTable::get_edge_data_accessor(int col_id) const {
  if (col_id < 0 || static_cast<size_t>(col_id) >= meta_->properties.size()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Edge property column id out of range: " + std::to_string(col_id) +
        " (edge has " + std::to_string(meta_->properties.size()) +
        " properties)");
  }
  if (!meta_->is_bundled()) {
    return EdgeDataAccessor(
        meta_->properties[col_id].id(),
        const_cast<ColumnBase*>(table_->get_column_by_id(col_id)));
  } else {
    if (col_id != 0) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Bundled edges store a single inline property; expected col_id 0 "
          "but got " +
          std::to_string(col_id));
    }
    return EdgeDataAccessor(meta_->properties[0].id(), nullptr);
  }
}

EdgeDataAccessor EdgeTable::get_edge_data_accessor(
    const std::string& col_name) const {
  auto prop_ind = meta_->get_property_index(col_name);
  if (prop_ind == -1) {
    THROW_INVALID_ARGUMENT_EXCEPTION("property " + col_name +
                                     " not found in edge table, or deleted");
  }
  return get_edge_data_accessor(static_cast<int>(prop_ind));
}

void EdgeTable::AddProperties(
    Checkpoint& ckp, const std::vector<std::string>& prop_names,
    const std::vector<DataType>& prop_types,
    const std::vector<execution::Value>& default_values) {
  if (prop_names.empty()) {
    return;
  }

  if (table_->col_num() == 0) {
    // NOTE: Rather than check meta_->is_bundled(),we check whether the table
    // is empty.
    if (meta_->properties.size() == 1 &&
        meta_->properties[0].id() != DataTypeId::kVarchar) {
      dropAndCreateNewBundledCSR(ckp, nullptr);
    } else {
      dropAndCreateNewUnbundledCSR(ckp, false);
    }
  } else {
    size_t property_size = table_->get_column_by_id(0)->size();
    table_->add_columns(ckp, prop_names, prop_types, default_values,
                        property_size, memory_level_);
  }
}

void EdgeTable::RenameProperties(const std::vector<std::string>& old_names,
                                 const std::vector<std::string>& new_names) {
  CHECK_EQ(old_names.size(), new_names.size());
  for (size_t i = 0; i < old_names.size(); ++i) {
    if (!meta_->is_bundled()) {
      table_->rename_column(old_names[i], new_names[i]);
    }
  }
}

void EdgeTable::DeleteProperties(Checkpoint& ckp,
                                 const std::vector<std::string>& col_names) {
  if (meta_->is_bundled()) {
    if (meta_->property_names.size() <= 0) {
      return;
    }
    bool found = false;
    for (auto col : col_names) {
      if (col == meta_->property_names[0]) {
        found = true;
        break;
      }
    }
    if (found) {
      dropAndCreateNewUnbundledCSR(ckp, true);
    }
  } else {
    for (const auto& col : col_names) {
      table_->delete_column(col);
      VLOG(1) << "delete column " << col;
    }
    if (table_->col_num() == 0) {
      dropAndCreateNewUnbundledCSR(ckp, true);
    } else if (table_->col_num() == 1) {
      auto remaining_col = table_->get_column_by_id(0);
      if (remaining_col->type() != DataTypeId::kVarchar) {
        dropAndCreateNewBundledCSR(ckp, remaining_col);
      }
    }
  }
}

std::pair<int32_t, const void*> EdgeTable::AddEdge(
    vid_t src_lid, vid_t dst_lid,
    const std::vector<execution::Value>& edge_data, timestamp_t ts,
    Allocator& alloc, bool insert_safe) {
  return internal::insert_edge_into_csr_internal(
      *out_csr_, *in_csr_, *table_.get(), table_idx_, *meta_, src_lid, dst_lid,
      edge_data, ts, alloc, insert_safe);
}

void EdgeTable::BatchAddEdges(const IndexerType& src_indexer,
                              const IndexerType& dst_indexer,
                              std::shared_ptr<IDataChunkSupplier> supplier) {
  in_csr_->resize(dst_indexer.size());
  out_csr_->resize(src_indexer.size());
  std::vector<vid_t> src_lid, dst_lid;
  // Collect per-property columns across chunks (for bundled: single column;
  // for unbundled: full property DataChunks).
  std::vector<std::shared_ptr<execution::IContextColumn>> bundled_data_cols;
  std::vector<std::shared_ptr<execution::DataChunk>> unbundled_data_chunks;
  while (true) {
    auto chunk = supplier->GetNextChunk();
    if (chunk == nullptr) {
      break;
    }
    auto src_col = chunk->get(0);
    auto dst_col = chunk->get(1);
    parse_endpoint_column(src_indexer, src_col, src_lid);
    parse_endpoint_column(dst_indexer, dst_col, dst_lid);
    if (chunk->col_num() > 2) {
      if (meta_->is_bundled()) {
        // Bundled: only one property column (index 2).
        bundled_data_cols.push_back(chunk->get(2));
      } else {
        // Unbundled: collect remaining columns as a DataChunk.
        auto prop_chunk = std::make_shared<execution::DataChunk>();
        for (size_t i = 2; i < chunk->col_num(); ++i) {
          auto c = chunk->get(static_cast<int>(i));
          if (c) {
            prop_chunk->set(static_cast<int>(i - 2), c);
          }
        }
        unbundled_data_chunks.push_back(prop_chunk);
      }
    }
  }
  std::vector<bool> valid_flags;
  filterInvalidEdges(src_lid, dst_lid, valid_flags);
  size_t new_size = table_idx_.load() + src_lid.size();
  if (new_size >= Capacity()) {
    auto new_cap = new_size;
    while (new_size >= new_cap) {
      new_cap = new_cap < 4096 ? 4096 : new_cap + (new_cap + 4) / 5;
    }
    EnsureCapacity(new_cap);
  }
  if (meta_->is_bundled()) {
    batch_add_bundled_edges_impl(out_csr_.get(), in_csr_.get(), meta_, src_lid,
                                 dst_lid, bundled_data_cols, valid_flags);
  } else {
    auto oe_csr = dynamic_cast<TypedCsrBase<uint64_t>*>(out_csr_.get());
    auto ie_csr = dynamic_cast<TypedCsrBase<uint64_t>*>(in_csr_.get());
    assert(oe_csr != nullptr && ie_csr != nullptr);
    batch_add_unbundled_edges_impl(
        src_lid, dst_lid, oe_csr, ie_csr, table_.get(), table_idx_, capacity_,
        meta_->properties, unbundled_data_chunks, valid_flags);
  }
}

void EdgeTable::BatchAddEdges(
    const std::vector<vid_t>& src_lid_list,
    const std::vector<vid_t>& dst_lid_list,
    const std::vector<std::vector<execution::Value>>& edge_data_list) {
  size_t new_size = table_idx_.load() + src_lid_list.size();
  if (new_size >= Capacity()) {
    auto new_cap = new_size;
    while (new_size >= new_cap) {
      new_cap = new_cap < 4096 ? 4096 : new_cap + (new_cap + 4) / 5;
    }
    EnsureCapacity(new_cap);
  }
  if (meta_->is_bundled()) {
    std::vector<execution::Value> flat_edge_data;
    assert(meta_->properties.size() == 1);
    if (meta_->properties[0] == DataTypeId::kEmpty) {
    } else {
      flat_edge_data.reserve(edge_data_list.size());
      for (const auto& edata : edge_data_list) {
        assert(edata.size() == 1);
        flat_edge_data.push_back(edata[0]);
      }
    }
    auto prop_type = meta_->properties[0].id();
    batch_put_edges_to_bundled_csr(src_lid_list, dst_lid_list, prop_type,
                                   flat_edge_data, out_csr_.get());
    batch_put_edges_to_bundled_csr(dst_lid_list, src_lid_list, prop_type,
                                   flat_edge_data, in_csr_.get());
  } else {
    auto oe_csr = dynamic_cast<TypedCsrBase<uint64_t>*>(out_csr_.get());
    auto ie_csr = dynamic_cast<TypedCsrBase<uint64_t>*>(in_csr_.get());
    assert(oe_csr != nullptr && ie_csr != nullptr);
    size_t offset = table_idx_.fetch_add(src_lid_list.size());
    insert_edges_separated_impl(oe_csr, ie_csr, src_lid_list, dst_lid_list,
                                offset);
    for (size_t i = 0; i < edge_data_list.size(); ++i) {
      table_->insert(offset + i, edge_data_list[i], true);
    }
  }
}

void EdgeTable::Compact(bool compact_csr,
                        const std::optional<std::string>& sort_key_for_nbr,
                        timestamp_t ts) {
  if (compact_csr) {
    out_csr_->compact();
    in_csr_->compact();
  }
  // must reset timestamp before sorting, otherwise the unsorted_since_ may not
  // be properly setted
  out_csr_->reset_timestamp();
  in_csr_->reset_timestamp();
  if (sort_key_for_nbr.has_value()) {
    if (!meta_->is_bundled()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "sort key is not supported for unbundled edge table currently");
    }
    out_csr_->batch_sort_by_edge_data(1);
    in_csr_->batch_sort_by_edge_data(1);
  }
}

size_t EdgeTable::PropTableSize() const {
  if (meta_->is_bundled()) {
    return 0;
  }
  // TODO(zhanglei): the size may be inaccurate if some edges are deleted but
  // not compacted yet.
  return table_idx_.load();
}

size_t EdgeTable::Capacity() const {
  if (meta_->is_bundled()) {
    if (out_csr_) {
      return out_csr_->capacity();
    } else if (in_csr_) {
      return in_csr_->capacity();
    } else {
      THROW_RUNTIME_ERROR("both csr are null");
    }
  }
  return capacity_.load();
}

void EdgeTable::dropAndCreateNewBundledCSR(Checkpoint& ckp,
                                           ColumnBase* remaining_col) {
  DataTypeId property_type = (remaining_col == nullptr)
                                 ? meta_->properties[0].id()
                                 : remaining_col->type();

  std::unique_ptr<CsrBase> new_out_csr, new_in_csr;
  new_out_csr =
      create_csr(meta_->oe_mutable, meta_->oe_strategy, property_type);
  new_in_csr = create_csr(meta_->ie_mutable, meta_->ie_strategy, property_type);
  ModuleDescriptor out_csr_desc;
  ModuleDescriptor in_csr_desc;
  new_out_csr->Open(ckp, out_csr_desc, MemoryLevel::kInMemory);
  new_in_csr->Open(ckp, in_csr_desc, MemoryLevel::kInMemory);

  new_out_csr->resize(out_csr_->size());
  new_in_csr->resize(in_csr_->size());

  if (remaining_col == nullptr) {
    auto edges = out_csr_->batch_export(nullptr);
    auto default_props = meta_->get_default_property_values();
    batch_put_edges_with_default_edata(std::get<0>(edges), std::get<1>(edges),
                                       property_type, default_props[0],
                                       new_out_csr.get());
    batch_put_edges_with_default_edata(std::get<1>(edges), std::get<0>(edges),
                                       property_type, default_props[0],
                                       new_in_csr.get());
  } else {
    std::unique_ptr<ColumnBase> row_id_col_base(
        CreateColumn(DataTypeId::kUInt64));
    auto row_id_col = dynamic_cast<ULongColumn*>(row_id_col_base.get());
    row_id_col->Open(ckp, ModuleDescriptor(), MemoryLevel::kInMemory);
    auto edges = out_csr_->batch_export(row_id_col);
    std::vector<execution::Value> remaining_data;
    remaining_data.reserve(row_id_col->size());
    for (size_t i = 0; i < row_id_col->size(); ++i) {
      auto row_id = row_id_col->get_view(i);
      CHECK_LT(row_id, remaining_col->size());
      remaining_data.emplace_back(remaining_col->get_any(row_id));
    }
    batch_put_edges_to_bundled_csr(std::get<0>(edges), std::get<1>(edges),
                                   property_type, remaining_data,
                                   new_out_csr.get());
    batch_put_edges_to_bundled_csr(std::get<1>(edges), std::get<0>(edges),
                                   property_type, remaining_data,
                                   new_in_csr.get());
  }

  table_->close();
  table_ = std::make_unique<Table>();
  table_idx_.store(0);
  capacity_.store(0);
  out_csr_ = std::move(new_out_csr);
  in_csr_ = std::move(new_in_csr);
}

void EdgeTable::dropAndCreateNewUnbundledCSR(Checkpoint& ckp,
                                             bool delete_property) {
  // In this method, the edge table must be bundled, so the table must be
  // opened opened. In open_in_memory method, table will try to read the
  // existing table file from checkpoint_dir, but it must not exist.
  if (!delete_property) {
    LOG(INFO) << "rebuild unbundled edge csr with edge properties: "
              << meta_->property_names.size();
    table_ = std::make_unique<Table>(meta_->property_names, meta_->properties);
    table_->Init(ckp, MemoryLevel::kInMemory);
  }

  ColumnBase* prev_data_col = nullptr;

  if (!delete_property) {
    if (table_->col_num() >= 1 &&
        table_->get_column_by_id(0)->type() != DataTypeId::kVarchar &&
        table_->get_column_by_id(0)->type() != DataTypeId::kEmpty) {
      prev_data_col = table_->get_column_by_id(0);
    }
  } else {
    // delete_property == true, which means the EdgeTable will become use csr of
    // empty type. we need to reset capacity and table_idx to 0
    table_idx_.store(0);
    capacity_.store(0);
  }

  auto edges = out_csr_->batch_export(prev_data_col);
  auto prop_defaults = meta_->get_default_property_values();
  if (prev_data_col && prev_data_col->size() > 0) {
    table_->resize(prev_data_col->size(), prop_defaults);
    table_idx_.store(prev_data_col->size());
    EnsureCapacity(prev_data_col->size());
  } else if (!delete_property) {
    table_->resize(std::get<0>(edges).size(), prop_defaults);
    table_idx_.store(std::get<0>(edges).size());
    EnsureCapacity(std::get<0>(edges).size());
  }
  std::vector<uint64_t> row_ids;
  for (size_t i = 0; i < std::get<0>(edges).size(); ++i) {
    row_ids.push_back(i);
  }
  std::unique_ptr<CsrBase> new_out_csr, new_in_csr;
  if (delete_property) {
    new_out_csr =
        create_csr(meta_->oe_mutable, meta_->oe_strategy, DataTypeId::kEmpty);
    new_in_csr =
        create_csr(meta_->ie_mutable, meta_->ie_strategy, DataTypeId::kEmpty);
  } else {
    new_out_csr =
        create_csr(meta_->oe_mutable, meta_->oe_strategy, DataTypeId::kUInt64);
    new_in_csr =
        create_csr(meta_->ie_mutable, meta_->ie_strategy, DataTypeId::kUInt64);
  }

  new_out_csr->Open(ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
  new_in_csr->Open(ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
  new_out_csr->resize(out_csr_->size());
  new_in_csr->resize(in_csr_->size());
  if (delete_property) {
    dynamic_cast<TypedCsrBase<EmptyType>*>(new_out_csr.get())
        ->batch_put_edges(std::get<0>(edges), std::get<1>(edges), {});
    dynamic_cast<TypedCsrBase<EmptyType>*>(new_in_csr.get())
        ->batch_put_edges(std::get<1>(edges), std::get<0>(edges), {});
  } else {
    dynamic_cast<TypedCsrBase<uint64_t>*>(new_out_csr.get())
        ->batch_put_edges(std::get<0>(edges), std::get<1>(edges), row_ids);
    dynamic_cast<TypedCsrBase<uint64_t>*>(new_in_csr.get())
        ->batch_put_edges(std::get<1>(edges), std::get<0>(edges), row_ids);
  }
  out_csr_ = std::move(new_out_csr);
  in_csr_ = std::move(new_in_csr);
}

// --- Static key builders ---

static std::string EdgeKeyBase(const std::string& src, const std::string& edge,
                               const std::string& dst,
                               const std::string& suffix) {
  return "edge_" + src + "_" + edge + "_" + dst + "_" + suffix;
}

std::string EdgeTable::KeyOutCsr(const std::string& src,
                                 const std::string& edge,
                                 const std::string& dst) {
  return EdgeKeyBase(src, edge, dst, "out_csr");
}

std::string EdgeTable::KeyInCsr(const std::string& src, const std::string& edge,
                                const std::string& dst) {
  return EdgeKeyBase(src, edge, dst, "in_csr");
}

std::string EdgeTable::KeyProperty(const std::string& src,
                                   const std::string& edge,
                                   const std::string& dst, size_t index) {
  return EdgeKeyBase(src, edge, dst, "prop_" + std::to_string(index));
}

std::string EdgeTable::ScalarKey(const std::string& src,
                                 const std::string& edge,
                                 const std::string& dst,
                                 const std::string& field) {
  return "edge_" + src + "_" + edge + "_" + dst + "/" + field;
}

// --- Snapshot orchestration ---

EdgeTable EdgeTable::OpenFrom(std::shared_ptr<Checkpoint> ckp,
                              std::shared_ptr<const EdgeSchema> es,
                              ModuleBroker& store,
                              const CheckpointManifest& meta,
                              MemoryLevel level) {
  EdgeTable et(es);
  et.ckp_ = ckp;
  et.SetMemoryLevel(level);
  const auto& src = es->src_label_name;
  const auto& edge = es->edge_label_name;
  const auto& dst = es->dst_label_name;

  if (!store.Contains(KeyOutCsr(src, edge, dst))) {
    et.Init(ckp, level);
    return et;
  }

  et.SetInCsr(store.TakeModule<CsrBase>(KeyInCsr(src, edge, dst)));
  et.SetOutCsr(store.TakeModule<CsrBase>(KeyOutCsr(src, edge, dst)));

  if (!es->is_bundled()) {
    auto table = std::make_unique<Table>(es->property_names, es->properties);
    for (size_t i = 0; i < es->properties.size(); ++i) {
      table->SetColumn(
          static_cast<int>(i),
          store.TakeModule<ColumnBase>(KeyProperty(src, edge, dst, i)));
    }
    et.SetTable(std::move(table));
    et.SetTableIdx(
        meta.GetScalarAs<uint64_t>(ScalarKey(src, edge, dst, "table_idx"))
            .value_or(0));
  } else {
    et.SetTable(std::make_unique<Table>());
  }
  et.SetCapacity(
      meta.GetScalarAs<uint64_t>(ScalarKey(src, edge, dst, "capacity"))
          .value_or(0));
  return et;
}

void EdgeTable::DisassembleTo(ModuleBroker& store, CheckpointManifest& meta,
                              Checkpoint& ckp) {
  if (!meta_) {
    return;
  }
  const auto& src = meta_->src_label_name;
  const auto& edge = meta_->edge_label_name;
  const auto& dst = meta_->dst_label_name;

  store.SetModule(KeyOutCsr(src, edge, dst), TakeOutCsr());
  store.SetModule(KeyInCsr(src, edge, dst), TakeInCsr());
  if (!meta_->is_bundled()) {
    auto table = TakeTable();
    for (size_t i = 0; i < table->col_num(); ++i) {
      table->get_column_by_id(i)->Dump(ckp, meta,
                                       KeyProperty(src, edge, dst, i));
    }
    meta.SetScalar(ScalarKey(src, edge, dst, "table_idx"),
                   std::to_string(GetTableIdx()));
  }
  meta.SetScalar(ScalarKey(src, edge, dst, "capacity"),
                 std::to_string(GetCapacity()));
}

}  // namespace neug
