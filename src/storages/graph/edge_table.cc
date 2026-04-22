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

#include "neug/storages/csr/generic_view_utils.h"
#include "neug/storages/csr/immutable_csr.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/file_names.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/arrow_utils.h"
#include "neug/utils/file_utils.h"
#include "neug/utils/property/types.h"

namespace neug {

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

template <typename EDATA_T, typename ARROW_COL_T>
std::vector<Property> extract_edge_data(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& data_batches,
    const std::vector<bool>& valid_flags) {
  std::vector<Property> edge_data;
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
        edge_data.emplace_back(
            neug::PropUtils<EDATA_T>::to_prop(casted->Value(i)));
      }
    }
  }
  return edge_data;
}

// Helper alias to resolve the Arrow array type for a property type.
template <typename T>
using ExtractEdgeArrowArray = typename TypeConverter<T>::ArrowArrayType;

std::vector<Property> extract_bundled_edge_data_from_batches(
    std::shared_ptr<const EdgeSchema> meta,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& data_batches,
    const std::vector<bool>& valid_flags) {
  assert(meta->is_bundled());
  if (meta->properties.empty() ||
      meta->properties[0].id() == DataTypeId::kEmpty) {
    return std::vector<Property>();
  }
  switch (meta->properties[0].id()) {
#define EXTRACT_EDGE_DATA_CASE(enum_val, type)                                \
  case DataTypeId::enum_val:                                                  \
    return extract_edge_data<type, ExtractEdgeArrowArray<type>>(data_batches, \
                                                                valid_flags);
    FOR_EACH_DATA_TYPE_PRIMITIVE(EXTRACT_EDGE_DATA_CASE)
  case DataTypeId::kDate:
    return extract_edge_data<Date, arrow::Date64Array>(data_batches,
                                                       valid_flags);
  case DataTypeId::kTimestampMs:
    return extract_edge_data<DateTime, arrow::TimestampArray>(data_batches,
                                                              valid_flags);
  case DataTypeId::kInterval:
    return extract_edge_data<Interval, arrow::LargeStringArray>(data_batches,
                                                                valid_flags);
#undef EXTRACT_EDGE_DATA_CASE
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("not support edge data type: " +
                                  meta->properties[0].ToString());
  }
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

// TODO(zhanglei): Support default value for non-empty edge data type
void batch_put_edges_with_default_edata(const std::vector<vid_t>& src_lid,
                                        const std::vector<vid_t>& dst_lid,
                                        DataTypeId property_type,
                                        Property default_value,
                                        CsrBase* out_csr) {
  assert(src_lid.size() == dst_lid.size());
  switch (property_type) {
#define TYPE_DISPATCHER(enum_val, type)                                       \
  case DataTypeId::enum_val:                                                  \
    batch_put_edges_with_default_edata_impl<type>(                            \
        src_lid, dst_lid, PropUtils<type>::to_typed(default_value), out_csr); \
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

void batch_put_edges_to_bundled_csr(const std::vector<vid_t>& src_lid,
                                    const std::vector<vid_t>& dst_lid,
                                    DataTypeId property_type,
                                    const std::vector<Property>& edge_data,
                                    CsrBase* out_csr) {
  switch (property_type) {
#define TYPE_DISPATCHER(enum_val, type)                          \
  case DataTypeId::enum_val: {                                   \
    std::vector<type> typed_data;                                \
    typed_data.reserve(edge_data.size());                        \
    for (const auto& prop : edge_data) {                         \
      typed_data.emplace_back(PropUtils<type>::to_typed(prop));  \
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

static void parse_endpoint_column(const IndexerType& indexer,
                                  const std::shared_ptr<arrow::Array>& array,
                                  std::vector<vid_t>& lids) {
  if (array->type()->Equals(arrow::utf8())) {
    auto casted = std::static_pointer_cast<arrow::StringArray>(array);
    for (int64_t i = 0; i < casted->length(); ++i) {
      auto str = casted->GetView(i);
      std::string_view sv(str.data(), str.size());
      auto vid = indexer.get_index(Property::From(sv));
      lids.push_back(vid);
    }
  } else if (array->type()->Equals(arrow::large_utf8())) {
    auto casted = std::static_pointer_cast<arrow::LargeStringArray>(array);
    for (int64_t i = 0; i < casted->length(); ++i) {
      auto str = casted->GetView(i);
      std::string_view sv(str.data(), str.size());
      auto vid = indexer.get_index(Property::From(sv));
      lids.push_back(vid);
    }
  } else if (array->type()->Equals(arrow::int64())) {
    auto casted = std::static_pointer_cast<arrow::Int64Array>(array);
    for (int64_t i = 0; i < casted->length(); ++i) {
      auto vid = indexer.get_index(Property::From(casted->Value(i)));
      lids.push_back(vid);
    }
  } else if (array->type()->Equals(arrow::uint64())) {
    auto casted = std::static_pointer_cast<arrow::UInt64Array>(array);
    for (int64_t i = 0; i < casted->length(); ++i) {
      auto vid = indexer.get_index(Property::From(casted->Value(i)));
      lids.push_back(vid);
    }
  } else if (array->type()->Equals(arrow::int32())) {
    auto casted = std::static_pointer_cast<arrow::Int32Array>(array);
    for (int64_t i = 0; i < casted->length(); ++i) {
      auto vid = indexer.get_index(Property::From(casted->Value(i)));
      lids.push_back(vid);
    }
  } else if (array->type()->Equals(arrow::uint32())) {
    auto casted = std::static_pointer_cast<arrow::UInt32Array>(array);
    for (int64_t i = 0; i < casted->length(); ++i) {
      auto vid = indexer.get_index(Property::From(casted->Value(i)));
      lids.push_back(vid);
    }
  } else {
    LOG(FATAL) << "not support type " << array->type()->ToString();
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
    const std::vector<Property>& property_vec) {
  std::vector<EDATA_T> edge_data;
  edge_data.reserve(edge_data.size());
  for (const auto& prop : property_vec) {
    edge_data.push_back(PropUtils<EDATA_T>::to_typed(prop));
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

static std::vector<Property> get_row_from_recordbatch(
    const std::vector<DataType>& prop_types,
    const std::vector<std::shared_ptr<arrow::DataType>>& expected_types,
    const std::shared_ptr<arrow::RecordBatch>& rb, int64_t row_idx) {
  std::vector<Property> row;
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
      row.push_back(Property());
      continue;
    }
    switch (prop_types[i].id()) {
#define GET_PRIMITIVE_PROPERTY_CASE(enum_val, type)                   \
  case DataTypeId::enum_val: {                                        \
    auto casted =                                                     \
        std::static_pointer_cast<ExtractEdgeArrowArray<type>>(array); \
    row.push_back(PropUtils<type>::to_prop(casted->Value(row_idx)));  \
    break;                                                            \
  }
      FOR_EACH_DATA_TYPE_PRIMITIVE(GET_PRIMITIVE_PROPERTY_CASE)
#undef GET_PRIMITIVE_PROPERTY_CASE
    case DataTypeId::kVarchar: {
      if (array->type()->Equals(arrow::utf8())) {
        auto casted = std::static_pointer_cast<arrow::StringArray>(array);
        auto str = casted->GetView(row_idx);
        row.push_back(Property::from_string_view(str));
      } else if (array->type()->Equals(arrow::large_utf8())) {
        auto casted = std::static_pointer_cast<arrow::LargeStringArray>(array);
        auto str = casted->GetView(row_idx);
        row.push_back(Property::from_string_view(str));
      } else {
        LOG(FATAL) << "not support type " << array->type()->ToString();
      }
      break;
    }
    case DataTypeId::kDate: {
      auto casted = std::static_pointer_cast<arrow::Date64Array>(array);
      Date d;
      d.from_timestamp(casted->Value(row_idx));
      row.push_back(Property::from_date(d));
      break;
    }
    case DataTypeId::kTimestampMs: {
      auto casted = std::static_pointer_cast<arrow::TimestampArray>(array);
      row.push_back(Property::from_datetime(DateTime(casted->Value(row_idx))));
      break;
    }
    case DataTypeId::kInterval: {
      if (array->type()->id() == arrow::Type::STRING) {
        auto casted = std::static_pointer_cast<arrow::StringArray>(array);
        row.push_back(
            Property::from_interval(Interval(casted->GetView(row_idx))));
      } else if (array->type()->id() == arrow::Type::LARGE_STRING) {
        auto casted = std::static_pointer_cast<arrow::LargeStringArray>(array);
        row.push_back(
            Property::from_interval(Interval(casted->GetView(row_idx))));
      } else {
        THROW_NOT_IMPLEMENTED_EXCEPTION("Not support typed: " +
                                        array->type()->ToString());
      }
      break;
    }
    default:
      LOG(FATAL) << "not support type " << array->type()->ToString();
    }
  }
  return row;
}

void batch_add_unbundled_edges_impl(
    const std::vector<vid_t>& src_lid_list,
    const std::vector<vid_t>& dst_lid_list, TypedCsrBase<uint64_t>* out_csr,
    TypedCsrBase<uint64_t>* in_csr, Table* table_,
    std::atomic<uint64_t>& table_idx_, std::atomic<uint64_t>& capacity_,
    const std::vector<DataType>& prop_types,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& data_batches,
    const std::vector<bool>& valid_flags) {
  size_t offset = table_idx_.fetch_add(src_lid_list.size());
  insert_edges_separated_impl(out_csr, in_csr, src_lid_list, dst_lid_list,
                              offset);
  std::vector<std::shared_ptr<arrow::DataType>> expected_types;
  for (auto pt : prop_types) {
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
        auto row = get_row_from_recordbatch(prop_types, expected_types, rb, i);
        table_->insert(offset++, row, true);
      }
    }
  }
}

void batch_add_bundled_edges_impl(CsrBase* out_csr, CsrBase* in_csr,
                                  const std::vector<DataType>& prop_types,
                                  const std::vector<vid_t>& src_lid_list,
                                  const std::vector<vid_t>& dst_lid_list,
                                  const std::vector<Property>& edge_data) {
  if (prop_types.empty() || prop_types[0].id() == DataTypeId::kEmpty) {
    insert_edges_empty_impl(dynamic_cast<TypedCsrBase<EmptyType>*>(out_csr),
                            dynamic_cast<TypedCsrBase<EmptyType>*>(in_csr),
                            src_lid_list, dst_lid_list);
    return;
  }
  switch (prop_types[0].id()) {
#define TYPE_DISPATCHER(enum_val, type)                                        \
  case DataTypeId::enum_val:                                                   \
    insert_edges_bundled_typed_impl(                                           \
        dynamic_cast<TypedCsrBase<type>*>(out_csr),                            \
        dynamic_cast<TypedCsrBase<type>*>(in_csr), src_lid_list, dst_lid_list, \
        edge_data);                                                            \
    break;
    FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("not support edge data type: " +
                                  std::to_string(prop_types[0].id()));
  }
}

EdgeTable::EdgeTable(std::shared_ptr<const EdgeSchema> meta) : meta_(meta) {
  table_ = std::make_unique<Table>();

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
  capacity_ = edge_table.capacity_.load();
}

void EdgeTable::Swap(EdgeTable& edge_table) {
  std::swap(meta_, edge_table.meta_);
  std::swap(work_dir_, edge_table.work_dir_);
  std::swap(memory_level_, edge_table.memory_level_);
  auto v = csr_alter_version_.load();
  csr_alter_version_.store(edge_table.csr_alter_version_.load());
  edge_table.csr_alter_version_.store(v);
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

void EdgeTable::SetEdgeSchema(std::shared_ptr<const EdgeSchema> meta) {
  meta_ = meta;
}

void load_statistic_file(const std::string& work_dir,
                         const std::string& src_label_name,
                         const std::string& dst_label_name,
                         const std::string& edge_label_name,
                         std::atomic<uint64_t>& cap_atomic,
                         std::atomic<uint64_t>& table_idx_atomic) {
  size_t cap = 0, size = 0;
  auto statistic_file_path =
      checkpoint_dir(work_dir) + "/" +
      statistics_file_prefix(src_label_name, dst_label_name, edge_label_name);
  if (!std::filesystem::exists(statistic_file_path)) {
    cap_atomic.store(0);
    table_idx_atomic.store(0);
    return;
  }
  read_statistic_file(statistic_file_path, cap, size);
  cap_atomic.store(cap);
  table_idx_atomic.store(size);
}

void EdgeTable::Open(const std::string& work_dir, MemoryLevel memory_level) {
  work_dir_ = work_dir;
  memory_level_ = memory_level;
  auto ckp_dir_path = checkpoint_dir(work_dir);
  auto ie_prefix_path = ie_prefix(meta_->src_label_name, meta_->dst_label_name,
                                  meta_->edge_label_name);
  auto oe_prefix_path = oe_prefix(meta_->src_label_name, meta_->dst_label_name,
                                  meta_->edge_label_name);
  auto edata_prefix_path = edata_prefix(
      meta_->src_label_name, meta_->dst_label_name, meta_->edge_label_name);
  if (memory_level == MemoryLevel::kSyncToFile) {
    in_csr_->open(ie_prefix_path, ckp_dir_path, work_dir);
    out_csr_->open(oe_prefix_path, ckp_dir_path, work_dir);
  } else if (memory_level == MemoryLevel::kInMemory) {
    in_csr_->open_in_memory(ckp_dir_path + "/" + ie_prefix_path);
    out_csr_->open_in_memory(ckp_dir_path + "/" + oe_prefix_path);
  } else if (memory_level == MemoryLevel::kHugePagePreferred) {
    in_csr_->open_with_hugepages(ckp_dir_path + "/" + ie_prefix_path);
    out_csr_->open_with_hugepages(ckp_dir_path + "/" + oe_prefix_path);
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "unsupported memory level: " +
        std::to_string(static_cast<int>(memory_level)));
  }

  if (!meta_->is_bundled()) {
    if (memory_level == MemoryLevel::kSyncToFile) {
      table_->open(edata_prefix_path, work_dir_, meta_->property_names,
                   meta_->properties);
    } else if (memory_level == MemoryLevel::kInMemory) {
      table_->open_in_memory(edata_prefix_path, work_dir_,
                             meta_->property_names, meta_->properties);
    } else if (memory_level == MemoryLevel::kHugePagePreferred) {
      table_->open_with_hugepages(edata_prefix_path, work_dir_,
                                  meta_->property_names, meta_->properties);
    } else {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "unsupported memory level: " +
          std::to_string(static_cast<int>(memory_level)));
    }
    assert(table_->col_num() > 0);
    size_t table_cap = table_->get_column_by_id(0)->size();
    load_statistic_file(work_dir, meta_->src_label_name, meta_->dst_label_name,
                        meta_->edge_label_name, capacity_, table_idx_);
    if (table_cap != capacity_.load()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "capacity in statistic file not match actual table capacity, maybe "
          "the graph is not dumped properly");
    }
  }
}

void EdgeTable::Dump(const std::string& checkpoint_dir_path) {
  in_csr_->dump(ie_prefix(meta_->src_label_name, meta_->dst_label_name,
                          meta_->edge_label_name),
                checkpoint_dir_path);
  out_csr_->dump(oe_prefix(meta_->src_label_name, meta_->dst_label_name,
                           meta_->edge_label_name),
                 checkpoint_dir_path);
  if (!meta_->is_bundled()) {
    table_->dump(edata_prefix(meta_->src_label_name, meta_->dst_label_name,
                              meta_->edge_label_name),
                 checkpoint_dir_path);
    auto statistc_file_path =
        checkpoint_dir_path + "/" +
        statistics_file_prefix(meta_->src_label_name, meta_->dst_label_name,
                               meta_->edge_label_name);
    write_statistic_file(statistc_file_path, Capacity(), PropTableSize());
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

void EdgeTable::RevertDeleteEdge(vid_t src_lid, vid_t dst_lid,
                                 int32_t oe_offset, int32_t ie_offset,
                                 timestamp_t ts) {
  out_csr_->revert_delete_edge(src_lid, dst_lid, oe_offset, ts);
  in_csr_->revert_delete_edge(dst_lid, src_lid, ie_offset, ts);
}

void EdgeTable::UpdateEdgeProperty(vid_t src_lid, vid_t dst_lid,
                                   int32_t oe_offset, int32_t ie_offset,
                                   int32_t col_id, const Property& prop,
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
    table_->resize(capacity, meta_->default_property_values);
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

GenericView EdgeTable::get_outgoing_view(timestamp_t ts) const {
  return out_csr_->get_generic_view(ts);
}

GenericView EdgeTable::get_incoming_view(timestamp_t ts) const {
  return in_csr_->get_generic_view(ts);
}

EdgeDataAccessor EdgeTable::get_edge_data_accessor(int col_id) const {
  if (!meta_->is_bundled()) {
    return EdgeDataAccessor(meta_->properties[col_id].id(),
                            table_->get_column_by_id(col_id).get());
  } else {
    if (meta_->properties.empty()) {
      return EdgeDataAccessor(DataTypeId::kEmpty, nullptr);
    } else {
      return EdgeDataAccessor(meta_->properties[0].id(), nullptr);
    }
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

void EdgeTable::AddProperties(const std::vector<std::string>& prop_names,
                              const std::vector<DataType>& prop_types,
                              const std::vector<Property>& default_values) {
  if (prop_names.empty()) {
    return;
  }

  if (table_->col_num() == 0) {
    // NOTE: Rather than check meta_->is_bundled(),we check whether the table
    // is empty.
    if (meta_->properties.size() == 1 &&
        meta_->properties[0].id() != DataTypeId::kVarchar) {
      dropAndCreateNewBundledCSR(nullptr);
    } else {
      dropAndCreateNewUnbundledCSR(false);
    }
  } else {
    size_t property_size = table_->get_column_by_id(0)->size();
    table_->add_columns(prop_names, prop_types, default_values, property_size,
                        memory_level_);
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

void EdgeTable::DeleteProperties(const std::vector<std::string>& col_names) {
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
      dropAndCreateNewUnbundledCSR(true);
    }
  } else {
    for (const auto& col : col_names) {
      table_->delete_column(col);
      VLOG(1) << "delete column " << col;
    }
    if (table_->col_num() == 0) {
      dropAndCreateNewUnbundledCSR(true);
    } else if (table_->col_num() == 1) {
      auto remaining_col = table_->get_column_by_id(0);
      if (remaining_col->type() != DataTypeId::kVarchar) {
        dropAndCreateNewBundledCSR(remaining_col);
      }
    }
  }
}

int32_t EdgeTable::AddEdge(vid_t src_lid, vid_t dst_lid,
                           const std::vector<Property>& edge_data,
                           timestamp_t ts, Allocator& alloc, bool insert_safe) {
  int32_t oe_offset;
  if (meta_->is_bundled()) {
    assert(edge_data.size() == 1 ||
           (edge_data.size() == 0 &&
            (meta_->properties.empty() ||
             meta_->properties[0] == DataTypeId::kEmpty)));
    Property bundled_data = edge_data.empty() ? Property() : edge_data[0];
    in_csr_->put_generic_edge(dst_lid, src_lid, bundled_data, ts, alloc);
    oe_offset =
        out_csr_->put_generic_edge(src_lid, dst_lid, bundled_data, ts, alloc);
  } else {
    if (meta_->properties.size() != edge_data.size()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "edge data size not match edge table property size");
    }
    size_t row_id = table_idx_.fetch_add(1);
    Property prop;
    prop.set_uint64(row_id);
    in_csr_->put_generic_edge(dst_lid, src_lid, prop, ts, alloc);
    oe_offset = out_csr_->put_generic_edge(src_lid, dst_lid, prop, ts, alloc);
    table_->insert(row_id, edge_data, insert_safe);
  }
  return oe_offset;
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
  size_t new_size = table_idx_.load() + src_lid.size();
  if (new_size >= Capacity()) {
    auto new_cap = new_size;
    while (new_size >= new_cap) {
      new_cap = new_cap < 4096 ? 4096 : new_cap + (new_cap + 4) / 5;
    }
    EnsureCapacity(new_cap);
  }
  if (meta_->is_bundled()) {
    auto edges = extract_bundled_edge_data_from_batches(meta_, data_batches,
                                                        valid_flags);
    batch_add_bundled_edges_impl(out_csr_.get(), in_csr_.get(),
                                 meta_->properties, src_lid, dst_lid, edges);
  } else {
    auto oe_csr = dynamic_cast<TypedCsrBase<uint64_t>*>(out_csr_.get());
    auto ie_csr = dynamic_cast<TypedCsrBase<uint64_t>*>(in_csr_.get());
    assert(oe_csr != nullptr && ie_csr != nullptr);
    batch_add_unbundled_edges_impl(
        src_lid, dst_lid, oe_csr, ie_csr, table_.get(), table_idx_, capacity_,
        meta_->properties, data_batches, valid_flags);
  }
}

void EdgeTable::BatchAddEdges(
    const std::vector<vid_t>& src_lid_list,
    const std::vector<vid_t>& dst_lid_list,
    const std::vector<std::vector<Property>>& edge_data_list) {
  size_t new_size = table_idx_.load() + src_lid_list.size();
  if (new_size >= Capacity()) {
    auto new_cap = new_size;
    while (new_size >= new_cap) {
      new_cap = new_cap < 4096 ? 4096 : new_cap + (new_cap + 4) / 5;
    }
    EnsureCapacity(new_cap);
  }
  if (meta_->is_bundled()) {
    std::vector<Property> flat_edge_data;
    assert(meta_->properties.size() == 1);
    if (meta_->properties[0] == DataTypeId::kEmpty) {
    } else {
      flat_edge_data.reserve(edge_data_list.size());
      for (const auto& edata : edge_data_list) {
        assert(edata.size() == 1);
        flat_edge_data.push_back(edata[0]);
      }
    }
    batch_add_bundled_edges_impl(out_csr_.get(), in_csr_.get(),
                                 meta_->properties, src_lid_list, dst_lid_list,
                                 flat_edge_data);
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

void EdgeTable::Compact(bool compact_csr, bool sort_on_compaction,
                        timestamp_t ts) {
  if (compact_csr) {
    out_csr_->compact();
    in_csr_->compact();
  }
  if (sort_on_compaction) {
    out_csr_->batch_sort_by_edge_data(ts);
    in_csr_->batch_sort_by_edge_data(ts);
  }
  out_csr_->reset_timestamp();
  in_csr_->reset_timestamp();
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

void EdgeTable::dropAndCreateNewBundledCSR(
    std::shared_ptr<ColumnBase> remaining_col) {
  DataTypeId property_type = (remaining_col == nullptr)
                                 ? meta_->properties[0].id()
                                 : remaining_col->type();
  auto suffix = get_next_csr_path_suffix();
  std::string next_oe_csr_path =
      tmp_dir(work_dir_) + "/" +
      oe_prefix(meta_->src_label_name, meta_->dst_label_name,
                meta_->edge_label_name) +
      suffix;
  std::string next_ie_csr_path =
      tmp_dir(work_dir_) + "/" +
      ie_prefix(meta_->src_label_name, meta_->dst_label_name,
                meta_->edge_label_name) +
      suffix;

  std::unique_ptr<CsrBase> new_out_csr, new_in_csr;
  new_out_csr =
      create_csr(meta_->oe_mutable, meta_->oe_strategy, property_type);
  new_in_csr = create_csr(meta_->ie_mutable, meta_->ie_strategy, property_type);

  new_out_csr->open_in_memory(next_oe_csr_path);
  new_in_csr->open_in_memory(next_ie_csr_path);
  new_out_csr->resize(out_csr_->size());
  new_in_csr->resize(in_csr_->size());

  if (remaining_col == nullptr) {
    auto edges = out_csr_->batch_export(nullptr);
    batch_put_edges_with_default_edata(
        std::get<0>(edges), std::get<1>(edges), property_type,
        meta_->default_property_values[0], new_out_csr.get());
    batch_put_edges_with_default_edata(
        std::get<1>(edges), std::get<0>(edges), property_type,
        meta_->default_property_values[0], new_in_csr.get());
  } else {
    auto row_id_col = std::make_shared<ULongColumn>();
    row_id_col->open_in_memory("");
    auto edges = out_csr_->batch_export(row_id_col);
    std::vector<Property> remaining_data;
    remaining_data.reserve(row_id_col->size());
    for (size_t i = 0; i < row_id_col->size(); ++i) {
      auto row_id = row_id_col->get_view(i);
      CHECK_LT(row_id, remaining_col->size());
      remaining_data.emplace_back(remaining_col->get_prop(row_id));
    }
    batch_put_edges_to_bundled_csr(std::get<0>(edges), std::get<1>(edges),
                                   property_type, remaining_data,
                                   new_out_csr.get());
    batch_put_edges_to_bundled_csr(std::get<1>(edges), std::get<0>(edges),
                                   property_type, remaining_data,
                                   new_in_csr.get());
  }

  table_->drop();
  table_ = std::make_unique<Table>();
  table_idx_.store(0);
  capacity_.store(0);
  out_csr_->close();
  in_csr_->close();
  out_csr_ = std::move(new_out_csr);
  in_csr_ = std::move(new_in_csr);
}

void EdgeTable::dropAndCreateNewUnbundledCSR(bool delete_property) {
  auto suffix = get_next_csr_path_suffix();
  std::string next_oe_csr_path =
      tmp_dir(work_dir_) + "/" +
      oe_prefix(meta_->src_label_name, meta_->dst_label_name,
                meta_->edge_label_name) +
      suffix;
  std::string next_ie_csr_path =
      tmp_dir(work_dir_) + "/" +
      ie_prefix(meta_->src_label_name, meta_->dst_label_name,
                meta_->edge_label_name) +
      suffix;
  std::string next_table_prefix = edata_prefix(
      meta_->src_label_name, meta_->dst_label_name, meta_->edge_label_name);
  // In this method, the edge table must be bundled, so the table must be
  // opened opened. In open_in_memory method, table will try to read the
  // existing table file from checkpoint_dir, but it must not exist.
  if (!delete_property) {
    LOG(INFO) << "rebuild unbundled edge csr with edge properties: "
              << meta_->property_names.size();
    table_->open_in_memory(next_table_prefix, work_dir_, meta_->property_names,
                           meta_->properties);
  }

  std::shared_ptr<ColumnBase> prev_data_col = nullptr;

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
  if (prev_data_col && prev_data_col->size() > 0) {
    table_->resize(prev_data_col->size(), meta_->default_property_values);
    table_idx_.store(prev_data_col->size());
    EnsureCapacity(prev_data_col->size());
  } else if (!delete_property) {
    table_->resize(std::get<0>(edges).size(), meta_->default_property_values);
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

  new_out_csr->open_in_memory(next_oe_csr_path);
  new_in_csr->open_in_memory(next_ie_csr_path);
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
  out_csr_->close();
  in_csr_->close();
  out_csr_ = std::move(new_out_csr);
  in_csr_ = std::move(new_in_csr);
}

std::string EdgeTable::get_next_csr_path_suffix() {
  return std::string("_v_") + std::to_string(csr_alter_version_.fetch_add(1));
}

}  // namespace neug
