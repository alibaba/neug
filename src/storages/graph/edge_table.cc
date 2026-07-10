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
#include <sys/resource.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <ostream>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "neug/common/columns/value_columns.h"
#include "neug/common/types/container_types.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/csr/csr_view_utils.h"
#include "neug/storages/csr/immutable_csr.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/loader/chunk_pipeline_utils.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/storages/module/type_name.h"
#include "neug/storages/module_descriptor.h"
#include "neug/utils/io/file/file_utils.h"
#include "neug/utils/property/types.h"

namespace neug {

namespace {

size_t vector_bool_storage_bytes(size_t bit_capacity) {
  return (bit_capacity + 7) / 8;
}

uint64_t process_peak_rss_bytes() {
  struct rusage usage {};
  if (getrusage(RUSAGE_SELF, &usage) != 0) {
    return 0;
  }
#if defined(__APPLE__)
  return static_cast<uint64_t>(usage.ru_maxrss);
#else
  return static_cast<uint64_t>(usage.ru_maxrss) * 1024;
#endif
}

size_t estimate_edge_fallback_buffer_bytes(
    const std::vector<vid_t>& src_lid, const std::vector<vid_t>& dst_lid,
    const std::vector<bool>& valid_flags,
    const std::vector<std::shared_ptr<IContextColumn>>& bundled_data_cols,
    const std::vector<std::shared_ptr<DataChunk>>& unbundled_data_chunks) {
  return src_lid.capacity() * sizeof(vid_t) +
         dst_lid.capacity() * sizeof(vid_t) +
         vector_bool_storage_bytes(valid_flags.capacity()) +
         bundled_data_cols.capacity() *
             sizeof(std::shared_ptr<IContextColumn>) +
         unbundled_data_chunks.capacity() * sizeof(std::shared_ptr<DataChunk>);
}

}  // namespace

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
                                        const Value& default_value,
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

void batch_put_edges_to_bundled_csr(const std::vector<vid_t>& src_lid,
                                    const std::vector<vid_t>& dst_lid,
                                    DataTypeId property_type,
                                    const std::vector<Value>& edge_data,
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
    const std::vector<std::shared_ptr<IContextColumn>>& data_cols,
    const std::vector<bool>& valid_flags) {
  std::vector<EDATA_T> edge_data;
  edge_data.reserve(src_lid.size());
  size_t cur_index = 0;
  for (auto& col : data_cols) {
    auto value_col = std::dynamic_pointer_cast<ValueColumn<EDATA_T>>(col);
    if (value_col) {
      for (size_t i = 0; i < value_col->size(); ++i) {
        if (valid_flags[cur_index++]) {
          edge_data.push_back(value_col->get_value(i));
        }
      }
    } else {
      // Fallback: virtual dispatch path (non-ValueColumn, rare).
      for (size_t i = 0; i < col->size(); ++i) {
        if (valid_flags[cur_index++]) {
          auto val = col->get_elem(i);
          edge_data.push_back(val.template GetValue<EDATA_T>());
        }
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

/// Type-erased inserter: writes ValueColumn<T>::get_value(src_idx) to
/// TypedColumn<T>::set_value(dst_idx), bypassing get_elem() + set_any().
struct TypedColumnInserter {
  const IContextColumn* src;
  ColumnBase* dst;
  void (*fn)(const TypedColumnInserter&, size_t dst_idx, size_t src_idx,
             bool insert_safe);

  inline void insert(size_t dst_idx, size_t src_idx, bool insert_safe) const {
    fn(*this, dst_idx, src_idx, insert_safe);
  }
};

/// Fixed-length types: direct set_value, no Value, no virtual dispatch.
/// Null entries already have T() in data_, so set_value writes the same
/// default that set_any would write for null.
template <typename T>
void insert_typed_impl(const TypedColumnInserter& ins, size_t dst_idx,
                       size_t src_idx, bool /*insert_safe*/) {
  auto* typed_dst = static_cast<TypedColumn<T>*>(ins.dst);
  auto vc = static_cast<const ValueColumn<T>*>(ins.src);
  typed_dst->set_value(dst_idx, vc->get_value(src_idx));
}

/// Varchar: source is ValueColumn<std::string>, dest is
/// TypedColumn<std::string_view>. Needs set_any for buffer resize, but skips
/// get_elem() virtual dispatch.
void insert_varchar_impl(const TypedColumnInserter& ins, size_t dst_idx,
                         size_t src_idx, bool insert_safe) {
  auto* typed_dst = static_cast<TypedColumn<std::string_view>*>(ins.dst);
  auto vc = static_cast<const ValueColumn<std::string>*>(ins.src);
  typed_dst->set_any(dst_idx,
                     Value::CreateValue<std::string>(vc->get_value(src_idx)),
                     insert_safe);
}

TypedColumnInserter make_inserter(const DataType& type,
                                  const IContextColumn* src, ColumnBase* dst) {
  switch (type.id()) {
#define MAKE_INSERTER(enum_val, cpp_type) \
  case DataTypeId::enum_val:              \
    return {src, dst, &insert_typed_impl<cpp_type>};
    FOR_EACH_DATA_TYPE_NO_STRING(MAKE_INSERTER)
#undef MAKE_INSERTER
  case DataTypeId::kVarchar:
    return {src, dst, &insert_varchar_impl};
  default:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unsupported data type for column inserter: " +
        std::to_string(static_cast<int>(type.id())));
    return {};
  }
}

void batch_add_unbundled_edges_impl(
    const std::vector<vid_t>& src_lid_list,
    const std::vector<vid_t>& dst_lid_list, TypedCsrBase<uint64_t>* out_csr,
    TypedCsrBase<uint64_t>* in_csr, Table* table_,
    std::atomic<uint64_t>& table_idx_, std::atomic<uint64_t>& capacity_,
    const std::vector<DataType>& prop_types,
    const std::vector<std::shared_ptr<DataChunk>>& data_chunks,
    const std::vector<bool>& valid_flags) {
  size_t offset = table_idx_.fetch_add(src_lid_list.size());
  insert_edges_separated_impl(out_csr, in_csr, src_lid_list, dst_lid_list,
                              offset);
  size_t cur_index = 0;
  for (auto& chunk : data_chunks) {
    size_t num_rows = chunk->row_num();
    // Build per-column accessors for this chunk.
    std::vector<std::shared_ptr<IContextColumn>> prop_cols;
    prop_cols.reserve(chunk->col_num());
    for (auto& c : chunk->columns) {
      if (c)
        prop_cols.push_back(c);
    }
    // Pre-resolve typed inserters for each column (once per chunk).
    std::vector<TypedColumnInserter> inserters;
    inserters.reserve(prop_cols.size());
    for (size_t j = 0; j < prop_cols.size(); ++j) {
      inserters.push_back(make_inserter(prop_types[j], prop_cols[j].get(),
                                        table_->get_column_by_id(j)));
    }
    // Pre-compute valid rows: column-major loop needs branch-free inner loop.
    std::vector<size_t> valid_rows;
    valid_rows.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
      assert(cur_index < valid_flags.size());
      if (valid_flags[cur_index++]) {
        valid_rows.push_back(i);
      }
    }
    // Column-major: same fn pointer per inner iteration (BTB-friendly),
    // sequential dst writes (cache-friendly), no branch.
    for (auto& ins : inserters) {
      for (size_t k = 0; k < valid_rows.size(); ++k) {
        ins.insert(offset + k, valid_rows[k], true);
      }
    }
    offset += valid_rows.size();
  }
}

void batch_add_bundled_edges_impl(
    CsrBase* out_csr, CsrBase* in_csr, std::shared_ptr<const EdgeSchema> meta,
    const std::vector<vid_t>& src_lid_list,
    const std::vector<vid_t>& dst_lid_list,
    const std::vector<std::shared_ptr<IContextColumn>>& data_cols,
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

template <typename EDATA_T>
class EmptyCsrBulkWriter {
 public:
  static constexpr bool kSupportsDisjointConcurrentFill = true;
  static constexpr bool kStoresEdges = false;
  static constexpr bool kNeedsDegreeCount = false;
  static constexpr bool kChecksSingleUniqueness = false;
  static constexpr bool kNeedsConcurrentGrouping = false;
  static constexpr bool kTracksInputEdgeCount = false;

  void PrepareBuild(vid_t /*vertex_count*/) {}
  void AllocateFromCounts() {}
  int ReserveConcurrent(vid_t /*src*/, int /*count*/) { return 0; }
  void PutAt(vid_t /*src*/, int /*slot*/, vid_t /*dst*/,
             const EDATA_T& /*data*/, timestamp_t /*ts*/) {}
  void PutSerial(vid_t /*src*/, vid_t /*dst*/, const EDATA_T& /*data*/,
                 timestamp_t /*ts*/) {}
  void Finish() {}
};

template <typename EDATA_T, typename F>
bool with_csr_bulk_writer(CsrBase* csr, F&& callback) {
  if (auto* typed = dynamic_cast<MutableCsr<EDATA_T>*>(csr)) {
    MutableCsrBulkBuildAccess<EDATA_T> writer(*typed);
    std::forward<F>(callback)(writer);
    return true;
  }
  if (auto* typed = dynamic_cast<SingleMutableCsr<EDATA_T>*>(csr)) {
    SingleMutableCsrBulkBuildAccess<EDATA_T> writer(*typed);
    std::forward<F>(callback)(writer);
    return true;
  }
  if (dynamic_cast<EmptyCsr<EDATA_T>*>(csr) != nullptr) {
    EmptyCsrBulkWriter<EDATA_T> writer;
    std::forward<F>(callback)(writer);
    return true;
  }
  return false;
}

template <typename EDATA_T>
class BulkEdgeDataReader {
 public:
  explicit BulkEdgeDataReader(const std::shared_ptr<IContextColumn>& column)
      : column_(column.get()) {
    CHECK(column_ != nullptr);
    auto values = std::dynamic_pointer_cast<ValueColumn<EDATA_T>>(column);
    if (values) {
      values_ = &values->data();
    }
  }

  EDATA_T Get(size_t row) const {
    if (values_ != nullptr) {
      return (*values_)[row];
    }
    return column_->get_elem(row).template GetValue<EDATA_T>();
  }

 private:
  const IContextColumn* column_;
  const vector_t<EDATA_T>* values_ = nullptr;
};

template <>
class BulkEdgeDataReader<EmptyType> {
 public:
  explicit BulkEdgeDataReader(
      const std::shared_ptr<IContextColumn>& /*column*/) {}

  EmptyType Get(size_t /*row*/) const { return EmptyType(); }
};

struct BulkEdgeEndpointScratch {
  std::vector<vid_t> src_lids;
  std::vector<vid_t> dst_lids;
};

void index_bulk_edge_endpoints(const std::shared_ptr<DataChunk>& chunk,
                               const IndexerType& src_indexer,
                               const IndexerType& dst_indexer,
                               BulkEdgeEndpointScratch& scratch) {
  CHECK(chunk != nullptr);
  CHECK_GE(chunk->col_num(), 2);
  auto src_column = chunk->get(0);
  auto dst_column = chunk->get(1);
  CHECK(src_column != nullptr);
  CHECK(dst_column != nullptr);
  CHECK_EQ(src_column->size(), dst_column->size());

  scratch.src_lids.clear();
  scratch.dst_lids.clear();
  src_indexer.get_index(*src_column, scratch.src_lids);
  dst_indexer.get_index(*dst_column, scratch.dst_lids);
  CHECK_EQ(scratch.src_lids.size(), scratch.dst_lids.size());
}

inline bool is_valid_bulk_edge(vid_t src, vid_t dst) {
  return src != std::numeric_limits<vid_t>::max() &&
         dst != std::numeric_limits<vid_t>::max();
}

size_t count_valid_bulk_edges(const BulkEdgeEndpointScratch& endpoints) {
  size_t valid_edges = 0;
  for (size_t row = 0; row < endpoints.src_lids.size(); ++row) {
    valid_edges +=
        is_valid_bulk_edge(endpoints.src_lids[row], endpoints.dst_lids[row]);
  }
  return valid_edges;
}

struct BulkEdgeCountScratch {
  BulkEdgeEndpointScratch endpoints;
  flat_hash_map<vid_t, uint32_t> out_counts;
  flat_hash_map<vid_t, uint32_t> in_counts;
  bool out_single_duplicate = false;
  bool in_single_duplicate = false;
  uint64_t valid_edges = 0;
};

struct BulkEdgeCountChunkResult {
  size_t valid_edges = 0;
  size_t out_single_checks = 0;
  size_t in_single_checks = 0;
};

struct BulkEdgeCountProfile {
  size_t out_updates = 0;
  size_t in_updates = 0;
};

struct BulkEdgeCountSummary {
  uint64_t valid_edges = 0;
  bool out_single_duplicate = false;
  bool in_single_duplicate = false;
};

struct BulkEdgeCountConfig {
  bool concurrent = false;
  bool check_out_single = false;
  bool check_in_single = false;
};

constexpr size_t kBulkEdgeInitialGroupReserve = 4096;

size_t bulk_edge_group_reserve(size_t rows) {
  // Eagerly reserving one hash slot per edge wastes substantial memory for a
  // supernode. The map can grow past this cap for high-cardinality chunks, and
  // the worker-local scratch reuses that larger backing store on later chunks.
  return std::min(rows, kBulkEdgeInitialGroupReserve);
}

template <typename OutWriter, typename InWriter>
BulkEdgeCountChunkResult count_bulk_edge_chunk(
    const BulkEdgeEndpointScratch& endpoints, OutWriter& out, InWriter& in,
    BulkEdgeCountScratch& scratch, const BulkEdgeCountConfig& config) {
  CHECK_LE(endpoints.src_lids.size(),
           static_cast<size_t>(std::numeric_limits<uint32_t>::max()));
  BulkEdgeCountChunkResult result;

  if (config.concurrent) {
    if constexpr (OutWriter::kNeedsDegreeCount) {
      scratch.out_counts.clear();
      scratch.out_counts.reserve(
          bulk_edge_group_reserve(endpoints.src_lids.size()));
    }
    if constexpr (InWriter::kNeedsDegreeCount) {
      scratch.in_counts.clear();
      scratch.in_counts.reserve(
          bulk_edge_group_reserve(endpoints.dst_lids.size()));
    }
  }
  for (size_t row = 0; row < endpoints.src_lids.size(); ++row) {
    const auto src = endpoints.src_lids[row];
    const auto dst = endpoints.dst_lids[row];
    if (!is_valid_bulk_edge(src, dst)) {
      continue;
    }
    ++result.valid_edges;
    if (config.concurrent) {
      if constexpr (OutWriter::kNeedsDegreeCount) {
        ++scratch.out_counts[src];
      }
      if constexpr (InWriter::kNeedsDegreeCount) {
        ++scratch.in_counts[dst];
      }
      if constexpr (OutWriter::kChecksSingleUniqueness) {
        if (config.check_out_single && !scratch.out_single_duplicate) {
          ++result.out_single_checks;
          scratch.out_single_duplicate = out.CheckUniqueConcurrent(src);
        }
      }
      if constexpr (InWriter::kChecksSingleUniqueness) {
        if (config.check_in_single && !scratch.in_single_duplicate) {
          ++result.in_single_checks;
          scratch.in_single_duplicate = in.CheckUniqueConcurrent(dst);
        }
      }
    } else {
      if constexpr (OutWriter::kNeedsDegreeCount) {
        out.CountSerial(src);
      }
      if constexpr (InWriter::kNeedsDegreeCount) {
        in.CountSerial(dst);
      }
      if constexpr (OutWriter::kChecksSingleUniqueness) {
        if (config.check_out_single && !scratch.out_single_duplicate) {
          ++result.out_single_checks;
          scratch.out_single_duplicate = out.CheckUniqueSerial(src);
        }
      }
      if constexpr (InWriter::kChecksSingleUniqueness) {
        if (config.check_in_single && !scratch.in_single_duplicate) {
          ++result.in_single_checks;
          scratch.in_single_duplicate = in.CheckUniqueSerial(dst);
        }
      }
    }
  }
  if (config.concurrent) {
    if constexpr (OutWriter::kNeedsDegreeCount) {
      for (const auto& [src, count] : scratch.out_counts) {
        CHECK_LE(count, static_cast<uint32_t>(std::numeric_limits<int>::max()));
        out.CountConcurrent(src, static_cast<int>(count));
      }
    }
    if constexpr (InWriter::kNeedsDegreeCount) {
      for (const auto& [dst, count] : scratch.in_counts) {
        CHECK_LE(count, static_cast<uint32_t>(std::numeric_limits<int>::max()));
        in.CountConcurrent(dst, static_cast<int>(count));
      }
    }
  }
  return result;
}

BulkEdgeCountProfile profile_bulk_edge_count(
    const BulkEdgeCountScratch& scratch,
    const BulkEdgeCountChunkResult& chunk_result, bool concurrent_count,
    bool counts_out_degree, bool counts_in_degree) {
  BulkEdgeCountProfile result;
  if (counts_out_degree) {
    result.out_updates =
        concurrent_count ? scratch.out_counts.size() : chunk_result.valid_edges;
  }
  if (counts_in_degree) {
    result.in_updates =
        concurrent_count ? scratch.in_counts.size() : chunk_result.valid_edges;
  }
  return result;
}

ChunkSourceOptions make_bulk_edge_source_options(
    const ChunkPipelineAllocation& allocation, bool preserve_order) {
  ChunkSourceOptions source_options;
  source_options.parallel_enabled = allocation.parallel_enabled;
  source_options.producer_count = allocation.producer_count;
  source_options.queue_capacity = allocation.queue_capacity;
  source_options.preserve_order = preserve_order;
  return source_options;
}

struct BulkEdgeCountWorkerStats {
  size_t chunks = 0;
  size_t edges = 0;
  size_t out_updates = 0;
  size_t in_updates = 0;
  size_t out_single_checks = 0;
  size_t in_single_checks = 0;
  int64_t work_time_ns = 0;
};

using BulkEdgeCountChunk = std::function<BulkEdgeCountChunkResult(
    BulkEdgeCountScratch&, const BulkEdgeCountConfig&)>;

BulkEdgeCountSummary count_bulk_edges(
    const std::shared_ptr<IDataChunkSource>& source,
    const IndexerType& src_indexer, const IndexerType& dst_indexer,
    const ChunkPipelineAllocation& allocation, bool counts_out_degree,
    bool counts_in_degree, bool check_out_single, bool check_in_single,
    const BulkEdgeCountChunk& count_chunk) {
  // Degree accumulation and single-slot uniqueness checks are commutative, so
  // this pass never needs input order even when fill may later fall back to the
  // ordered last-write-wins path.
  auto source_options = make_bulk_edge_source_options(allocation, false);
  // The degree pass only needs endpoint OIDs. Push this projection through
  // ProjectingChunkSource into CSVChunkSource so edge properties are not typed,
  // allocated, and discarded during the first parse.
  source_options.projected_columns = {0, 1};
  auto supplier = source->Open(source_options);
  CHECK(supplier != nullptr);

  const bool profile_stages = VLOG_IS_ON(1);
  std::chrono::steady_clock::time_point row_count_start;
  std::chrono::steady_clock::time_point row_count_end;
  int64_t row_num = 0;
  if (profile_stages) {
    row_count_start = std::chrono::steady_clock::now();
    row_num = supplier->RowNum();
    row_count_end = std::chrono::steady_clock::now();
  }
  const auto worker_count = allocation.consumer_count;
  std::vector<BulkEdgeCountScratch> scratches(
      static_cast<size_t>(worker_count));
  std::vector<BulkEdgeCountWorkerStats> worker_stats;
  if (profile_stages) {
    worker_stats.resize(static_cast<size_t>(worker_count));
  }
  const BulkEdgeCountConfig count_config{
      .concurrent = worker_count > 1,
      .check_out_single = check_out_single,
      .check_in_single = check_in_single,
  };
  auto count = [&](int32_t worker, const std::shared_ptr<DataChunk>& chunk) {
    CHECK_GE(worker, 0);
    CHECK_LT(worker, worker_count);
    const auto start = profile_stages ? std::chrono::steady_clock::now()
                                      : std::chrono::steady_clock::time_point{};
    auto& scratch = scratches[static_cast<size_t>(worker)];
    index_bulk_edge_endpoints(chunk, src_indexer, dst_indexer,
                              scratch.endpoints);
    const auto chunk_result = count_chunk(scratch, count_config);
    scratch.valid_edges += static_cast<uint64_t>(chunk_result.valid_edges);
    if (profile_stages) {
      const auto work_end = std::chrono::steady_clock::now();
      const auto profile = profile_bulk_edge_count(
          scratch, chunk_result, count_config.concurrent, counts_out_degree,
          counts_in_degree);
      auto& stats = worker_stats[static_cast<size_t>(worker)];
      ++stats.chunks;
      stats.edges += chunk_result.valid_edges;
      stats.out_updates += profile.out_updates;
      stats.in_updates += profile.in_updates;
      stats.out_single_checks += chunk_result.out_single_checks;
      stats.in_single_checks += chunk_result.in_single_checks;
      stats.work_time_ns +=
          std::chrono::duration_cast<std::chrono::nanoseconds>(work_end - start)
              .count();
    }
  };
  VLOG(1) << "Bulk edge count pass: bytes=" << source->EstimatedBytes()
          << ", producers=" << allocation.producer_count
          << ", consumers=" << allocation.consumer_count
          << ", queue_capacity=" << allocation.queue_capacity
          << ", check_out_single=" << check_out_single
          << ", check_in_single=" << check_in_single
          << ", direct_supplier_queue="
          << supplier->SupportsConcurrentGetNext();
  const auto consume_start = profile_stages
                                 ? std::chrono::steady_clock::now()
                                 : std::chrono::steady_clock::time_point{};
  if (supplier->SupportsConcurrentGetNext()) {
    consume_concurrent_supplier_indexed(*supplier, worker_count, count);
  } else if (worker_count == 1) {
    while (auto chunk = supplier->GetNextChunk()) {
      count(0, chunk);
    }
  } else {
    ChunkPipelineOptions options;
    options.consumer_count = worker_count;
    options.queue_capacity = allocation.queue_capacity;
    consume_chunk_pipeline_indexed(*supplier, options, count);
  }
  const auto consume_end = profile_stages
                               ? std::chrono::steady_clock::now()
                               : std::chrono::steady_clock::time_point{};
  BulkEdgeCountSummary summary;
  for (size_t worker = 0; worker < scratches.size(); ++worker) {
    summary.valid_edges += scratches[worker].valid_edges;
    summary.out_single_duplicate =
        summary.out_single_duplicate || scratches[worker].out_single_duplicate;
    summary.in_single_duplicate =
        summary.in_single_duplicate || scratches[worker].in_single_duplicate;
  }
  if (!profile_stages) {
    return summary;
  }
  size_t out_updates = 0;
  size_t in_updates = 0;
  size_t out_single_checks = 0;
  size_t in_single_checks = 0;
  int64_t vid_degree_time_ns = 0;
  for (const auto& stats : worker_stats) {
    out_updates += stats.out_updates;
    in_updates += stats.in_updates;
    out_single_checks += stats.out_single_checks;
    in_single_checks += stats.in_single_checks;
    vid_degree_time_ns += stats.work_time_ns;
  }
  for (size_t worker = 0; worker < worker_stats.size(); ++worker) {
    const auto& stats = worker_stats[worker];
    VLOG(2) << "Bulk edge count worker: worker=" << worker
            << ", chunks=" << stats.chunks << ", edges=" << stats.edges
            << ", out_atomic_updates=" << stats.out_updates
            << ", in_atomic_updates=" << stats.in_updates
            << ", out_single_checks=" << stats.out_single_checks
            << ", in_single_checks=" << stats.in_single_checks
            << ", worker_ms=" << stats.work_time_ns / 1000000;
  }
  VLOG(1) << "Bulk edge count stages: rows=" << row_num << ", row_count_ms="
          << std::chrono::duration_cast<std::chrono::milliseconds>(
                 row_count_end - row_count_start)
                 .count()
          << ", pipeline_wall_ms="
          << std::chrono::duration_cast<std::chrono::milliseconds>(
                 consume_end - consume_start)
                 .count()
          << ", valid_edges=" << summary.valid_edges
          << ", out_atomic_updates=" << out_updates
          << ", in_atomic_updates=" << in_updates
          << ", out_single_checks=" << out_single_checks
          << ", in_single_checks=" << in_single_checks
          << ", out_single_duplicate=" << summary.out_single_duplicate
          << ", in_single_duplicate=" << summary.in_single_duplicate
          << ", vid_degree_worker_ms=" << vid_degree_time_ns / 1000000;
  return summary;
}

constexpr uint32_t kInvalidBulkEdgeIndex = std::numeric_limits<uint32_t>::max();

struct BulkEdgeGroup {
  uint32_t count = 0;
  uint32_t head = kInvalidBulkEdgeIndex;
  uint32_t tail = kInvalidBulkEdgeIndex;
};

struct BulkEdgeFillScratch {
  BulkEdgeEndpointScratch endpoints;
  flat_hash_map<vid_t, BulkEdgeGroup> out_groups;
  flat_hash_map<vid_t, BulkEdgeGroup> in_groups;
  std::vector<uint32_t> out_next;
  std::vector<uint32_t> in_next;
};

struct BulkEdgeFillResult {
  size_t valid_edges = 0;
  size_t out_reservations = 0;
  size_t in_reservations = 0;
};

void append_bulk_edge_group(flat_hash_map<vid_t, BulkEdgeGroup>& groups,
                            std::vector<uint32_t>& next, vid_t vertex,
                            uint32_t edge_index) {
  auto [it, inserted] = groups.try_emplace(vertex);
  auto& group = it->second;
  if (inserted) {
    group.head = edge_index;
  } else {
    CHECK_NE(group.tail, kInvalidBulkEdgeIndex);
    next[group.tail] = edge_index;
  }
  group.tail = edge_index;
  CHECK_LT(group.count, std::numeric_limits<uint32_t>::max());
  ++group.count;
}

template <typename OutWriter, typename InWriter>
void group_bulk_edge_chunk(const BulkEdgeEndpointScratch& endpoints,
                           BulkEdgeFillScratch& scratch) {
  CHECK_LE(endpoints.src_lids.size(),
           static_cast<size_t>(std::numeric_limits<uint32_t>::max()));
  if constexpr (OutWriter::kNeedsConcurrentGrouping) {
    scratch.out_groups.clear();
    scratch.out_groups.reserve(
        bulk_edge_group_reserve(endpoints.src_lids.size()));
    scratch.out_next.assign(endpoints.src_lids.size(), kInvalidBulkEdgeIndex);
  }
  if constexpr (InWriter::kNeedsConcurrentGrouping) {
    scratch.in_groups.clear();
    scratch.in_groups.reserve(
        bulk_edge_group_reserve(endpoints.dst_lids.size()));
    scratch.in_next.assign(endpoints.dst_lids.size(), kInvalidBulkEdgeIndex);
  }
  for (size_t row = 0; row < endpoints.src_lids.size(); ++row) {
    const auto src = endpoints.src_lids[row];
    const auto dst = endpoints.dst_lids[row];
    if (!is_valid_bulk_edge(src, dst)) {
      continue;
    }
    const auto edge_index = static_cast<uint32_t>(row);
    if constexpr (OutWriter::kNeedsConcurrentGrouping) {
      append_bulk_edge_group(scratch.out_groups, scratch.out_next, src,
                             edge_index);
    }
    if constexpr (InWriter::kNeedsConcurrentGrouping) {
      append_bulk_edge_group(scratch.in_groups, scratch.in_next, dst,
                             edge_index);
    }
  }
}

template <typename EDATA_T, typename OutWriter, typename InWriter>
void fill_bulk_edge_chunk_serial(const BulkEdgeEndpointScratch& endpoints,
                                 const BulkEdgeDataReader<EDATA_T>& data_reader,
                                 OutWriter& out, InWriter& in) {
  size_t valid_edges = 0;
  for (size_t row = 0; row < endpoints.src_lids.size(); ++row) {
    const auto src = endpoints.src_lids[row];
    const auto dst = endpoints.dst_lids[row];
    if (!is_valid_bulk_edge(src, dst)) {
      continue;
    }
    ++valid_edges;
    const auto data = data_reader.Get(row);
    if constexpr (OutWriter::kStoresEdges) {
      out.PutSerial(src, dst, data, 0);
    }
    if constexpr (InWriter::kStoresEdges) {
      in.PutSerial(dst, src, data, 0);
    }
  }
  if constexpr (OutWriter::kTracksInputEdgeCount) {
    out.RecordFilledEdges(valid_edges);
  }
  if constexpr (InWriter::kTracksInputEdgeCount) {
    in.RecordFilledEdges(valid_edges);
  }
}

template <typename EDATA_T, typename OutWriter, typename InWriter>
void fill_bulk_edge_chunk_concurrent(
    const BulkEdgeEndpointScratch& endpoints,
    const BulkEdgeDataReader<EDATA_T>& data_reader, OutWriter& out,
    InWriter& in, BulkEdgeFillScratch& scratch) {
  group_bulk_edge_chunk<OutWriter, InWriter>(endpoints, scratch);
  constexpr bool kDirectOut =
      OutWriter::kStoresEdges && !OutWriter::kNeedsConcurrentGrouping;
  constexpr bool kDirectIn =
      InWriter::kStoresEdges && !InWriter::kNeedsConcurrentGrouping;
  if constexpr (kDirectOut || kDirectIn) {
    size_t valid_edges = 0;
    for (size_t row = 0; row < endpoints.src_lids.size(); ++row) {
      const auto src = endpoints.src_lids[row];
      const auto dst = endpoints.dst_lids[row];
      if (!is_valid_bulk_edge(src, dst)) {
        continue;
      }
      ++valid_edges;
      const auto data = data_reader.Get(row);
      if constexpr (kDirectOut) {
        out.PutConcurrent(src, dst, data, 0);
      }
      if constexpr (kDirectIn) {
        in.PutConcurrent(dst, src, data, 0);
      }
    }
    if constexpr (OutWriter::kTracksInputEdgeCount) {
      out.RecordFilledEdges(valid_edges);
    }
    if constexpr (InWriter::kTracksInputEdgeCount) {
      in.RecordFilledEdges(valid_edges);
    }
  }
  if constexpr (OutWriter::kNeedsConcurrentGrouping) {
    for (const auto& [src, group] : scratch.out_groups) {
      CHECK_LE(group.count,
               static_cast<uint32_t>(std::numeric_limits<int>::max()));
      auto slot = out.ReserveConcurrent(src, static_cast<int>(group.count));
      auto edge_index = group.head;
      for (uint32_t i = 0; i < group.count; ++i) {
        CHECK_NE(edge_index, kInvalidBulkEdgeIndex);
        const auto data = data_reader.Get(edge_index);
        out.PutAt(src, slot++, endpoints.dst_lids[edge_index], data, 0);
        edge_index = scratch.out_next[edge_index];
      }
      CHECK_EQ(edge_index, kInvalidBulkEdgeIndex);
    }
  }
  if constexpr (InWriter::kNeedsConcurrentGrouping) {
    for (const auto& [dst, group] : scratch.in_groups) {
      CHECK_LE(group.count,
               static_cast<uint32_t>(std::numeric_limits<int>::max()));
      auto slot = in.ReserveConcurrent(dst, static_cast<int>(group.count));
      auto edge_index = group.head;
      for (uint32_t i = 0; i < group.count; ++i) {
        CHECK_NE(edge_index, kInvalidBulkEdgeIndex);
        const auto data = data_reader.Get(edge_index);
        in.PutAt(dst, slot++, endpoints.src_lids[edge_index], data, 0);
        edge_index = scratch.in_next[edge_index];
      }
      CHECK_EQ(edge_index, kInvalidBulkEdgeIndex);
    }
  }
}

BulkEdgeFillResult profile_bulk_edge_fill(
    const BulkEdgeEndpointScratch& endpoints,
    const BulkEdgeFillScratch* concurrent_scratch, bool stores_out_direction,
    bool stores_in_direction, bool groups_out_direction,
    bool groups_in_direction) {
  BulkEdgeFillResult result;
  result.valid_edges = count_valid_bulk_edges(endpoints);
  if (stores_out_direction) {
    result.out_reservations =
        concurrent_scratch == nullptr
            ? result.valid_edges
            : (groups_out_direction ? concurrent_scratch->out_groups.size()
                                    : 0);
  }
  if (stores_in_direction) {
    result.in_reservations =
        concurrent_scratch == nullptr
            ? result.valid_edges
            : (groups_in_direction ? concurrent_scratch->in_groups.size() : 0);
  }
  return result;
}

template <typename EDATA_T>
using BulkEdgeSerialFillChunk = std::function<void(
    const BulkEdgeEndpointScratch&, const BulkEdgeDataReader<EDATA_T>&)>;

template <typename EDATA_T>
using BulkEdgeConcurrentFillChunk = std::function<void(
    const BulkEdgeEndpointScratch&, const BulkEdgeDataReader<EDATA_T>&,
    BulkEdgeFillScratch&)>;

template <typename EDATA_T>
struct BulkEdgeBuildOps {
  bool stores_out_direction = false;
  bool stores_in_direction = false;
  bool counts_out_degree = false;
  bool counts_in_degree = false;
  bool checks_out_single = false;
  bool checks_in_single = false;
  bool groups_out_direction = false;
  bool groups_in_direction = false;
  bool supports_disjoint_concurrent_fill = false;
  std::function<void(vid_t, vid_t)> prepare_build;
  BulkEdgeCountChunk count_chunk;
  std::function<void(uint64_t)> set_input_edge_count;
  std::function<void()> allocate_from_counts;
  BulkEdgeSerialFillChunk<EDATA_T> fill_serial_chunk;
  BulkEdgeConcurrentFillChunk<EDATA_T> fill_concurrent_chunk;
  std::function<void()> finish;
};

template <typename EDATA_T, typename OutWriter, typename InWriter>
BulkEdgeBuildOps<EDATA_T> make_bulk_edge_build_ops(OutWriter& out,
                                                   InWriter& in) {
  BulkEdgeBuildOps<EDATA_T> ops;
  constexpr bool kStoresAnyDirection =
      OutWriter::kStoresEdges || InWriter::kStoresEdges;
  constexpr bool kNeedsAnyDegreeCount =
      OutWriter::kNeedsDegreeCount || InWriter::kNeedsDegreeCount;
  constexpr bool kSupportsCountPass = kNeedsAnyDegreeCount ||
                                      OutWriter::kChecksSingleUniqueness ||
                                      InWriter::kChecksSingleUniqueness;
  ops.stores_out_direction = OutWriter::kStoresEdges;
  ops.stores_in_direction = InWriter::kStoresEdges;
  ops.counts_out_degree = OutWriter::kNeedsDegreeCount;
  ops.counts_in_degree = InWriter::kNeedsDegreeCount;
  ops.checks_out_single = OutWriter::kChecksSingleUniqueness;
  ops.checks_in_single = InWriter::kChecksSingleUniqueness;
  ops.groups_out_direction = OutWriter::kNeedsConcurrentGrouping;
  ops.groups_in_direction = InWriter::kNeedsConcurrentGrouping;
  ops.prepare_build = [&out, &in](vid_t out_vertices, vid_t in_vertices) {
    out.PrepareBuild(out_vertices);
    in.PrepareBuild(in_vertices);
  };
  ops.allocate_from_counts = [&out, &in]() {
    out.AllocateFromCounts();
    in.AllocateFromCounts();
  };
  if constexpr (kSupportsCountPass) {
    ops.count_chunk = [&out, &in](BulkEdgeCountScratch& scratch,
                                  const BulkEdgeCountConfig& config) {
      return count_bulk_edge_chunk(scratch.endpoints, out, in, scratch, config);
    };
  }
  if constexpr (OutWriter::kTracksInputEdgeCount &&
                InWriter::kTracksInputEdgeCount) {
    ops.set_input_edge_count = [&out, &in](uint64_t count) {
      out.SetInputEdgeCount(count);
      in.SetInputEdgeCount(count);
    };
  } else if constexpr (OutWriter::kTracksInputEdgeCount) {
    ops.set_input_edge_count = [&out](uint64_t count) {
      out.SetInputEdgeCount(count);
    };
  } else if constexpr (InWriter::kTracksInputEdgeCount) {
    ops.set_input_edge_count = [&in](uint64_t count) {
      in.SetInputEdgeCount(count);
    };
  }
  if constexpr (kStoresAnyDirection) {
    ops.supports_disjoint_concurrent_fill =
        OutWriter::kSupportsDisjointConcurrentFill &&
        InWriter::kSupportsDisjointConcurrentFill;
    ops.fill_serial_chunk =
        [&out, &in](const BulkEdgeEndpointScratch& endpoints,
                    const BulkEdgeDataReader<EDATA_T>& data_reader) {
          fill_bulk_edge_chunk_serial(endpoints, data_reader, out, in);
        };
    if constexpr (OutWriter::kSupportsDisjointConcurrentFill &&
                  InWriter::kSupportsDisjointConcurrentFill) {
      ops.fill_concurrent_chunk =
          [&out, &in](const BulkEdgeEndpointScratch& endpoints,
                      const BulkEdgeDataReader<EDATA_T>& data_reader,
                      BulkEdgeFillScratch& scratch) {
            fill_bulk_edge_chunk_concurrent(endpoints, data_reader, out, in,
                                            scratch);
          };
    }
  }
  ops.finish = [&out, &in]() {
    out.Finish();
    in.Finish();
  };
  return ops;
}

struct BulkEdgeFillWorkerStats {
  size_t chunks = 0;
  size_t edges = 0;
  size_t out_reservations = 0;
  size_t in_reservations = 0;
  int64_t endpoint_time_ns = 0;
  int64_t fill_time_ns = 0;
};

template <typename EDATA_T>
void fill_bulk_edges(const std::shared_ptr<IDataChunkSource>& source,
                     const IndexerType& src_indexer,
                     const IndexerType& dst_indexer,
                     const ChunkPipelineAllocation& allocation,
                     bool preserve_order, bool allow_concurrent_fill,
                     const BulkEdgeBuildOps<EDATA_T>& ops) {
  CHECK(!allow_concurrent_fill || ops.supports_disjoint_concurrent_fill);
  CHECK(!allow_concurrent_fill || !preserve_order);
  const auto source_options =
      make_bulk_edge_source_options(allocation, preserve_order);
  auto supplier = source->Open(source_options);
  CHECK(supplier != nullptr);

  const bool profile_stages = VLOG_IS_ON(1);
  const auto pipeline_start = profile_stages
                                  ? std::chrono::steady_clock::now()
                                  : std::chrono::steady_clock::time_point{};
  const auto worker_count = allocation.consumer_count;
  std::vector<BulkEdgeFillWorkerStats> worker_stats;
  if (profile_stages) {
    worker_stats.resize(static_cast<size_t>(worker_count));
  }
  bool concurrent_fill = false;

  if (allow_concurrent_fill) {
    if (worker_count > 1) {
      CHECK(static_cast<bool>(ops.fill_concurrent_chunk));
      concurrent_fill = true;
      std::vector<BulkEdgeFillScratch> scratches(
          static_cast<size_t>(worker_count));
      auto fill = [&](int32_t worker, const std::shared_ptr<DataChunk>& chunk) {
        CHECK_GE(worker, 0);
        CHECK_LT(worker, worker_count);
        auto& scratch = scratches[static_cast<size_t>(worker)];
        const auto endpoint_start =
            profile_stages ? std::chrono::steady_clock::now()
                           : std::chrono::steady_clock::time_point{};
        index_bulk_edge_endpoints(chunk, src_indexer, dst_indexer,
                                  scratch.endpoints);
        if (profile_stages) {
          worker_stats[static_cast<size_t>(worker)].endpoint_time_ns +=
              std::chrono::duration_cast<std::chrono::nanoseconds>(
                  std::chrono::steady_clock::now() - endpoint_start)
                  .count();
        }
        const auto fill_start = profile_stages
                                    ? std::chrono::steady_clock::now()
                                    : std::chrono::steady_clock::time_point{};
        const auto data_column = chunk->col_num() > 2 ? chunk->get(2) : nullptr;
        BulkEdgeDataReader<EDATA_T> data_reader(data_column);
        ops.fill_concurrent_chunk(scratch.endpoints, data_reader, scratch);
        if (profile_stages) {
          const auto fill_end = std::chrono::steady_clock::now();
          const auto result = profile_bulk_edge_fill(
              scratch.endpoints, &scratch, ops.stores_out_direction,
              ops.stores_in_direction, ops.groups_out_direction,
              ops.groups_in_direction);
          auto& stats = worker_stats[static_cast<size_t>(worker)];
          ++stats.chunks;
          stats.edges += result.valid_edges;
          stats.out_reservations += result.out_reservations;
          stats.in_reservations += result.in_reservations;
          stats.fill_time_ns +=
              std::chrono::duration_cast<std::chrono::nanoseconds>(fill_end -
                                                                   fill_start)
                  .count();
        }
      };

      if (supplier->SupportsConcurrentGetNext()) {
        consume_concurrent_supplier_indexed(*supplier, worker_count, fill);
      } else {
        ChunkPipelineOptions options;
        options.consumer_count = worker_count;
        options.queue_capacity = allocation.queue_capacity;
        consume_chunk_pipeline_indexed(*supplier, options, fill);
      }
    }
  }

  if (!concurrent_fill) {
    BulkEdgeEndpointScratch endpoints;
    while (auto chunk = supplier->GetNextChunk()) {
      const auto endpoint_start = profile_stages
                                      ? std::chrono::steady_clock::now()
                                      : std::chrono::steady_clock::time_point{};
      index_bulk_edge_endpoints(chunk, src_indexer, dst_indexer, endpoints);
      if (profile_stages) {
        worker_stats.front().endpoint_time_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - endpoint_start)
                .count();
      }
      const auto fill_start = profile_stages
                                  ? std::chrono::steady_clock::now()
                                  : std::chrono::steady_clock::time_point{};
      const auto data_column = chunk->col_num() > 2 ? chunk->get(2) : nullptr;
      BulkEdgeDataReader<EDATA_T> data_reader(data_column);
      ops.fill_serial_chunk(endpoints, data_reader);
      if (profile_stages) {
        const auto fill_end = std::chrono::steady_clock::now();
        const auto result = profile_bulk_edge_fill(
            endpoints, nullptr, ops.stores_out_direction,
            ops.stores_in_direction, ops.groups_out_direction,
            ops.groups_in_direction);
        auto& stats = worker_stats.front();
        ++stats.chunks;
        stats.edges += result.valid_edges;
        stats.out_reservations += result.out_reservations;
        stats.in_reservations += result.in_reservations;
        stats.fill_time_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(fill_end -
                                                                 fill_start)
                .count();
      }
    }
  }

  const auto pipeline_end = profile_stages
                                ? std::chrono::steady_clock::now()
                                : std::chrono::steady_clock::time_point{};
  if (!profile_stages) {
    return;
  }
  const auto pipeline_wall_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(pipeline_end -
                                                            pipeline_start)
          .count();
  size_t total_edges = 0;
  size_t total_chunks = 0;
  size_t out_reservations = 0;
  size_t in_reservations = 0;
  size_t min_worker_edges = std::numeric_limits<size_t>::max();
  size_t max_worker_edges = 0;
  int64_t endpoint_time_ns = 0;
  int64_t fill_time_ns = 0;
  for (const auto& stats : worker_stats) {
    total_edges += stats.edges;
    total_chunks += stats.chunks;
    out_reservations += stats.out_reservations;
    in_reservations += stats.in_reservations;
    min_worker_edges = std::min(min_worker_edges, stats.edges);
    max_worker_edges = std::max(max_worker_edges, stats.edges);
    endpoint_time_ns += stats.endpoint_time_ns;
    fill_time_ns += stats.fill_time_ns;
  }
  for (size_t worker = 0; worker < worker_stats.size(); ++worker) {
    const auto& stats = worker_stats[worker];
    VLOG(2) << "Bulk edge fill worker: worker=" << worker
            << ", chunks=" << stats.chunks << ", edges=" << stats.edges
            << ", out_range_reservations=" << stats.out_reservations
            << ", in_range_reservations=" << stats.in_reservations
            << ", endpoint_index_ms=" << stats.endpoint_time_ns / 1000000
            << ", fill_ms=" << stats.fill_time_ns / 1000000;
  }
  VLOG(1) << "Bulk edge fill stages: pipeline_wall_ms=" << pipeline_wall_ms
          << ", concurrent_fill=" << concurrent_fill
          << ", fill_workers=" << (concurrent_fill ? worker_count : 1)
          << ", chunks=" << total_chunks << ", edges=" << total_edges
          << ", out_range_reservations=" << out_reservations
          << ", in_range_reservations=" << in_reservations
          << ", min_worker_edges=" << min_worker_edges
          << ", max_worker_edges=" << max_worker_edges
          << ", endpoint_index_worker_ms=" << endpoint_time_ns / 1000000
          << ", csr_fill_worker_ms=" << fill_time_ns / 1000000;
}

vid_t csr_vertex_capacity(const IndexerType& indexer) {
  const size_t capacity = indexer.capacity();
  CHECK_LE(capacity, static_cast<size_t>(std::numeric_limits<vid_t>::max()))
      << "CSR vertex capacity exceeds the vertex id range";
  return static_cast<vid_t>(capacity);
}

template <typename EDATA_T>
void build_bundled_edges_with_ops(
    const BulkEdgeBuildOps<EDATA_T>& ops, const IndexerType& src_indexer,
    const IndexerType& dst_indexer,
    const std::shared_ptr<IDataChunkSource>& source) {
  // VertexTable owns its reserve policy. Edge storage consumes the resulting
  // indexer capacity instead of duplicating PropertyGraph::Dump's policy.
  ops.prepare_build(csr_vertex_capacity(src_indexer),
                    csr_vertex_capacity(dst_indexer));
  if (!ops.stores_out_direction && !ops.stores_in_direction) {
    ops.allocate_from_counts();
    ops.finish();
    return;
  }
  const auto estimated_bytes = source->EstimatedBytes();
  const auto count_allocation = resolve_chunk_pipeline_allocation(
      estimated_bytes, source->ParallelEnabled(), false);
  const bool needs_degree_count = ops.counts_out_degree || ops.counts_in_degree;
  const bool has_single_direction =
      ops.checks_out_single || ops.checks_in_single;
  // Single-only layouts retain their one-pass ordered path. Mixed
  // Single/Mutable layouts already need the degree pass, so that pass also
  // verifies whether fixed Single slots are disjoint across all chunks.
  const bool check_single_uniqueness =
      has_single_direction && needs_degree_count;
  const bool run_count_pass = needs_degree_count || check_single_uniqueness;
  VLOG(1) << "Bulk edge count allocation: bytes=" << estimated_bytes
          << ", producers=" << count_allocation.producer_count
          << ", consumers=" << count_allocation.consumer_count
          << ", queue_capacity=" << count_allocation.queue_capacity
          << ", degree_count_pass=" << needs_degree_count
          << ", single_uniqueness_check=" << check_single_uniqueness
          << ", count_pass=" << run_count_pass;
  BulkEdgeCountSummary count_summary;
  if (run_count_pass) {
    CHECK(static_cast<bool>(ops.count_chunk));
    count_summary = count_bulk_edges(
        source, src_indexer, dst_indexer, count_allocation,
        ops.counts_out_degree, ops.counts_in_degree,
        check_single_uniqueness && ops.checks_out_single,
        check_single_uniqueness && ops.checks_in_single, ops.count_chunk);
  }
  const bool single_uniqueness_verified =
      has_single_direction && check_single_uniqueness;
  if (single_uniqueness_verified) {
    CHECK(static_cast<bool>(ops.set_input_edge_count));
    ops.set_input_edge_count(count_summary.valid_edges);
  }
  const bool single_duplicate =
      (ops.checks_out_single && count_summary.out_single_duplicate) ||
      (ops.checks_in_single && count_summary.in_single_duplicate);
  const bool single_slots_disjoint =
      !has_single_direction ||
      (single_uniqueness_verified && !single_duplicate);
  const bool allow_concurrent_fill =
      ops.supports_disjoint_concurrent_fill && single_slots_disjoint;
  const bool preserve_fill_order =
      has_single_direction && !single_slots_disjoint;
  const auto fill_allocation =
      preserve_fill_order
          ? resolve_chunk_pipeline_allocation(estimated_bytes,
                                              source->ParallelEnabled(), true)
          : count_allocation;
  VLOG(1) << "Bulk edge fill allocation: bytes=" << estimated_bytes
          << ", producers=" << fill_allocation.producer_count
          << ", consumers=" << fill_allocation.consumer_count
          << ", queue_capacity=" << fill_allocation.queue_capacity
          << ", single_uniqueness_verified=" << single_uniqueness_verified
          << ", out_single_duplicate=" << count_summary.out_single_duplicate
          << ", in_single_duplicate=" << count_summary.in_single_duplicate
          << ", allow_concurrent_fill=" << allow_concurrent_fill
          << ", preserve_order=" << preserve_fill_order;
  const bool profile_stages = VLOG_IS_ON(1);
  const auto allocate_start = profile_stages
                                  ? std::chrono::steady_clock::now()
                                  : std::chrono::steady_clock::time_point{};
  ops.allocate_from_counts();
  const auto fill_start = profile_stages
                              ? std::chrono::steady_clock::now()
                              : std::chrono::steady_clock::time_point{};
  fill_bulk_edges<EDATA_T>(source, src_indexer, dst_indexer, fill_allocation,
                           preserve_fill_order, allow_concurrent_fill, ops);
  ops.finish();
  if (!profile_stages) {
    return;
  }
  const auto fill_end = std::chrono::steady_clock::now();
  VLOG(1) << "Bulk edge CSR stages: allocate_ms="
          << std::chrono::duration_cast<std::chrono::milliseconds>(
                 fill_start - allocate_start)
                 .count()
          << ", fill_pass_ms="
          << std::chrono::duration_cast<std::chrono::milliseconds>(fill_end -
                                                                   fill_start)
                 .count();
}

template <typename EDATA_T>
bool build_bundled_edges_typed(
    CsrBase* out_csr, CsrBase* in_csr, const IndexerType& src_indexer,
    const IndexerType& dst_indexer,
    const std::shared_ptr<IDataChunkSource>& source) {
  bool built = false;
  const bool out_supported =
      with_csr_bulk_writer<EDATA_T>(out_csr, [&](auto& out) {
        const bool in_supported =
            with_csr_bulk_writer<EDATA_T>(in_csr, [&](auto& in) {
              auto ops = make_bulk_edge_build_ops<EDATA_T>(out, in);
              build_bundled_edges_with_ops(ops, src_indexer, dst_indexer,
                                           source);
            });
        built = in_supported;
      });
  return out_supported && built;
}

bool build_bundled_edges(CsrBase* out_csr, CsrBase* in_csr,
                         const std::shared_ptr<const EdgeSchema>& schema,
                         const IndexerType& src_indexer,
                         const IndexerType& dst_indexer,
                         const std::shared_ptr<IDataChunkSource>& source) {
  const auto property_type = schema->properties.empty()
                                 ? DataTypeId::kEmpty
                                 : schema->properties[0].id();
  switch (property_type) {
#define TYPE_DISPATCHER(enum_val, cpp_type)                                  \
  case DataTypeId::enum_val:                                                 \
    return build_bundled_edges_typed<cpp_type>(out_csr, in_csr, src_indexer, \
                                               dst_indexer, source);
    FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kEmpty:
    return build_bundled_edges_typed<EmptyType>(out_csr, in_csr, src_indexer,
                                                dst_indexer, source);
  default:
    return false;
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
                                   int32_t col_id, const Value& prop,
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

void EdgeTable::AddProperties(Checkpoint& ckp,
                              const std::vector<std::string>& prop_names,
                              const std::vector<DataType>& prop_types,
                              const std::vector<Value>& default_values) {
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
    vid_t src_lid, vid_t dst_lid, const std::vector<Value>& edge_data,
    timestamp_t ts, Allocator& alloc, bool insert_safe) {
  return internal::insert_edge_into_csr_internal(
      *out_csr_, *in_csr_, *table_.get(), table_idx_, *meta_, src_lid, dst_lid,
      edge_data, ts, alloc, insert_safe);
}

void EdgeTable::BatchAddEdges(const IndexerType& src_indexer,
                              const IndexerType& dst_indexer,
                              std::shared_ptr<IDataChunkSupplier> supplier) {
  const bool profile_stages = VLOG_IS_ON(1);
  const auto total_start = std::chrono::steady_clock::now();
  int64_t row_num_time_ns = 0;
  int64_t collect_time_ns = 0;
  int64_t filter_time_ns = 0;
  int64_t ensure_capacity_time_ns = 0;
  int64_t write_time_ns = 0;
  size_t chunk_count = 0;
  size_t input_rows = 0;
  size_t cached_property_columns = 0;
  size_t peak_buffer_bytes = 0;
  const auto peak_rss_start_bytes =
      profile_stages ? process_peak_rss_bytes() : 0;
  // Keep fallback COPY paths aligned with the vertex table's actual capacity,
  // while leaving completely unloaded edge tables lazy until persistence.
  in_csr_->resize(csr_vertex_capacity(dst_indexer));
  out_csr_->resize(csr_vertex_capacity(src_indexer));
  std::vector<vid_t> src_lid, dst_lid;
  // Pre-reserve capacity to reduce vector reallocation on large graphs.
  const auto row_num_start = profile_stages
                                 ? std::chrono::steady_clock::now()
                                 : std::chrono::steady_clock::time_point{};
  auto total_rows = supplier->RowNum();
  if (profile_stages) {
    row_num_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::steady_clock::now() - row_num_start)
                          .count();
  }
  if (total_rows > 0) {
    src_lid.reserve(total_rows);
    dst_lid.reserve(total_rows);
  }
  // Collect per-property columns across chunks (for bundled: single column;
  // for unbundled: full property DataChunks).
  std::vector<std::shared_ptr<IContextColumn>> bundled_data_cols;
  std::vector<std::shared_ptr<DataChunk>> unbundled_data_chunks;
  std::vector<bool> valid_flags;
  while (true) {
    const auto collect_start = profile_stages
                                   ? std::chrono::steady_clock::now()
                                   : std::chrono::steady_clock::time_point{};
    auto chunk = supplier->GetNextChunk();
    if (chunk == nullptr) {
      break;
    }
    ++chunk_count;
    input_rows += chunk->row_num();
    auto src_col = chunk->get(0);
    auto dst_col = chunk->get(1);
    src_indexer.get_index(*src_col, src_lid);
    dst_indexer.get_index(*dst_col, dst_lid);
    if (chunk->col_num() > 2) {
      if (meta_->is_bundled()) {
        // Bundled: only one property column (index 2).
        bundled_data_cols.push_back(chunk->get(2));
        ++cached_property_columns;
      } else {
        // Unbundled: collect remaining columns as a DataChunk.
        auto prop_chunk = std::make_shared<DataChunk>();
        for (size_t i = 2; i < chunk->col_num(); ++i) {
          auto c = chunk->get(static_cast<int>(i));
          if (c) {
            prop_chunk->set(static_cast<int>(i - 2), c);
            ++cached_property_columns;
          }
        }
        unbundled_data_chunks.push_back(prop_chunk);
      }
    }
    if (profile_stages) {
      collect_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::steady_clock::now() - collect_start)
                             .count();
      peak_buffer_bytes = std::max(
          peak_buffer_bytes, estimate_edge_fallback_buffer_bytes(
                                 src_lid, dst_lid, valid_flags,
                                 bundled_data_cols, unbundled_data_chunks));
    }
  }
  const auto filter_start = profile_stages
                                ? std::chrono::steady_clock::now()
                                : std::chrono::steady_clock::time_point{};
  filterInvalidEdges(src_lid, dst_lid, valid_flags);
  if (profile_stages) {
    filter_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         std::chrono::steady_clock::now() - filter_start)
                         .count();
    peak_buffer_bytes = std::max(
        peak_buffer_bytes, estimate_edge_fallback_buffer_bytes(
                               src_lid, dst_lid, valid_flags, bundled_data_cols,
                               unbundled_data_chunks));
  }
  const auto valid_rows = src_lid.size();
  const auto invalid_rows = valid_flags.size() - valid_rows;
  const auto ensure_capacity_start =
      profile_stages ? std::chrono::steady_clock::now()
                     : std::chrono::steady_clock::time_point{};
  size_t new_size = table_idx_.load() + src_lid.size();
  if (new_size >= Capacity()) {
    auto new_cap = new_size;
    while (new_size >= new_cap) {
      new_cap = new_cap < 4096 ? 4096 : new_cap + (new_cap + 4) / 5;
    }
    EnsureCapacity(new_cap);
  }
  if (profile_stages) {
    ensure_capacity_time_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - ensure_capacity_start)
            .count();
  }
  const auto write_start = profile_stages
                               ? std::chrono::steady_clock::now()
                               : std::chrono::steady_clock::time_point{};
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
  if (profile_stages) {
    write_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - write_start)
                        .count();
    const auto total_time_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - total_start)
            .count();
    const auto peak_rss_end_bytes = process_peak_rss_bytes();
    VLOG(1) << "Fallback edge load stages: input_rows=" << input_rows
            << ", valid_rows=" << valid_rows
            << ", invalid_rows=" << invalid_rows << ", chunks=" << chunk_count
            << ", cached_property_columns=" << cached_property_columns
            << ", peak_buffer_bytes=" << peak_buffer_bytes
            << ", process_peak_rss_start_bytes=" << peak_rss_start_bytes
            << ", process_peak_rss_end_bytes=" << peak_rss_end_bytes
            << ", total_ms=" << total_time_ns / 1000000
            << ", row_num_ms=" << row_num_time_ns / 1000000
            << ", collect_lookup_ms=" << collect_time_ns / 1000000
            << ", filter_ms=" << filter_time_ns / 1000000
            << ", ensure_capacity_ms=" << ensure_capacity_time_ns / 1000000
            << ", write_ms=" << write_time_ns / 1000000;
  }
}

void EdgeTable::BatchBuildEdges(const IndexerType& src_indexer,
                                const IndexerType& dst_indexer,
                                std::shared_ptr<IDataChunkSource> source) {
  if (!source) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "BatchBuildEdges requires a non-null data source");
  }
  CHECK(source->rewindable());
  CHECK(CanBatchBuild())
      << "Bulk edge build requires an empty bundled edge table";

  EdgeTable staged(meta_);
  staged.Init(ckp_, memory_level_);
  CHECK(build_bundled_edges(staged.out_csr_.get(), staged.in_csr_.get(), meta_,
                            src_indexer, dst_indexer, source))
      << "Bulk edge build does not support this CSR layout";
  Swap(staged);
}

void EdgeTable::BatchAddEdges(
    const std::vector<vid_t>& src_lid_list,
    const std::vector<vid_t>& dst_lid_list,
    const std::vector<std::vector<Value>>& edge_data_list) {
  size_t new_size = table_idx_.load() + src_lid_list.size();
  if (new_size >= Capacity()) {
    auto new_cap = new_size;
    while (new_size >= new_cap) {
      new_cap = new_cap < 4096 ? 4096 : new_cap + (new_cap + 4) / 5;
    }
    EnsureCapacity(new_cap);
  }
  if (meta_->is_bundled()) {
    std::vector<Value> flat_edge_data;
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

void EdgeTable::Compact(const std::optional<std::string>& sort_key_for_nbr,
                        timestamp_t ts) {
  out_csr_->compact();
  in_csr_->compact();
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
    std::vector<Value> remaining_data;
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
