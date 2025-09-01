
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

#ifndef STORAGES_RT_MUTABLE_GRAPH_LOADER_ABSTRACT_ARROW_FRAGMENT_LOADER_H_
#define STORAGES_RT_MUTABLE_GRAPH_LOADER_ABSTRACT_ARROW_FRAGMENT_LOADER_H_

#include <arrow/api.h>
#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/array_primitive.h>
#include <glog/logging.h>
#include <stddef.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
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
#include <utility>
#include <variant>
#include <vector>

#include "libgrape-lite/grape/utils/concurrent_queue.h"
#include "neug/storages/csr/dual_csr.h"
#include "neug/storages/file_names.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/loader/basic_fragment_loader.h"
#include "neug/storages/loader/i_fragment_loader.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/storages/loader/loading_config.h"
#include "neug/utils/arrow_utils.h"
#include "neug/utils/id_indexer.h"
#include "neug/utils/indexers.h"
#include "neug/utils/mmap_vector.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"

namespace grape {
struct EmptyType;
}  // namespace grape

namespace gs {

template <typename T>
class mmap_vector;

class MutablePropertyFragment;

void printDiskRemaining(const std::string& path);
// The interface providing visitor pattern for RecordBatch.

bool check_primary_key_type(std::shared_ptr<arrow::DataType> data_type);

// For Primitive types.
template <typename COL_T>
void set_column(gs::ColumnBase* col, std::shared_ptr<arrow::ChunkedArray> array,
                const std::vector<size_t>& offset) {
  using arrow_array_type = typename gs::TypeConverter<COL_T>::ArrowArrayType;
  auto array_type = array->type();
  auto arrow_type = gs::TypeConverter<COL_T>::ArrowTypeValue();
  CHECK(array_type->Equals(arrow_type))
      << "Inconsistent data type, expect " << arrow_type->ToString()
      << ", but got " << array_type->ToString();
  size_t cur_ind = 0;
  for (auto j = 0; j < array->num_chunks(); ++j) {
    auto casted = std::static_pointer_cast<arrow_array_type>(array->chunk(j));
    size_t size = col->size();
    for (auto k = 0; k < casted->length(); ++k) {
      if (offset[cur_ind] >= size) {
        cur_ind++;
      } else {
        col->set_any(offset[cur_ind++],
                     std::move(AnyConverter<COL_T>::to_any(casted->Value(k))));
      }
    }
  }
}

// For String types.
void set_column_from_string_array(gs::ColumnBase* col,
                                  std::shared_ptr<arrow::ChunkedArray> array,
                                  const std::vector<size_t>& offset,
                                  bool enable_resize = false);

template <typename COL_T>  // COL_T = DateTime or Timestamp
void set_column_from_timestamp_array(gs::ColumnBase* col,
                                     std::shared_ptr<arrow::ChunkedArray> array,
                                     const std::vector<size_t>& offset) {
  auto type = array->type();
  auto col_type = col->type();
  auto size = col->size();
  size_t cur_ind = 0;
  if (type->Equals(arrow::timestamp(arrow::TimeUnit::type::MILLI))) {
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::TimestampArray>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        if (offset[cur_ind] >= size) {
          cur_ind++;
        } else {
          col->set_any(
              offset[cur_ind++],
              std::move(AnyConverter<COL_T>::to_any(casted->Value(k))));
        }
      }
    }
  } else {
    LOG(FATAL) << "Not implemented: converting " << type->ToString() << " to "
               << col_type;
  }
}

void set_interval_column_from_string_array(
    gs::ColumnBase* col, std::shared_ptr<arrow::ChunkedArray> array,
    const std::vector<size_t>& offset);

void set_column_from_date_array(gs::ColumnBase* col,
                                std::shared_ptr<arrow::ChunkedArray> array,
                                const std::vector<size_t>& offset);

void set_properties_column(gs::ColumnBase* col,
                           std::shared_ptr<arrow::ChunkedArray> array,
                           const std::vector<size_t>& offset);

void check_edge_invariant(
    const Schema& schema,
    const std::vector<std::tuple<size_t, std::string, std::string>>&
        column_mappings,
    size_t src_col_ind, size_t dst_col_ind, label_t src_label_i,
    label_t dst_label_i, label_t edge_label_i);

template <typename KEY_T>
struct _add_vertex {
  void operator()(const std::shared_ptr<arrow::Array>& col,
                  IdIndexer<KEY_T, vid_t>& indexer,
                  std::vector<size_t>& offset) {
    size_t row_num = col->length();
    vid_t vid;
    if constexpr (!std::is_same<std::string_view, KEY_T>::value) {
      // for non-string value
      auto expected_type = gs::TypeConverter<KEY_T>::ArrowTypeValue();
      using arrow_array_t = typename gs::TypeConverter<KEY_T>::ArrowArrayType;
      if (!col->type()->Equals(expected_type)) {
        LOG(FATAL) << "Inconsistent data type, expect "
                   << expected_type->ToString() << ", but got "
                   << col->type()->ToString();
      }
      auto casted_array = std::static_pointer_cast<arrow_array_t>(col);
      for (size_t i = 0; i < row_num; ++i) {
        if (!indexer.add(casted_array->Value(i), vid)) {
          VLOG(2) << "Duplicate vertex id: " << casted_array->Value(i) << "..";
          offset.emplace_back(std::numeric_limits<size_t>::max());
        } else {
          offset.emplace_back(vid);
        }
      }
    } else {
      if (col->type()->Equals(arrow::utf8())) {
        auto casted_array = std::static_pointer_cast<arrow::StringArray>(col);
        for (size_t i = 0; i < row_num; ++i) {
          auto str = casted_array->GetView(i);
          std::string_view str_view(str.data(), str.size());
          if (!indexer.add(str_view, vid)) {
            VLOG(2) << "Duplicate vertex id: " << str_view << "..";
            offset.emplace_back(std::numeric_limits<size_t>::max());
          } else {
            offset.emplace_back(vid);
          }
        }
      } else if (col->type()->Equals(arrow::large_utf8())) {
        auto casted_array =
            std::static_pointer_cast<arrow::LargeStringArray>(col);
        for (size_t i = 0; i < row_num; ++i) {
          auto str = casted_array->GetView(i);
          std::string_view str_view(str.data(), str.size());
          if (!indexer.add(str_view, vid)) {
            VLOG(2) << "Duplicate vertex id: " << str_view << "..";
            offset.emplace_back(std::numeric_limits<size_t>::max());
          } else {
            offset.emplace_back(vid);
          }
        }
      } else {
        LOG(FATAL) << "Not support type: " << col->type()->ToString();
      }
    }
  }
};

template <typename PK_T, typename EDATA_T, typename VECTOR_T>
void _append(bool is_dst, size_t cur_ind, std::shared_ptr<arrow::Array> col,
             const IndexerType& indexer, VECTOR_T& parsed_edges,
             std::vector<std::atomic<int32_t>>& degree) {
  static constexpr auto invalid_vid = std::numeric_limits<vid_t>::max();
  if constexpr (std::is_same_v<PK_T, std::string_view>) {
    if (col->type()->Equals(arrow::utf8())) {
      auto casted = std::static_pointer_cast<arrow::StringArray>(col);
      for (auto j = 0; j < casted->length(); ++j) {
        auto str = casted->GetView(j);
        std::string_view str_view(str.data(), str.size());
        auto vid = indexer.get_index(Any::From(str_view));
        if (is_dst) {
          std::get<1>(parsed_edges[cur_ind++]) = vid;
        } else {
          std::get<0>(parsed_edges[cur_ind++]) = vid;
        }
        if (vid != invalid_vid) {
          degree[vid]++;
        }
      }
    } else {
      // must be large utf8
      auto casted = std::static_pointer_cast<arrow::LargeStringArray>(col);
      for (auto j = 0; j < casted->length(); ++j) {
        auto str = casted->GetView(j);
        std::string_view str_view(str.data(), str.size());
        auto vid = indexer.get_index(Any::From(str_view));
        if (is_dst) {
          std::get<1>(parsed_edges[cur_ind++]) = vid;
        } else {
          std::get<0>(parsed_edges[cur_ind++]) = vid;
        }
        if (vid != invalid_vid) {
          degree[vid]++;
        }
      }
    }
  } else {
    using arrow_array_type = typename gs::TypeConverter<PK_T>::ArrowArrayType;
    auto casted = std::static_pointer_cast<arrow_array_type>(col);
    for (auto j = 0; j < casted->length(); ++j) {
      auto vid = indexer.get_index(Any::From(casted->Value(j)));
      if (is_dst) {
        std::get<1>(parsed_edges[cur_ind++]) = vid;
      } else {
        std::get<0>(parsed_edges[cur_ind++]) = vid;
      }
      if (vid != invalid_vid) {
        degree[vid]++;
      }
    }
  }
}

template <typename SRC_PK_T, typename DST_PK_T, typename EDATA_T,
          typename VECTOR_T>
static void append_edges(
    std::shared_ptr<arrow::Array> src_col,
    std::shared_ptr<arrow::Array> dst_col, const IndexerType& src_indexer,
    const IndexerType& dst_indexer, std::shared_ptr<arrow::Array>& edata_cols,
    VECTOR_T& parsed_edges, std::vector<std::atomic<int32_t>>& ie_degree,
    std::vector<std::atomic<int32_t>>& oe_degree, size_t offset = 0) {
  CHECK(src_col->length() == dst_col->length());
  auto indexer_check_lambda = [](const IndexerType& cur_indexer,
                                 const std::shared_ptr<arrow::Array>& cur_col) {
    if (cur_indexer.get_type() == PropertyType::kInt64) {
      CHECK(cur_col->type()->Equals(arrow::int64()));
    } else if (cur_indexer.get_type() == PropertyType::kStringView) {
      CHECK(cur_col->type()->Equals(arrow::utf8()) ||
            cur_col->type()->Equals(arrow::large_utf8()));
    } else if (cur_indexer.get_type() == PropertyType::kInt32) {
      CHECK(cur_col->type()->Equals(arrow::int32()));
    } else if (cur_indexer.get_type() == PropertyType::kUInt32) {
      CHECK(cur_col->type()->Equals(arrow::uint32()));
    } else if (cur_indexer.get_type() == PropertyType::kUInt64) {
      CHECK(cur_col->type()->Equals(arrow::uint64()));
    }
  };

  indexer_check_lambda(src_indexer, src_col);
  indexer_check_lambda(dst_indexer, dst_col);
  auto old_size = parsed_edges.size();
  parsed_edges.resize(old_size + src_col->length());
  VLOG(10) << "resize parsed_edges from" << old_size << " to "
           << parsed_edges.size()
           << "EDATA_T: " << AnyConverter<EDATA_T>::type_name();

  // if EDATA_T is grape::EmptyType, no need to read columns
  auto edata_col_thread = std::thread([&]() {
    if constexpr (std::is_same<EDATA_T, RecordView>::value ||
                  std::is_same<EDATA_T, std::string_view>::value) {
      size_t cur_ind = old_size;
      for (auto j = 0; j < src_col->length(); ++j) {
        std::get<2>(parsed_edges[cur_ind++]) = offset++;
      }
    } else if constexpr (!std::is_same<EDATA_T, grape::EmptyType>::value) {
      auto edata_col = edata_cols;
      CHECK(src_col->length() == edata_col->length());
      size_t cur_ind = old_size;
      auto type = edata_col->type();
      if (!type->Equals(TypeConverter<EDATA_T>::ArrowTypeValue())) {
        LOG(FATAL) << "Inconsistent data type, expect "
                   << TypeConverter<EDATA_T>::ArrowTypeValue()->ToString()
                   << ", but got " << type->ToString();
      }

      using arrow_array_type =
          typename gs::TypeConverter<EDATA_T>::ArrowArrayType;
      // cast chunk to EDATA_T array
      auto data = std::static_pointer_cast<arrow_array_type>(edata_col);
      for (auto j = 0; j < edata_col->length(); ++j) {
        std::get<2>(parsed_edges[cur_ind++]) = data->Value(j);
      }
      VLOG(10) << "Finish inserting:  " << src_col->length() << " edges";
    }
  });
  size_t cur_ind = old_size;
  auto src_col_thread = std::thread([&]() {
    _append<SRC_PK_T, EDATA_T>(false, cur_ind, src_col, src_indexer,
                               parsed_edges, oe_degree);
  });
  auto dst_col_thread = std::thread([&]() {
    _append<DST_PK_T, EDATA_T>(true, cur_ind, dst_col, dst_indexer,
                               parsed_edges, ie_degree);
  });
  src_col_thread.join();
  dst_col_thread.join();
  edata_col_thread.join();
}

// A AbstractArrowFragmentLoader with can load fragment from arrow::table.
// Cannot be used directly, should be inherited.
class AbstractArrowFragmentLoader : public IFragmentLoader {
 public:
  AbstractArrowFragmentLoader(const std::string& work_dir, const Schema& schema,
                              const LoadingConfig& loading_config)
      : loading_config_(loading_config),
        schema_(schema),
        thread_num_(loading_config_.GetParallelism()),
        build_csr_in_mem_(loading_config_.GetBuildCsrInMem()),
        use_mmap_vector_(loading_config_.GetUseMmapVector()),
        basic_fragment_loader_(schema_, work_dir) {
    vertex_label_num_ = schema_.vertex_label_num();
    edge_label_num_ = schema_.edge_label_num();
    mtxs_ = new std::mutex[vertex_label_num_];
  }

  ~AbstractArrowFragmentLoader() {
    if (mtxs_) {
      delete[] mtxs_;
    }
  }

  void AddVerticesRecordBatch(
      label_t v_label_id, const std::vector<std::string>& input_paths,
      std::function<std::vector<std::shared_ptr<IRecordBatchSupplier>>(
          label_t, const std::string&, const LoadingConfig&, int)>
          supplier_creator);

  // Add edges in record batch to output_parsed_edges, output_ie_degrees and
  // output_oe_degrees.

  void AddEdgesRecordBatch(
      label_t src_label_id, label_t dst_label_id, label_t edge_label_id,
      const std::vector<std::string>& input_paths,
      std::function<std::vector<std::shared_ptr<IRecordBatchSupplier>>(
          label_t, label_t, label_t, const std::string&, const LoadingConfig&,
          int)>
          supplier_creator);

  static void BatchConsumer(
      const std::vector<std::string>& files, size_t column_num,
      std::function<std::vector<std::shared_ptr<IRecordBatchSupplier>>(
          const std::string&)>
          supplier_creator,
      std::function<void(const arrow::ArrayVector&, size_t)>&& consumer) {
    VLOG(10) << "Parsing  file:" << files.size();

    grape::BlockingQueue<std::shared_ptr<arrow::RecordBatch>> queue;
    queue.SetLimit(1024);
    std::vector<std::thread> work_threads;

    for (auto& file : files) {
      VLOG(10) << "Parsing file:" << file;
      auto record_batch_supplier_vec = supplier_creator(file);
      queue.SetProducerNum(record_batch_supplier_vec.size());

      for (size_t idx = 0; idx < record_batch_supplier_vec.size(); ++idx) {
        work_threads.emplace_back(
            [&](int i) {
              auto& record_batch_supplier = record_batch_supplier_vec[i];
              bool first_batch = true;
              while (true) {
                auto batch = record_batch_supplier->GetNextBatch();
                if (!batch) {
                  queue.DecProducerNum();
                  break;
                }
                if (first_batch) {
                  auto header = batch->schema()->field_names();
                  CHECK(column_num == header.size())
                      << "File header of size: " << header.size()
                      << " does not match schema column size: " << column_num;
                  first_batch = false;
                }
                queue.Put(batch);
              }
            },
            idx);
      }

      for (unsigned idx = 0;
           idx <
           std::min(static_cast<unsigned>(8 * record_batch_supplier_vec.size()),
                    std::thread::hardware_concurrency());
           ++idx) {
        work_threads.emplace_back(
            [&](int i) {
              // It is possible that the inserted data will exceed the size of
              // the table, so we need to resize the table.
              while (true) {
                std::shared_ptr<arrow::RecordBatch> batch{nullptr};
                auto ret = queue.Get(batch);
                if (!ret) {
                  break;
                }
                if (!batch) {
                  LOG(FATAL) << "get nullptr batch";
                }
                consumer(batch->columns(), i);
              }
            },
            idx);
      }
      for (auto& t : work_threads) {
        t.join();
      }
      work_threads.clear();
      VLOG(10) << "Finish parsing file:" << file;
    }

    VLOG(10) << "Finish parsing file:" << files.size();
  }

  template <typename EDATA_T, typename VECTOR_T>
  static void append_edges_utils(std::shared_ptr<arrow::Array> src_col,
                                 std::shared_ptr<arrow::Array> dst_col,
                                 const IndexerType& src_indexer,
                                 const IndexerType& dst_indexer,
                                 std::shared_ptr<arrow::Array>& property_cols,
                                 VECTOR_T& parsed_edges,
                                 std::vector<std::atomic<int32_t>>& ie_degree,
                                 std::vector<std::atomic<int32_t>>& oe_degree,
                                 size_t offset) {
    auto src_col_type = src_col->type();
    if (src_col_type->Equals(arrow::int64())) {
      _append_edges_utils<int64_t, EDATA_T, VECTOR_T>(
          src_col, dst_col, src_indexer, dst_indexer, property_cols,
          parsed_edges, ie_degree, oe_degree, offset);
    } else if (src_col_type->Equals(arrow::uint64())) {
      _append_edges_utils<uint64_t, EDATA_T, VECTOR_T>(
          src_col, dst_col, src_indexer, dst_indexer, property_cols,
          parsed_edges, ie_degree, oe_degree, offset);
    } else if (src_col_type->Equals(arrow::int32())) {
      _append_edges_utils<int32_t, EDATA_T, VECTOR_T>(
          src_col, dst_col, src_indexer, dst_indexer, property_cols,
          parsed_edges, ie_degree, oe_degree, offset);
    } else if (src_col_type->Equals(arrow::uint32())) {
      _append_edges_utils<uint32_t, EDATA_T, VECTOR_T>(
          src_col, dst_col, src_indexer, dst_indexer, property_cols,
          parsed_edges, ie_degree, oe_degree, offset);
    } else {
      // must be string
      _append_edges_utils<std::string_view, EDATA_T, VECTOR_T>(
          src_col, dst_col, src_indexer, dst_indexer, property_cols,
          parsed_edges, ie_degree, oe_degree, offset);
    }
  }

  static Status batch_load_vertices(
      MutablePropertyFragment& graph, const label_t& v_label_id,
      std::vector<std::shared_ptr<IRecordBatchSupplier>>&
          record_batch_supplier_vec);
  static Status batch_load_edges(
      MutablePropertyFragment& graph, const label_t& src_v_label,
      const label_t& dst_v_label, const label_t& edge_label,
      std::vector<std::shared_ptr<IRecordBatchSupplier>>&
          record_batch_supplier_vec);

 protected:
  template <typename KEY_T>
  void addVertexBatchFromArray(
      label_t v_label_id, IdIndexer<KEY_T, vid_t>& indexer,
      std::shared_ptr<arrow::Array>& primary_key_col,
      const std::vector<std::shared_ptr<arrow::Array>>& property_cols,
      std::shared_mutex& rw_mutex) {
    size_t row_num = primary_key_col->length();
    auto col_num = property_cols.size();
    for (size_t i = 0; i < col_num; ++i) {
      CHECK_EQ(property_cols[i]->length(), row_num);
    }

    std::vector<size_t> vids;
    vids.reserve(row_num);
    {
      std::unique_lock<std::mutex> lock(mtxs_[v_label_id]);
      _add_vertex<KEY_T>()(primary_key_col, indexer, vids);
    }
    {
      std::shared_lock<std::shared_mutex> lock(rw_mutex);
      for (size_t j = 0; j < property_cols.size(); ++j) {
        auto array = property_cols[j];
        auto chunked_array = std::make_shared<arrow::ChunkedArray>(array);
        set_properties_column(
            basic_fragment_loader_.GetVertexTable(v_label_id).column_ptrs()[j],
            chunked_array, vids);
      }
    }

    VLOG(10) << "Insert rows: " << row_num;
  }

  template <typename KEY_T>
  void addVertexRecordBatchImpl(
      label_t v_label_id, const std::vector<std::string>& v_files,
      std::function<std::vector<std::shared_ptr<IRecordBatchSupplier>>(
          label_t, const std::string&, const LoadingConfig&, int)>
          supplier_creator) {
    std::string v_label_name = schema_.get_vertex_label_name(v_label_id);
    IdIndexer<KEY_T, vid_t> indexer;
    std::atomic<size_t> offset(0);
    std::shared_mutex rw_mutex;
    auto& vtable = basic_fragment_loader_.GetVertexTable(v_label_id);

    auto func = [&](const arrow::ArrayVector& columns, size_t) {
      const auto& primary_keys = schema_.get_vertex_primary_key(v_label_id);
      auto primary_key = primary_keys[0];
      auto primary_key_name = std::get<1>(primary_key);
      size_t primary_key_ind = std::get<2>(primary_key);
      CHECK(primary_key_ind < columns.size());
      auto primary_key_col = columns[primary_key_ind];
      auto property_cols = columns;
      property_cols.erase(property_cols.begin() + primary_key_ind);

      auto local_offset = offset.fetch_add(primary_key_col->length());
      size_t cur_row_num = std::max(vtable.row_num(), 1ul);
      while (cur_row_num < local_offset + primary_key_col->length()) {
        cur_row_num *= 2;
      }
      if (cur_row_num >= vtable.row_num()) {
        std::unique_lock<std::shared_mutex> lock(rw_mutex);
        if (cur_row_num >= vtable.row_num()) {
          LOG(INFO) << "Resize vertex table from " << vtable.row_num() << " to "
                    << cur_row_num << ", local_offset: " << local_offset;
          vtable.resize(cur_row_num);
        }
      }
      addVertexBatchFromArray<KEY_T>(v_label_id, indexer, primary_key_col,
                                     property_cols, rw_mutex);
    };
    auto supplier_creator_wrapper = [&](const std::string& file) {
      return supplier_creator(v_label_id, file, loading_config_,
                              std::thread::hardware_concurrency());
    };

    BatchConsumer(v_files,
                  schema_.get_vertex_property_names(v_label_id).size() + 1,
                  supplier_creator_wrapper, std::move(func));
    if (indexer.bucket_count() == 0) {
      indexer._rehash(schema_.get_max_vnum(v_label_name));
    }
    basic_fragment_loader_.FinishAddingVertex<KEY_T>(v_label_id, indexer);
  }

  template <typename SRC_PK_T, typename EDATA_T, typename VECTOR_T>
  static void _append_edges_utils(std::shared_ptr<arrow::Array> src_col,
                                  std::shared_ptr<arrow::Array> dst_col,
                                  const IndexerType& src_indexer,
                                  const IndexerType& dst_indexer,
                                  std::shared_ptr<arrow::Array>& property_cols,
                                  VECTOR_T& parsed_edges,
                                  std::vector<std::atomic<int32_t>>& ie_degree,
                                  std::vector<std::atomic<int32_t>>& oe_degree,
                                  size_t offset) {
    auto dst_col_type = dst_col->type();
    if (dst_col_type->Equals(arrow::int64())) {
      append_edges<SRC_PK_T, int64_t, EDATA_T>(
          src_col, dst_col, src_indexer, dst_indexer, property_cols,
          parsed_edges, ie_degree, oe_degree, offset);
    } else if (dst_col_type->Equals(arrow::uint64())) {
      append_edges<SRC_PK_T, uint64_t, EDATA_T>(
          src_col, dst_col, src_indexer, dst_indexer, property_cols,
          parsed_edges, ie_degree, oe_degree, offset);
    } else if (dst_col_type->Equals(arrow::int32())) {
      append_edges<SRC_PK_T, int32_t, EDATA_T>(
          src_col, dst_col, src_indexer, dst_indexer, property_cols,
          parsed_edges, ie_degree, oe_degree, offset);
    } else if (dst_col_type->Equals(arrow::uint32())) {
      append_edges<SRC_PK_T, uint32_t, EDATA_T>(
          src_col, dst_col, src_indexer, dst_indexer, property_cols,
          parsed_edges, ie_degree, oe_degree, offset);
    } else {
      // must be string
      append_edges<SRC_PK_T, std::string_view, EDATA_T>(
          src_col, dst_col, src_indexer, dst_indexer, property_cols,
          parsed_edges, ie_degree, oe_degree, offset);
    }
  }

  template <typename EDATA_T>
  void addEdgesRecordBatchImpl(
      label_t src_label_id, label_t dst_label_id, label_t e_label_id,
      const std::vector<std::string>& e_files,
      std::function<std::vector<std::shared_ptr<IRecordBatchSupplier>>(
          label_t, label_t, label_t, const std::string&, const LoadingConfig&,
          int)>
          supplier_creator) {
    if constexpr (std::is_same_v<EDATA_T, RecordView> ||
                  std::is_same_v<EDATA_T, std::string_view>) {
      if (use_mmap_vector_) {
        addEdgesRecordBatchImplHelper<
            EDATA_T, std::vector<std::tuple<vid_t, vid_t, size_t>>>(
            src_label_id, dst_label_id, e_label_id, e_files, supplier_creator);
      } else {
        addEdgesRecordBatchImplHelper<
            EDATA_T, std::vector<std::tuple<vid_t, vid_t, size_t>>>(
            src_label_id, dst_label_id, e_label_id, e_files, supplier_creator);
      }
    } else {
      if (use_mmap_vector_) {
        addEdgesRecordBatchImplHelper<
            EDATA_T, mmap_vector<std::tuple<vid_t, vid_t, EDATA_T>>>(
            src_label_id, dst_label_id, e_label_id, e_files, supplier_creator);
      } else {
        addEdgesRecordBatchImplHelper<
            EDATA_T, std::vector<std::tuple<vid_t, vid_t, EDATA_T>>>(
            src_label_id, dst_label_id, e_label_id, e_files, supplier_creator);
      }
    }
  }

  template <typename EDATA_T, typename VECTOR_T>
  void addEdgesRecordBatchImplHelper(
      label_t src_label_id, label_t dst_label_id, label_t e_label_id,
      const std::vector<std::string>& e_files,
      std::function<std::vector<std::shared_ptr<IRecordBatchSupplier>>(
          label_t, label_t, label_t, const std::string&, const LoadingConfig&,
          int)>
          supplier_creator) {
    auto src_label_name = schema_.get_vertex_label_name(src_label_id);
    auto dst_label_name = schema_.get_vertex_label_name(dst_label_id);
    auto edge_label_name = schema_.get_edge_label_name(e_label_id);
    auto edge_column_mappings = loading_config_.GetEdgeColumnMappings(
        src_label_id, dst_label_id, e_label_id);
    auto src_dst_col_pair = loading_config_.GetEdgeSrcDstCol(
        src_label_id, dst_label_id, e_label_id);
    if (src_dst_col_pair.first.size() != 1 ||
        src_dst_col_pair.second.size() != 1) {
      LOG(FATAL) << "We currently only support one src primary key and one "
                    "dst primary key";
    }
    size_t src_col_ind = src_dst_col_pair.first[0].second;
    size_t dst_col_ind = src_dst_col_pair.second[0].second;
    CHECK(src_col_ind != dst_col_ind);

    check_edge_invariant(schema_, edge_column_mappings, src_col_ind,
                         dst_col_ind, src_label_id, dst_label_id, e_label_id);

    const auto& src_indexer = basic_fragment_loader_.GetLFIndexer(src_label_id);
    const auto& dst_indexer = basic_fragment_loader_.GetLFIndexer(dst_label_id);
    std::vector<VECTOR_T> parsed_edges_vec(std::thread::hardware_concurrency());
    if constexpr (std::is_same_v<
                      VECTOR_T,
                      mmap_vector<std::tuple<vid_t, vid_t, EDATA_T>>> ||
                  std::is_same_v<
                      VECTOR_T,
                      mmap_vector<std::tuple<vid_t, vid_t, size_t>>>) {
      const auto& work_dir = basic_fragment_loader_.work_dir();
      for (unsigned i = 0; i < std::thread::hardware_concurrency(); ++i) {
        parsed_edges_vec[i].open(runtime_dir(work_dir) + "/" + src_label_name +
                                 "_" + dst_label_name + "_" + edge_label_name +
                                 "_" + std::to_string(i) + ".tmp");
        parsed_edges_vec[i].reserve(4096);
      }
    }
    std::vector<std::atomic<int32_t>> ie_degree(dst_indexer.size()),
        oe_degree(src_indexer.size());
    for (size_t idx = 0; idx < ie_degree.size(); ++idx) {
      ie_degree[idx].store(0);
    }
    for (size_t idx = 0; idx < oe_degree.size(); ++idx) {
      oe_degree[idx].store(0);
    }
    VLOG(10) << "src indexer size: " << src_indexer.size()
             << " dst indexer size: " << dst_indexer.size();

    std::atomic<size_t> offset(0);
    std::shared_mutex rw_mutex;
    auto supplier_creator_wrapper = [&](const std::string& filename) {
      return supplier_creator(src_label_id, dst_label_id, e_label_id, filename,
                              loading_config_,
                              std::thread::hardware_concurrency());
    };

    auto func = [&](const arrow::ArrayVector& columns, size_t idx) {
      CHECK(columns.size() >= 2);
      auto src_col = columns[0];
      auto dst_col = columns[1];
      auto src_col_type = src_col->type();
      auto dst_col_type = dst_col->type();
      CHECK(check_primary_key_type(src_col_type))
          << "unsupported src_col type: " << src_col_type->ToString();
      CHECK(check_primary_key_type(dst_col_type))
          << "unsupported dst_col type: " << dst_col_type->ToString();
      auto& parsed_edges = parsed_edges_vec[idx];

      std::vector<std::shared_ptr<arrow::Array>> property_cols;
      for (size_t i = 2; i < columns.size(); ++i) {
        property_cols.emplace_back(columns[i]);
      }
      size_t offset_i = 0;
      if constexpr (std::is_same<EDATA_T, RecordView>::value ||
                    std::is_same<EDATA_T, std::string_view>::value) {
        auto& table = basic_fragment_loader_.GetEdgePropertiesTable(
            src_label_id, dst_label_id, e_label_id);
        CHECK(table.col_num() == property_cols.size())
            << table.col_num() << " " << property_cols.size();
        offset_i = offset.fetch_add(src_col->length());
        std::vector<size_t> offsets;
        for (size_t _i = 0; _i < static_cast<size_t>(src_col->length()); ++_i) {
          offsets.emplace_back(offset_i + _i);
        }
        size_t row_num = std::max(table.row_num(), 1ul);

        while (row_num < offset_i + src_col->length()) {
          row_num *= 2;
        }
        if (row_num > table.row_num()) {
          std::unique_lock<std::shared_mutex> lock(rw_mutex);
          if (row_num > table.row_num()) {
            table.resize(row_num);
          }
        }

        {
          std::shared_lock<std::shared_mutex> lock(rw_mutex);
          for (size_t i = 0; i < table.col_num(); ++i) {
            auto col = table.get_column_by_id(i);
            auto chunked_array =
                std::make_shared<arrow::ChunkedArray>(property_cols[i]);
            set_properties_column(col.get(), chunked_array, offsets);
          }
        }
      }
      // add edges to vector
      CHECK(src_col->length() == dst_col->length());
      append_edges_utils<EDATA_T, VECTOR_T>(
          src_col, dst_col, src_indexer, dst_indexer, property_cols[0],
          parsed_edges, ie_degree, oe_degree, offset_i);
    };

    BatchConsumer(
        e_files,
        schema_.get_edge_property_names(src_label_id, dst_label_id, e_label_id)
                .size() +
            2,
        supplier_creator_wrapper, std::move(func));
    VLOG(10) << "Finish parsing edge file:" << e_files.size() << " for label "
             << src_label_name << " -> " << dst_label_name << " -> "
             << edge_label_name;
    std::vector<int32_t> ie_deg(ie_degree.size());
    std::vector<int32_t> oe_deg(oe_degree.size());
    for (size_t idx = 0; idx < ie_deg.size(); ++idx) {
      ie_deg[idx] = ie_degree[idx];
    }
    for (size_t idx = 0; idx < oe_deg.size(); ++idx) {
      oe_deg[idx] = oe_degree[idx];
    }

    basic_fragment_loader_.PutEdges<EDATA_T, VECTOR_T>(
        src_label_id, dst_label_id, e_label_id, parsed_edges_vec, ie_deg,
        oe_deg, build_csr_in_mem_);

    size_t sum = 0;
    for (auto& edges : parsed_edges_vec) {
      sum += edges.size();
      if constexpr (
          std::is_same<VECTOR_T,
                       mmap_vector<std::tuple<vid_t, vid_t, EDATA_T>>>::value ||
          std::is_same<VECTOR_T,
                       mmap_vector<std::tuple<vid_t, vid_t, size_t>>>::value) {
        edges.unlink();
      }
    }

    VLOG(10) << "Finish putting: " << sum << " edges";
  }

  const LoadingConfig& loading_config_;
  const Schema& schema_;
  size_t vertex_label_num_, edge_label_num_;
  int32_t thread_num_;
  std::mutex* mtxs_;
  bool build_csr_in_mem_;
  bool use_mmap_vector_;
  mutable BasicFragmentLoader basic_fragment_loader_;
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_LOADER_ABSTRACT_ARROW_FRAGMENT_LOADER_H_
