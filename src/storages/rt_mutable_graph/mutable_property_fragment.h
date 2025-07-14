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

#include "src/engines/graph_db/runtime/common/utils/bitset.h"
#include "src/storages/rt_mutable_graph/csr/mutable_csr.h"
#include "src/storages/rt_mutable_graph/dual_csr.h"
#include "src/storages/rt_mutable_graph/file_names.h"
#include "src/storages/rt_mutable_graph/loader/abstract_arrow_fragment_loader.h"
#include "src/storages/rt_mutable_graph/loader/basic_fragment_loader.h"
#include "src/storages/rt_mutable_graph/loader/loader_utils.h"
#include "src/storages/rt_mutable_graph/schema.h"
#include "src/storages/rt_mutable_graph/types.h"
#include "src/utils/allocators.h"
#include "src/utils/arrow_utils.h"
#include "src/utils/id_indexer.h"
#include "src/utils/indexers.h"
#include "src/utils/property/column.h"
#include "src/utils/property/table.h"
#include "src/utils/property/types.h"
#include "src/utils/result.h"
#include "third_party/libgrape-lite/grape/utils/concurrent_queue.h"

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

template <typename PK_T, typename EDATA_T, typename VECTOR_T>
void insert_edges(bool is_dst, size_t cur_ind,
                  std::shared_ptr<arrow::Array> col, const IndexerType& indexer,
                  VECTOR_T& parsed_edges,
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

  template <typename PK_T>
  Status batch_load_vertices(const label_t& v_label_id,
                             std::vector<std::shared_ptr<IRecordBatchSupplier>>&
                                 record_batch_supplier_vec) {
    std::string vertex_type_name = schema_.get_vertex_label_name(v_label_id);
    auto schema_column_names = schema_.get_vertex_property_names(v_label_id);
    auto primary_key = schema_.get_vertex_primary_key(v_label_id)[0];
    auto primary_key_name = std::get<1>(primary_key);
    size_t primary_key_ind = std::get<2>(primary_key);

    grape::BlockingQueue<std::shared_ptr<arrow::RecordBatch>> queue;
    queue.SetLimit(1024);
    std::vector<std::thread> work_threads;
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
                CHECK(schema_column_names.size() + 1 == header.size())
                    << "File header of size: " << header.size()
                    << " does not match schema column size: "
                    << schema_column_names.size() + 1;
                first_batch = false;
              }
              queue.Put(batch);
            }
          },
          idx);
    }

    std::shared_mutex rw_mutex;
    std::atomic<size_t> offset(0);
    for (unsigned idx = 0;
         idx <
         std::min(static_cast<unsigned>(8 * record_batch_supplier_vec.size()),
                  std::thread::hardware_concurrency());
         ++idx) {
      work_threads.emplace_back(
          [&](int i) {
            auto& vtable = vertex_data_[v_label_id];
            while (true) {
              std::shared_ptr<arrow::RecordBatch> batch{nullptr};
              auto ret = queue.Get(batch);
              if (!ret) {
                break;
              }
              if (!batch) {
                LOG(FATAL) << "get nullptr batch";
              }
              auto columns = batch->columns();
              CHECK(primary_key_ind < columns.size());
              auto primary_key_column = columns[primary_key_ind];
              auto other_columns_array = columns;
              other_columns_array.erase(other_columns_array.begin() +
                                        primary_key_ind);

              auto local_offset =
                  offset.fetch_add(primary_key_column->length());
              size_t cur_row_num = std::max(vtable.row_num(), 1ul);
              while (cur_row_num <
                     local_offset + primary_key_column->length()) {
                cur_row_num *= 2;
              }
              if (cur_row_num > vtable.row_num()) {
                std::unique_lock<std::shared_mutex> lock(rw_mutex);
                if (cur_row_num > vtable.row_num()) {
                  vtable.resize(cur_row_num);
                }
              }

              size_t row_num = primary_key_column->length();
              auto col_num = other_columns_array.size();
              for (size_t j = 0; j < col_num; ++j) {
                CHECK_EQ(other_columns_array[j]->length(), row_num);
              }
              std::vector<size_t> vids;
              vids.reserve(row_num);

              {
                std::unique_lock<std::mutex> lock(*v_mutex_[v_label_id]);
                vid_t vid;
                if constexpr (!std::is_same<std::string_view, PK_T>::value) {
                  static constexpr vid_t sentinel =
                      std::numeric_limits<vid_t>::max();
                  auto expected_type =
                      gs::TypeConverter<PK_T>::ArrowTypeValue();
                  using arrow_array_t =
                      typename gs::TypeConverter<PK_T>::ArrowArrayType;
                  if (!primary_key_column->type()->Equals(expected_type)) {
                    LOG(FATAL) << "Inconsistent data type, expect "
                               << expected_type->ToString() << ", but got "
                               << primary_key_column->type()->ToString();
                  }
                  auto casted_array = std::static_pointer_cast<arrow_array_t>(
                      primary_key_column);
                  LOG(INFO) << "Indexer reserve size "
                            << lf_indexers_[v_label_id].size() + row_num;
                  {
                    lf_indexers_[v_label_id].reserve(
                        lf_indexers_[v_label_id].size() + row_num);
                  }
                  for (size_t j = 0; j < row_num; ++j) {
                    auto vertex_exist = lf_indexers_[v_label_id].get_index(
                        casted_array->Value(j), vid);
                    if (vertex_exist) {
                      vids.emplace_back(sentinel);
                    } else {
                      vid = lf_indexers_[v_label_id].insert(
                          casted_array->Value(j));
                      vids.emplace_back(vid);
                    }
                  }
                } else {
                  if (primary_key_column->type()->Equals(arrow::utf8())) {
                    auto casted_array =
                        std::static_pointer_cast<arrow::StringArray>(
                            primary_key_column);
                    for (size_t j = 0; j < row_num; ++j) {
                      auto str = casted_array->GetView(j);
                      std::string_view str_view(str.data(), str.size());
                      auto vertex_exist =
                          lf_indexers_[v_label_id].get_index(str_view, vid);
                      if (vertex_exist) {
                        vids.emplace_back(std::numeric_limits<size_t>::max());
                      } else {
                        vid = lf_indexers_[v_label_id].insert(str_view);
                        vids.emplace_back(vid);
                      }
                    }
                  } else if (primary_key_column->type()->Equals(
                                 arrow::large_utf8())) {
                    auto casted_array =
                        std::static_pointer_cast<arrow::LargeStringArray>(
                            primary_key_column);
                    for (size_t j = 0; j < row_num; ++j) {
                      auto str = casted_array->GetView(j);
                      std::string_view str_view(str.data(), str.size());
                      auto vertex_exist =
                          lf_indexers_[v_label_id].get_index(str_view, vid);
                      if (vertex_exist) {
                        vids.emplace_back(std::numeric_limits<size_t>::max());
                      } else {
                        vid = lf_indexers_[v_label_id].insert(str_view);
                        vids.emplace_back(vid);
                      }
                    }
                  } else {
                    LOG(FATAL) << "Not support type: "
                               << primary_key_column->type()->ToString();
                  }
                }
              }

              LOG(INFO) << "Start to set property column";
              // Set other columns
              {
                std::shared_lock<std::shared_mutex> lock(rw_mutex);
                for (size_t j = 0; j < other_columns_array.size(); j++) {
                  auto chunked_array = std::make_shared<arrow::ChunkedArray>(
                      other_columns_array[j]);
                  auto column = vertex_data_[v_label_id].column_ptrs()[j];
                  set_properties_column(column, chunked_array, vids);
                }
              }
            }
          },
          idx);
    }
    for (auto& t : work_threads) {
      t.join();
    }
    work_threads.clear();

    if (lid_num(v_label_id) > vertex_tomb_[v_label_id]->size()) {
      vertex_tomb_[v_label_id]->resize(lid_num(v_label_id));
    }

    vertex_data_[v_label_id].dump_without_close(
        vertex_table_prefix(vertex_type_name), snapshot_dir(work_dir_, 0));
    lf_indexers_[v_label_id].dump_without_close(
        LFIndexer<vid_t>::prefix() + "_" + vertex_map_prefix(vertex_type_name),
        snapshot_dir(work_dir_, 0));
    return gs::Status::OK();
  }

  template <typename SRC_PK_T, typename DST_PK_T, typename EDATA_T,
            typename VECTOR_T>
  Status batch_load_edges(const label_t& src_v_label,
                          const label_t& dst_v_label, const label_t& edge_label,
                          std::vector<std::shared_ptr<IRecordBatchSupplier>>&
                              record_batch_supplier_vec) {
    std::string src_vertex_type = schema_.get_vertex_label_name(src_v_label);
    std::string dst_vertex_type = schema_.get_vertex_label_name(dst_v_label);
    std::string edge_type_name = schema_.get_edge_label_name(edge_label);
    size_t index =
        schema_.generate_edge_label(src_v_label, dst_v_label, edge_label);

    std::vector<VECTOR_T> parsed_edges_vec(std::thread::hardware_concurrency());

    grape::BlockingQueue<std::shared_ptr<arrow::RecordBatch>> queue;
    queue.SetLimit(1024);
    std::vector<std::thread> work_threads;

    std::vector<std::vector<std::shared_ptr<arrow::Array>>> string_columns(
        std::thread::hardware_concurrency());
    queue.SetProducerNum(record_batch_supplier_vec.size());

    if constexpr (std::is_same<EDATA_T, RecordView>::value) {
      if (!init_state_.at(index)) {
        auto casted_csr =
            dynamic_cast<DualCsr<RecordView>*>(dual_csr_map_.at(index));
        casted_csr->InitTable(
            edata_prefix(src_vertex_type, dst_vertex_type, edge_type_name),
            tmp_dir(work_dir_));
      }
    }

    const auto& src_indexer = lf_indexers_[src_v_label];
    const auto& dst_indexer = lf_indexers_[dst_v_label];
    std::vector<std::atomic<int32_t>> ie_degree(dst_indexer.size()),
        oe_degree(src_indexer.size());
    for (size_t idx = 0; idx < ie_degree.size(); ++idx) {
      ie_degree[idx].store(0);
    }
    for (size_t idx = 0; idx < oe_degree.size(); ++idx) {
      oe_degree[idx].store(0);
    }

    std::atomic<size_t> offset(0);
    std::shared_mutex rw_mutex;
    for (size_t idx = 0; idx < record_batch_supplier_vec.size(); ++idx) {
      work_threads.emplace_back(
          [&](int idx) {
            auto& string_column = string_columns[idx];
            bool first_batch = true;
            auto& record_batch_supplier = record_batch_supplier_vec[idx];
            while (true) {
              auto batch = record_batch_supplier->GetNextBatch();
              if (!batch) {
                queue.DecProducerNum();
                break;
              }
              if (first_batch) {
                auto header = batch->schema()->field_names();
                auto schema_column_names = schema_.get_edge_property_names(
                    src_v_label, dst_v_label, edge_label);
                auto schema_column_types = schema_.get_edge_properties(
                    src_v_label, dst_v_label, edge_label);
                CHECK(schema_column_names.size() + 2 == header.size())
                    << "schema size: " << schema_column_names.size()
                    << " neq header size: " << header.size();
                first_batch = false;
              }
              for (auto i = 0; i < batch->num_columns(); ++i) {
                if (batch->column(i)->type()->Equals(arrow::utf8()) ||
                    batch->column(i)->type()->Equals(arrow::large_utf8())) {
                  string_column.emplace_back(batch->column(i));
                }
              }
              queue.Put(batch);
            }
          },
          idx);
    }
    for (size_t idx = 0;
         idx <
         std::min(static_cast<unsigned>(8 * record_batch_supplier_vec.size()),
                  std::thread::hardware_concurrency());
         ++idx) {
      work_threads.emplace_back(
          [&](int idx) {
            auto& parsed_edges = parsed_edges_vec[idx];
            while (true) {
              std::shared_ptr<arrow::RecordBatch> batch{nullptr};
              auto ret = queue.Get(batch);
              if (!ret) {
                break;
              }
              if (!batch) {
                LOG(FATAL) << "get nullptr batch";
              }
              auto columns = batch->columns();
              // We assume the src_col and dst_col will always be put
              // at front.
              CHECK(columns.size() >= 2);
              auto src_col = columns[0];
              auto dst_col = columns[1];
              auto src_col_type = src_col->type();
              auto dst_col_type = dst_col->type();
              CHECK(check_primary_key_type(src_col_type))
                  << "unsupported src_col type: " << src_col_type->ToString();
              CHECK(check_primary_key_type(dst_col_type))
                  << "unsupported dst_col type: " << dst_col_type->ToString();
              std::vector<std::shared_ptr<arrow::Array>> property_cols;
              for (size_t i = 2; i < columns.size(); ++i) {
                property_cols.emplace_back(columns[i]);
              }
              size_t offset_i = 0;
              if constexpr (std::is_same<EDATA_T, RecordView>::value) {
                auto casted_csr =
                    dynamic_cast<DualCsr<RecordView>*>(dual_csr_map_.at(index));
                CHECK(casted_csr != NULL);
                auto table = casted_csr->GetTable();
                CHECK(table.col_num() == property_cols.size());
                offset_i = offset.fetch_add(src_col->length());
                std::vector<size_t> offsets;
                for (size_t _i = 0; _i < static_cast<size_t>(src_col->length());
                     ++_i) {
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

              CHECK(src_col->length() == dst_col->length());
              auto old_size = parsed_edges.size();
              parsed_edges.resize(old_size + src_col->length());

              auto edata_col_thread = std::thread([&]() {
                if constexpr (std::is_same<EDATA_T, RecordView>::value) {
                  size_t cur_ind = old_size;
                  for (auto j = 0; j < src_col->length(); ++j) {
                    std::get<2>(parsed_edges[cur_ind++]) = offset++;
                  }
                } else if constexpr (!std::is_same<EDATA_T,
                                                   grape::EmptyType>::value) {
                  auto edata_col = property_cols[0];
                  CHECK(src_col->length() == edata_col->length());
                  size_t cur_ind = old_size;
                  auto type = edata_col->type();
                  if (!type->Equals(TypeConverter<EDATA_T>::ArrowTypeValue())) {
                    LOG(FATAL)
                        << "Inconsistent data type, expect "
                        << TypeConverter<EDATA_T>::ArrowTypeValue()->ToString()
                        << ", but got " << type->ToString();
                  }

                  using arrow_array_type =
                      typename gs::TypeConverter<EDATA_T>::ArrowArrayType;
                  // cast chunk to EDATA_T array
                  auto data =
                      std::static_pointer_cast<arrow_array_type>(edata_col);
                  for (auto j = 0; j < edata_col->length(); ++j) {
                    if constexpr (std::is_same<arrow_array_type,
                                               arrow::StringArray>::value ||
                                  std::is_same<
                                      arrow_array_type,
                                      arrow::LargeStringArray>::value) {
                      auto str = data->GetView(j);
                      std::string_view str_view(str.data(), str.size());
                      std::get<2>(parsed_edges[cur_ind++]) = str_view;
                    } else {
                      std::get<2>(parsed_edges[cur_ind++]) = data->Value(j);
                    }
                  }
                  LOG(INFO)
                      << "Finish inserting:  " << src_col->length() << " edges";
                }
              });

              size_t cur_ind = old_size;
              auto src_col_thread = std::thread([&]() {
                insert_edges<SRC_PK_T, EDATA_T>(false, cur_ind, src_col,
                                                src_indexer, parsed_edges,
                                                oe_degree);
              });
              auto dst_col_thread = std::thread([&]() {
                insert_edges<DST_PK_T, EDATA_T>(true, cur_ind, dst_col,
                                                dst_indexer, parsed_edges,
                                                ie_degree);
              });
              edata_col_thread.join();
              src_col_thread.join();
              dst_col_thread.join();
            }
          },
          idx);
    }

    for (auto& t : work_threads) {
      t.join();
    }

    std::vector<int32_t> ie_deg(ie_degree.size());
    std::vector<int32_t> oe_deg(oe_degree.size());
    for (size_t idx = 0; idx < ie_deg.size(); ++idx) {
      ie_deg[idx] = ie_degree[idx];
    }
    for (size_t idx = 0; idx < oe_deg.size(); ++idx) {
      oe_deg[idx] = oe_degree[idx];
    }

    LOG(INFO) << "Init csr for " << src_vertex_type << " " << edge_type_name
              << " " << dst_vertex_type << ", index is " << index;
    auto dual_csr = dual_csr_map_.at(index);
    CHECK(dual_csr != NULL);
    auto casted_dual_csr =
        BasicFragmentLoader::get_casted_dual_csr<EDATA_T>(dual_csr);
    auto INVALID_VID = std::numeric_limits<vid_t>::max();
    std::atomic<size_t> edge_count(0);
    if (!init_state_.at(index)) {
      dual_csr->BatchInit(
          oe_prefix(src_vertex_type, dst_vertex_type, edge_type_name),
          ie_prefix(src_vertex_type, dst_vertex_type, edge_type_name),
          edata_prefix(src_vertex_type, dst_vertex_type, edge_type_name),
          tmp_dir(work_dir_), oe_deg, ie_deg);
      init_state_[index] = true;
    } else {
      TypedMutableCsrBase<EDATA_T>* in_csr =
          dynamic_cast<TypedMutableCsrBase<EDATA_T>*>(ie_map_.at(index));
      TypedMutableCsrBase<EDATA_T>* out_csr =
          dynamic_cast<TypedMutableCsrBase<EDATA_T>*>(oe_map_.at(index));
      std::vector<int> cur_in_deg = in_csr->get_degree();
      std::vector<int> cur_out_deg = out_csr->get_degree();
      std::vector<int> cur_in_cap = in_csr->get_capacity();
      std::vector<int> cur_out_cap = out_csr->get_capacity();
      CHECK_EQ(ie_deg.size(), cur_in_deg.size());
      CHECK_EQ(oe_deg.size(), cur_out_deg.size());
      bool need_in_resize = false;
      bool need_out_resize = false;
      for (size_t i = 0; i < ie_deg.size(); i++) {
        if (ie_deg[i] > cur_in_cap[i] - cur_in_cap[i]) {
          need_in_resize = true;
          break;
        }
      }

      for (size_t i = 0; i < oe_deg.size(); i++) {
        if (oe_deg[i] > cur_out_cap[i] - cur_out_deg[i]) {
          need_out_resize = true;
          break;
        }
      }
      if (need_in_resize) {
        for (size_t i = 0; i < ie_deg.size(); i++) {
          ie_deg[i] += cur_in_deg[i];
        }
        in_csr->batch_resize(ie_deg);
      }

      if (need_out_resize) {
        for (size_t i = 0; i < oe_deg.size(); i++) {
          oe_deg[i] += cur_out_deg[i];
        }
        out_csr->batch_resize(oe_deg);
      }
    }

    if constexpr (std::is_same_v<EDATA_T, std::string_view>) {
      std::vector<std::thread> edge_threads;
      for (size_t i = 0; i < parsed_edges_vec.size(); ++i) {
        edge_threads.emplace_back(
            [&](int idx) {
              edge_count.fetch_add(parsed_edges_vec[idx].size());
              for (auto& edge : parsed_edges_vec[idx]) {
                if (std::get<1>(edge) == INVALID_VID ||
                    std::get<0>(edge) == INVALID_VID) {
                  VLOG(10) << "Skip invalid edge:" << std::get<0>(edge) << "->"
                           << std::get<1>(edge);
                  continue;
                }
                casted_dual_csr->BatchPutEdge(
                    std::get<0>(edge), std::get<1>(edge), std::get<2>(edge));
              }
            },
            i);
      }
      for (auto& t : edge_threads) {
        t.join();
      }
    } else {
      std::vector<std::thread> edge_threads;
      for (size_t i = 0; i < parsed_edges_vec.size(); ++i) {
        edge_threads.emplace_back(
            [&](int idx) {
              edge_count.fetch_add(parsed_edges_vec[idx].size());
              for (auto& edge : parsed_edges_vec[idx]) {
                if (std::get<1>(edge) == INVALID_VID ||
                    std::get<0>(edge) == INVALID_VID) {
                  VLOG(10) << "Skip invalid edge:" << std::get<0>(edge) << "->"
                           << std::get<1>(edge);
                  continue;
                }
                casted_dual_csr->BatchPutEdge(
                    std::get<0>(edge), std::get<1>(edge), std::get<2>(edge));
              }
            },
            i);
      }
      for (auto& t : edge_threads) {
        t.join();
      }
    }

    dual_csr->Dump(
        oe_prefix(src_vertex_type, dst_vertex_type, edge_type_name),
        ie_prefix(src_vertex_type, dst_vertex_type, edge_type_name),
        edata_prefix(src_vertex_type, dst_vertex_type, edge_type_name),
        snapshot_dir(work_dir_, 0));

    string_columns.clear();
    return gs::Status::OK();
  }

  Status batch_delete_vertices(const label_t& v_label_id,
                               const std::vector<vid_t>& vids);

  Status batch_delete_edges(const label_t& src_v_label,
                            const label_t& dst_v_label,
                            const label_t& edge_label,
                            std::vector<std::tuple<vid_t, vid_t>>& edges_vec);

  inline Table& get_vertex_table(label_t vertex_label) {
    return vertex_data_[vertex_label];
  }

  inline const Table& get_vertex_table(label_t vertex_label) const {
    return vertex_data_[vertex_label];
  }

  vid_t lid_num(label_t vertex_label) const;

  vid_t vertex_num(label_t vertex_label) const;

  bool is_valid_lid(label_t vertex_label, vid_t lid) const;

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
    size_t index =
        schema_.generate_edge_label(label, neighbor_label, edge_label);
    if (oe_map_.find(index) == oe_map_.end()) {
      LOG(ERROR) << "Edge csr not found for label: " << label
                 << ", neighbor_label: " << neighbor_label
                 << ", edge_label: " << edge_label;
      return nullptr;
    }
    return oe_map_.at(index);
  }

  inline const CsrBase* get_oe_csr(label_t label, label_t neighbor_label,
                                   label_t edge_label) const {
    size_t index =
        schema_.generate_edge_label(label, neighbor_label, edge_label);
    if (oe_map_.find(index) == oe_map_.end()) {
      LOG(ERROR) << "Edge csr not found for label: " << label
                 << ", neighbor_label: " << neighbor_label
                 << ", edge_label: " << edge_label;
      return nullptr;
    }
    return oe_map_.at(index);
  }

  inline CsrBase* get_ie_csr(label_t label, label_t neighbor_label,
                             label_t edge_label) {
    size_t index =
        schema_.generate_edge_label(neighbor_label, label, edge_label);
    if (ie_map_.find(index) == ie_map_.end()) {
      LOG(ERROR) << "Edge csr not found for label: " << label
                 << ", neighbor_label: " << neighbor_label
                 << ", edge_label: " << edge_label;
      return nullptr;
    }
    return ie_map_.at(index);
  }

  inline const CsrBase* get_ie_csr(label_t label, label_t neighbor_label,
                                   label_t edge_label) const {
    size_t index =
        schema_.generate_edge_label(neighbor_label, label, edge_label);
    if (ie_map_.find(index) == ie_map_.end()) {
      LOG(ERROR) << "Edge csr not found for label: " << (int32_t) label
                 << ", neighbor_label: " << (int32_t) neighbor_label
                 << ", edge_label: " << (int32_t) edge_label;
      return nullptr;
    }
    return ie_map_.at(index);
  }

  inline bool is_deleted(label_t label) const {
    return vertex_tomb_[label]->count() != 0;
  }

  inline std::shared_ptr<Bitset> get_vertex_tomb(label_t label) const {
    return vertex_tomb_[label];
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
  void dumpSchema() const;

 private:
  /* For Edge previously has zero property or one property, change the EDATA_T
   * to RecordView*/
  void change_csr_data_type_to_record_view(const std::string& src_vertex_type,
                                           const std::string& dst_vertex_type,
                                           const std::string& edge_type_name);

 public:
  std::string work_dir_;
  Schema schema_;
  std::vector<std::shared_ptr<std::mutex>> v_mutex_;
  std::vector<IndexerType> lf_indexers_;
  std::vector<Table> vertex_data_;

  std::vector<std::shared_ptr<Bitset>> vertex_tomb_;

  std::unordered_map<uint32_t, CsrBase*> ie_map_, oe_map_;
  std::unordered_map<uint32_t, DualCsrBase*> dual_csr_map_;
  std::unordered_map<uint32_t, bool> init_state_;

  size_t vertex_label_num_, edge_label_num_;
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_MUTABLE_PROPERTY_FRAGMENT_H_
