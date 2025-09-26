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
#include <sys/statvfs.h>

#include "libgrape-lite/grape/serialization/out_archive.h"
#include "libgrape-lite/grape/types.h"
#include "neug/storages/csr/immutable_csr.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/csr/nbr.h"
#include "neug/storages/loader/abstract_arrow_fragment_loader.h"

#include "neug/storages/graph/property_graph.h"

namespace gs {

void printDiskRemaining(const std::string& path) {
  struct statvfs buf;
  if (statvfs(path.c_str(), &buf) == 0) {
    LOG(INFO) << "Disk remaining: " << buf.f_bsize * buf.f_bavail / 1024 / 1024
              << "MB";
  }
}

bool check_primary_key_type(std::shared_ptr<arrow::DataType> data_type) {
  if (data_type->Equals(arrow::int64()) || data_type->Equals(arrow::uint64()) ||
      data_type->Equals(arrow::int32()) || data_type->Equals(arrow::uint32()) ||
      data_type->Equals(arrow::utf8()) ||
      data_type->Equals(arrow::large_utf8())) {
    return true;
  } else {
    return false;
  }
}

void set_column_from_string_array(gs::ColumnBase* col,
                                  std::shared_ptr<arrow::ChunkedArray> array,
                                  const std::vector<size_t>& offset,
                                  bool enable_resize) {
  auto type = array->type();
  auto size = col->size();
  auto typed_col = dynamic_cast<gs::TypedColumn<std::string_view>*>(col);
  if (enable_resize) {
    CHECK(typed_col != nullptr) << "Only support TypedColumn<std::string_view>";
  }
  CHECK(type->Equals(arrow::large_utf8()) || type->Equals(arrow::utf8()))
      << "Inconsistent data type, expect string, but got " << type->ToString();
  size_t cur_ind = 0;
  if (type->Equals(arrow::large_utf8())) {
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::LargeStringArray>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        auto str = casted->GetView(k);
        std::string_view sw;
        if (casted->IsNull(k)) {
          VLOG(1) << "Found null string in vertex property.";
          sw = "";
        } else {
          sw = std::string_view(str.data(), str.size());
        }
        if (offset[cur_ind] >= size) {
          cur_ind++;
        } else {
          if (!enable_resize) {
            col->set_any(offset[cur_ind++], std::move(sw));
          } else {
            typed_col->set_value_safe(offset[cur_ind++], std::move(sw));
          }
        }
      }
    }
  } else {
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::StringArray>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        auto str = casted->GetView(k);
        std::string_view sw(str.data(), str.size());
        if (offset[cur_ind] >= size) {
          cur_ind++;
        } else {
          if (!enable_resize) {
            col->set_any(offset[cur_ind++], std::move(sw));
          } else {
            typed_col->set_value_safe(offset[cur_ind++], std::move(sw));
          }
        }
      }
    }
  }
}

void set_properties_column(gs::ColumnBase* col,
                           std::shared_ptr<arrow::ChunkedArray> array,
                           const std::vector<size_t>& offset) {
  auto type = array->type();
  auto col_type = col->type();

  // TODO(zhanglei): reduce the dummy code here with a template function.
  if (col_type == PropertyType::kBool) {
    set_column<bool>(col, array, offset);
  } else if (col_type == PropertyType::kInt64) {
    set_column<int64_t>(col, array, offset);
  } else if (col_type == PropertyType::kInt32) {
    set_column<int32_t>(col, array, offset);
  } else if (col_type == PropertyType::kUInt64) {
    set_column<uint64_t>(col, array, offset);
  } else if (col_type == PropertyType::kUInt32) {
    set_column<uint32_t>(col, array, offset);
  } else if (col_type == PropertyType::kDouble) {
    set_column<double>(col, array, offset);
  } else if (col_type == PropertyType::kFloat) {
    set_column<float>(col, array, offset);
  } else if (col_type == PropertyType::kDateTime) {
    set_column_from_timestamp_array<DateTime>(col, array, offset);
  } else if (col_type == PropertyType::kTimestamp) {
    set_column_from_timestamp_array<TimeStamp>(col, array, offset);
  } else if (col_type == PropertyType::kDate) {
    set_column_from_date_array(col, array, offset);
  } else if (col_type == PropertyType::kInterval) {
    set_interval_column_from_string_array(col, array, offset);
  } else if (col_type == PropertyType::kStringMap) {
    set_column_from_string_array(col, array, offset);
  } else if (col_type.type_enum == impl::PropertyTypeImpl::kVarChar) {
    set_column_from_string_array(col, array, offset);
  } else if (col_type == PropertyType::kStringView) {
    set_column_from_string_array(col, array, offset, true);
  } else {
    LOG(FATAL) << "Not support type: " << type->ToString();
  }
}

void set_interval_column_from_string_array(
    gs::ColumnBase* col, std::shared_ptr<arrow::ChunkedArray> array,
    const std::vector<size_t>& offset) {
  auto type = array->type();
  auto col_type = col->type();
  auto size = col->size();
  size_t cur_ind = 0;
  if (type->Equals(arrow::large_utf8())) {
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::LargeStringArray>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        if (offset[cur_ind] >= size) {
          cur_ind++;
        } else {
          col->set_any(
              offset[cur_ind++],
              std::move(AnyConverter<Interval>::to_any(casted->GetView(k))));
        }
      }
    }
  } else {
    LOG(FATAL) << "Not implemented: converting " << type->ToString() << " to "
               << col_type;
  }
}

void set_column_from_date_array(gs::ColumnBase* col,
                                std::shared_ptr<arrow::ChunkedArray> array,
                                const std::vector<size_t>& offset) {
  auto type = array->type();
  auto col_type = col->type();
  auto size = col->size();
  size_t cur_ind = 0;
  if (type->Equals(arrow::date32())) {
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::Date32Array>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        if (offset[cur_ind] >= size) {
          cur_ind++;
        } else {
          col->set_any(offset[cur_ind++],
                       std::move(AnyConverter<Date>::to_any(casted->Value(k))));
        }
      }
    }
  } else {
    LOG(FATAL) << "Not implemented: converting " << type->ToString() << " to "
               << col_type;
  }
}

void check_edge_invariant(
    const Schema& schema,
    const std::vector<std::tuple<size_t, std::string, std::string>>&
        column_mappings,
    size_t src_col_ind, size_t dst_col_ind, label_t src_label_i,
    label_t dst_label_i, label_t edge_label_i) {
  if (column_mappings.size() > 0) {
    auto& mapping = column_mappings[0];
    if (std::get<0>(mapping) == src_col_ind ||
        std::get<0>(mapping) == dst_col_ind) {
      LOG(FATAL) << "Edge column mappings must not contain src_col_ind or "
                    "dst_col_ind";
    }
    auto src_label_name = schema.get_vertex_label_name(src_label_i);
    auto dst_label_name = schema.get_vertex_label_name(dst_label_i);
    auto edge_label_name = schema.get_edge_label_name(edge_label_i);
    // check property exists in schema
    if (!schema.edge_has_property(src_label_name, dst_label_name,
                                  edge_label_name, std::get<2>(mapping))) {
      LOG(FATAL) << "property " << std::get<2>(mapping)
                 << " not exists in schema for edge triplet " << src_label_name
                 << " -> " << edge_label_name << " -> " << dst_label_name;
    }
  }
}

void AbstractArrowFragmentLoader::AddVerticesRecordBatch(
    label_t v_label_id, const std::vector<std::string>& v_files,
    std::function<std::vector<std::shared_ptr<IRecordBatchSupplier>>(
        label_t, const std::string&, const LoadingConfig&, int)>
        supplier_creator) {
  auto primary_keys = schema_.get_vertex_primary_key(v_label_id);

  if (primary_keys.size() != 1) {
    LOG(FATAL) << "Only support one primary key for vertex.";
  }
  auto type = std::get<0>(primary_keys[0]);
  if (type != PropertyType::kInt64 && type != PropertyType::kStringView &&
      type != PropertyType::kInt32 && type != PropertyType::kUInt32 &&
      type != PropertyType::kUInt64) {
    LOG(FATAL)
        << "Only support int64_t, uint64_t, int32_t, uint32_t and string "
           "primary key for vertex.";
  }
  std::string v_label_name = schema_.get_vertex_label_name(v_label_id);
  VLOG(10) << "Start init vertices for label " << v_label_name << " with "
           << v_files.size() << " files.";

  if (type == PropertyType::kInt64) {
    addVertexRecordBatchImpl<int64_t>(v_label_id, v_files, supplier_creator);
  } else if (type == PropertyType::kInt32) {
    addVertexRecordBatchImpl<int32_t>(v_label_id, v_files, supplier_creator);
  } else if (type == PropertyType::kUInt32) {
    addVertexRecordBatchImpl<uint32_t>(v_label_id, v_files, supplier_creator);
  } else if (type == PropertyType::kUInt64) {
    addVertexRecordBatchImpl<uint64_t>(v_label_id, v_files, supplier_creator);
  } else if (type.type_enum == impl::PropertyTypeImpl::kVarChar ||
             type.type_enum == impl::PropertyTypeImpl::kStringView) {
    addVertexRecordBatchImpl<std::string_view>(v_label_id, v_files,
                                               supplier_creator);
  } else {
    LOG(FATAL) << "Unsupported primary key type for vertex, type: " << type
               << ", label: " << v_label_name;
  }
  VLOG(10) << "Finish init vertices for label " << v_label_name;
}

void AbstractArrowFragmentLoader::AddEdgesRecordBatch(
    label_t src_label_i, label_t dst_label_i, label_t edge_label_i,
    const std::vector<std::string>& filenames,
    std::function<std::vector<std::shared_ptr<IRecordBatchSupplier>>(
        label_t, label_t, label_t, const std::string&, const LoadingConfig&,
        int)>
        supplier_creator) {
  auto src_label_name = schema_.get_vertex_label_name(src_label_i);
  auto dst_label_name = schema_.get_vertex_label_name(dst_label_i);
  auto edge_label_name = schema_.get_edge_label_name(edge_label_i);
  if (filenames.size() <= 0) {
    LOG(FATAL) << "No edge files found for src label: " << src_label_name
               << " dst label: " << dst_label_name
               << " edge label: " << edge_label_name;
  }
  if (filenames.size() <= 0) {
    LOG(FATAL) << "No edge files found for src label: " << src_label_name
               << " dst label: " << dst_label_name
               << " edge label: " << edge_label_name;
  }
  VLOG(10) << "Init edges src label: " << src_label_name
           << " dst label: " << dst_label_name
           << " edge label: " << edge_label_name
           << " filenames: " << filenames.size();
  auto& property_types = schema_.get_edge_properties(
      src_label_name, dst_label_name, edge_label_name);
  size_t col_num = property_types.size();
  if (col_num == 0) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<grape::EmptyType>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesRecordBatchImpl<grape::EmptyType>(
          src_label_i, dst_label_i, edge_label_i, filenames, supplier_creator);
    }
  } else if (col_num == 1) {
    if (property_types[0] == PropertyType::kBool) {
      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<bool>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<bool>(src_label_i, dst_label_i, edge_label_i,
                                      filenames, supplier_creator);
      }
    } else if (property_types[0] == PropertyType::kDate) {
      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<Date>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<Date>(src_label_i, dst_label_i, edge_label_i,
                                      filenames, supplier_creator);
      }
    } else if (property_types[0] == PropertyType::kInt32) {
      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<int32_t>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<int32_t>(src_label_i, dst_label_i, edge_label_i,
                                         filenames, supplier_creator);
      }
    } else if (property_types[0] == PropertyType::kUInt32) {
      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<uint32_t>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<uint32_t>(src_label_i, dst_label_i,
                                          edge_label_i, filenames,
                                          supplier_creator);
      }
    } else if (property_types[0] == PropertyType::kInt64) {
      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<int64_t>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<int64_t>(src_label_i, dst_label_i, edge_label_i,
                                         filenames, supplier_creator);
      }
    } else if (property_types[0] == PropertyType::kUInt64) {
      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<uint64_t>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<uint64_t>(src_label_i, dst_label_i,
                                          edge_label_i, filenames,
                                          supplier_creator);
      }
    } else if (property_types[0] == PropertyType::kDouble) {
      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<double>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<double>(src_label_i, dst_label_i, edge_label_i,
                                        filenames, supplier_creator);
      }
    } else if (property_types[0] == PropertyType::kFloat) {
      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<float>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<float>(src_label_i, dst_label_i, edge_label_i,
                                       filenames, supplier_creator);
      }
    } else if (property_types[0] == PropertyType::kTimestamp) {
      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<TimeStamp>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<TimeStamp>(src_label_i, dst_label_i,
                                           edge_label_i, filenames,
                                           supplier_creator);
      }

    } else if (property_types[0] == PropertyType::kDateTime) {
      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<DateTime>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<DateTime>(src_label_i, dst_label_i,
                                          edge_label_i, filenames,
                                          supplier_creator);
      }
    } else if (property_types[0].type_enum ==
                   impl::PropertyTypeImpl::kVarChar ||
               property_types[0].type_enum ==
                   impl::PropertyTypeImpl::kStringView) {
      // Both varchar and string are treated as string. For String, we use the
      // default max length defined in
      // PropertyType::GetStringDefaultMaxLength()

      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<std::string_view>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<std::string_view>(src_label_i, dst_label_i,
                                                  edge_label_i, filenames,
                                                  supplier_creator);
      }
    } else {
      LOG(FATAL) << "Unsupported edge property type." << property_types[0];
    }
  } else {
    if (filenames.empty()) {
      LOG(FATAL) << "No edge files found for src label: " << src_label_name
                 << " dst label: " << dst_label_name
                 << " edge label: " << edge_label_name;
    } else {
      addEdgesRecordBatchImpl<RecordView>(
          src_label_i, dst_label_i, edge_label_i, filenames, supplier_creator);
    }
  }
}

template <typename PK_T>
static void collectVertices(std::vector<Any>& ids, std::vector<size_t>& vids,
                            std::shared_ptr<arrow::Array> primary_key_column,
                            size_t local_offset) {
  size_t row_num = primary_key_column->length();
  if constexpr (!std::is_same<std::string_view, PK_T>::value &&
                !std::is_same<std::string, PK_T>::value) {
    auto expected_type = gs::TypeConverter<PK_T>::ArrowTypeValue();
    using arrow_array_t = typename gs::TypeConverter<PK_T>::ArrowArrayType;
    if (!primary_key_column->type()->Equals(expected_type)) {
      LOG(FATAL) << "Inconsistent data type, expect "
                 << expected_type->ToString() << ", but got "
                 << primary_key_column->type()->ToString();
    }
    auto casted_array =
        std::static_pointer_cast<arrow_array_t>(primary_key_column);

    for (size_t j = 0; j < row_num; ++j) {
      vid_t vid = local_offset + j;
      vids.emplace_back(vid);
      ids[vid] = Any(static_cast<PK_T>(casted_array->Value(j)));
    }
  } else {
    if (primary_key_column->type()->Equals(arrow::utf8())) {
      auto casted_array =
          std::static_pointer_cast<arrow::StringArray>(primary_key_column);
      for (size_t j = 0; j < row_num; ++j) {
        vid_t vid = local_offset + j;
        vids.emplace_back(vid);
        auto str = casted_array->GetView(j);
        ids[vid] = Any(std::string(str));
      }
    } else if (primary_key_column->type()->Equals(arrow::large_utf8())) {
      auto casted_array =
          std::static_pointer_cast<arrow::LargeStringArray>(primary_key_column);
      for (size_t j = 0; j < row_num; ++j) {
        vid_t vid = local_offset + j;
        vids.emplace_back(vid);
        auto str = casted_array->GetView(j);
        ids[vid] = Any(std::string(str));
      }
    } else {
      LOG(FATAL) << "Not support type: "
                 << primary_key_column->type()->ToString();
    }
  }
}

gs::result<std::pair<std::vector<Any>, std::unique_ptr<Table>>>
AbstractArrowFragmentLoader::batch_load_vertices(
    const Schema& schema, const std::string& work_dir,
    const label_t& v_label_id,
    std::vector<std::shared_ptr<IRecordBatchSupplier>>&
        record_batch_supplier_vec) {
  std::string vertex_type_name = schema.get_vertex_label_name(v_label_id);
  auto schema_column_names = schema.get_vertex_property_names(v_label_id);
  auto primary_key = schema.get_vertex_primary_key(v_label_id)[0];
  auto primary_key_name = std::get<1>(primary_key);
  auto primary_key_type = std::get<0>(primary_key);
  size_t primary_key_ind = std::get<2>(primary_key);
  std::unique_ptr<Table> table = std::make_unique<Table>();
  std::vector<Any> ids;
  LOG(INFO) << "Work directory for vertex table: " << work_dir;
  if (!std::filesystem::exists(work_dir)) {
    std::filesystem::create_directories(work_dir);
  }

  table->init(vertex_type_name, work_dir, schema_column_names,
              schema.get_vertex_properties(v_label_id),
              schema.get_vertex_storage_strategies(vertex_type_name));
  std::shared_mutex rw_mutex;
  std::atomic<size_t> offset(0);

  auto supplier_creator =
      [&record_batch_supplier_vec](const std::string& file) {
        return record_batch_supplier_vec;
      };

  auto func = [&](const arrow::ArrayVector& columns, size_t) {
    auto primary_key_column = columns[primary_key_ind];
    auto other_columns_array = columns;
    other_columns_array.erase(other_columns_array.begin() + primary_key_ind);

    auto local_offset = offset.fetch_add(primary_key_column->length());
    size_t cur_row_num = std::max(table->row_num(), 1ul);
    while (cur_row_num < local_offset + primary_key_column->length()) {
      cur_row_num *= 2;
    }
    if (cur_row_num > table->row_num() || ids.size() < offset.load()) {
      std::unique_lock<std::shared_mutex> lock(rw_mutex);
      if (ids.size() < offset.load()) {
        ids.resize(offset.load());
      }
      if (cur_row_num > table->row_num()) {
        table->resize(cur_row_num);
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
      std::shared_lock<std::shared_mutex> lock(rw_mutex);
      if (primary_key_type == PropertyType::kInt64) {
        collectVertices<int64_t>(ids, vids, primary_key_column, local_offset);
      } else if (primary_key_type == PropertyType::kInt32) {
        collectVertices<int32_t>(ids, vids, primary_key_column, local_offset);
      } else if (primary_key_type == PropertyType::kUInt32) {
        collectVertices<uint32_t>(ids, vids, primary_key_column, local_offset);
      } else if (primary_key_type == PropertyType::kUInt64) {
        collectVertices<uint64_t>(ids, vids, primary_key_column, local_offset);
      } else if (primary_key_type.type_enum ==
                     impl::PropertyTypeImpl::kVarChar ||
                 primary_key_type.type_enum ==
                     impl::PropertyTypeImpl::kStringView) {
        collectVertices<std::string_view>(ids, vids, primary_key_column,
                                          local_offset);
      } else {
        LOG(FATAL) << "Unsupported primary key type: "
                   << primary_key_type.ToString();
      }
    }

    // Set other columns
    {
      std::shared_lock<std::shared_mutex> lock(rw_mutex);
      for (size_t j = 0; j < other_columns_array.size(); j++) {
        auto chunked_array =
            std::make_shared<arrow::ChunkedArray>(other_columns_array[j]);
        auto& column = table->column_ptrs()[j];
        set_properties_column(column, chunked_array, vids);
      }
    }
  };
  std::vector<std::string> files{""};
  AbstractArrowFragmentLoader::BatchConsumer(
      files, schema_column_names.size() + 1, supplier_creator, std::move(func));

  return std::make_pair(std::move(ids), std::move(table));
}

gs::result<std::pair<std::vector<std::tuple<vid_t, vid_t, size_t>>,
                     std::unique_ptr<Table>>>
AbstractArrowFragmentLoader::batch_load_edges(
    const Schema& schema, const std::string& work_dir,
    const label_t& src_v_label, const label_t& dst_v_label,
    const label_t& edge_label, std::function<vid_t(const Any&)> src_indexer,
    std::function<vid_t(const Any&)> dst_indexer, size_t src_v_num,
    size_t dst_v_num,
    std::vector<std::shared_ptr<IRecordBatchSupplier>>&
        record_batch_supplier_vec) {
  std::string src_label_name = schema.get_vertex_label_name(src_v_label);
  std::string dst_label_name = schema.get_vertex_label_name(dst_v_label);
  std::string edge_type_name = schema.get_edge_label_name(edge_label);
  auto src_pks = schema.get_vertex_primary_key(src_v_label);
  auto dst_pks = schema.get_vertex_primary_key(dst_v_label);
  if (src_pks.size() != 1 || dst_pks.size() != 1) {
    THROW_INTERNAL_EXCEPTION(
        "Only support one primary key for src or dst vertex.");
  }
  auto src_pk_property_type = std::get<0>(src_pks[0]);
  auto dst_pk_property_type = std::get<0>(dst_pks[0]);

  std::vector<std::vector<std::tuple<vid_t, vid_t, size_t>>> parsed_edges_vec(
      std::thread::hardware_concurrency());
  std::unique_ptr<Table> table = std::make_unique<Table>();
  if (!std::filesystem::exists(work_dir)) {
    std::filesystem::create_directories(work_dir);
  }

  table->init(
      edata_prefix(src_label_name, dst_label_name, edge_type_name), work_dir,
      schema.get_edge_property_names(src_v_label, dst_v_label, edge_label),
      schema.get_edge_properties(src_v_label, dst_v_label, edge_label), {});

  std::atomic<size_t> offset(0);
  std::shared_mutex rw_mutex;

  std::vector<std::atomic<int32_t>> ie_degree(dst_v_num), oe_degree(src_v_num);
  for (size_t idx = 0; idx < ie_degree.size(); ++idx) {
    ie_degree[idx].store(0);
  }
  for (size_t idx = 0; idx < oe_degree.size(); ++idx) {
    oe_degree[idx].store(0);
  }

  auto func = [&](const arrow::ArrayVector& columns, size_t idx) {
    // We assume the src_col and dst_col will always be put
    // at front.
    auto& parsed_edges = parsed_edges_vec[idx];
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
    {
      CHECK(table->col_num() == property_cols.size());
      offset_i = offset.fetch_add(src_col->length());
      std::vector<size_t> offsets;
      for (size_t _i = 0; _i < static_cast<size_t>(src_col->length()); ++_i) {
        offsets.emplace_back(offset_i + _i);
      }
      size_t row_num = std::max(table->row_num(), 1ul);

      while (row_num < offset_i + src_col->length()) {
        row_num *= 2;
      }
      if (row_num > table->row_num()) {
        std::unique_lock<std::shared_mutex> lock(rw_mutex);
        if (row_num > table->row_num()) {
          table->resize(row_num);
        }
      }

      {
        std::shared_lock<std::shared_mutex> lock(rw_mutex);
        for (size_t i = 0; i < table->col_num(); ++i) {
          auto col = table->get_column_by_id(i);
          auto chunked_array =
              std::make_shared<arrow::ChunkedArray>(property_cols[i]);
          set_properties_column(col.get(), chunked_array, offsets);
        }
      }
    }

    AbstractArrowFragmentLoader::append_edges_utils<
        RecordView, std::vector<std::tuple<vid_t, vid_t, size_t>>>(
        src_col, dst_col, src_indexer, src_pk_property_type, dst_indexer,
        dst_pk_property_type, property_cols[0], parsed_edges, ie_degree,
        oe_degree, offset_i);
  };

  auto supplier_creator =
      [&record_batch_supplier_vec](const std::string& file) {
        return record_batch_supplier_vec;
      };

  std::vector<std::string> files{""};
  AbstractArrowFragmentLoader::BatchConsumer(
      files,
      schema.get_edge_property_names(src_v_label, dst_v_label, edge_label)
              .size() +
          2,
      supplier_creator, std::move(func));

  // Merge all parsed edges
  std::vector<std::tuple<vid_t, vid_t, size_t>> parsed_edges;
  parsed_edges.reserve(offset.load());
  for (auto& edges_vec : parsed_edges_vec) {
    for (auto edges : edges_vec) {
      if (std::get<0>(edges) == std::numeric_limits<vid_t>::max() ||
          std::get<1>(edges) == std::numeric_limits<vid_t>::max()) {
        continue;
      }
      parsed_edges.push_back(edges);
    }
  }
  return std::make_pair(std::move(parsed_edges), std::move(table));
}

}  // namespace gs
