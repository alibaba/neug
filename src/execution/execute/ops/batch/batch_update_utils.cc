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

#include "neug/execution/execute/ops/batch/batch_update_utils.h"

#include <arrow/csv/options.h>
#include <arrow/type.h>
#include <arrow/util/value_parsing.h>
#include <glob.h>
#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <stddef.h>
#include <cstdint>
#include <ostream>
#include <stdexcept>
#include <tuple>

#include "neug/execution/common/columns/arrow_context_column.h"
#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/context.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/arrow_utils.h"
#include "neug/utils/string_utils.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace neug {

namespace execution {

namespace ops {

void put_column_types_option(const std::vector<DataType>& column_types,
                             std::vector<std::string>& column_names,
                             arrow::csv::ConvertOptions& convert_options) {
  if (column_types.size() != column_names.size()) {
    THROW_RUNTIME_ERROR("Column types size does not match column names size: " +
                        std::to_string(column_types.size()) + " vs " +
                        std::to_string(column_names.size()));
  }
  for (size_t i = 0; i < column_types.size(); ++i) {
    const auto& col_name = column_names[i];
    if (convert_options.column_types.find(col_name) !=
        convert_options.column_types.end()) {
      THROW_RUNTIME_ERROR("Duplicate column name found: " + col_name);
    }
    convert_options.column_types.insert(
        {col_name, neug::PropertyTypeToArrowType(column_types[i])});
  }
}

bool check_csv_import_options(
    const std::unordered_map<std::string, std::string>& options) {
  std::unordered_set<std::string> valid_keys = {
      CSV_DELIMITER_KEY,    CSV_DELIM_KEY,  CSV_HEADER_KEY, CSV_QUOTE_KEY,
      CSV_DOUBLE_QUOTE_KEY, CSV_ESCAPE_KEY, CSV_SKIP_KEY,   CSV_PARALLEL_KEY};
  int32_t delim_count = 0;
  for (const auto& [key, value] : options) {
    if (valid_keys.find(key) == valid_keys.end()) {
      LOG(ERROR) << "\"" << key << "\" is not a valid parameter";
      return false;
    }
    if (key == CSV_DELIMITER_KEY || key == CSV_DELIM_KEY) {
      delim_count++;
    }
  }
  if (delim_count >= 2) {
    LOG(ERROR) << "Too many \"DELIMITER\" parameters";
  }
  return true;
}

void add_member(rapidjson::Value& object,
                rapidjson::Document::AllocatorType& allocator,
                const std::string& key, Property value) {
  if (value.type() == DataTypeId::kBoolean) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.as_bool(), allocator);
  } else if (value.type() == DataTypeId::kInt32) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.as_int32(), allocator);
  } else if (value.type() == DataTypeId::kUInt32) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.as_uint32(), allocator);
  } else if (value.type() == DataTypeId::kInt64) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.as_int64(), allocator);
  } else if (value.type() == DataTypeId::kUInt64) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.as_uint64(), allocator);
  } else if (value.type() == DataTypeId::kFloat) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.as_float(), allocator);
  } else if (value.type() == DataTypeId::kDouble) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.as_double(), allocator);
  } else if (value.type() == DataTypeId::kDate) {
    std::string date = value.as_date().to_string();
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     rapidjson::Value(date.c_str(), allocator).Move(),
                     allocator);
  } else if (value.type() == DataTypeId::kTimestampMs) {
    std::string date_time = value.as_datetime().to_string();
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     rapidjson::Value(date_time.c_str(), allocator).Move(),
                     allocator);
  } else if (value.type() == DataTypeId::kVarchar) {
    rapidjson::Value valueVal;
    auto str_value = value.as_string_view();
    valueVal.SetString(str_value.data(), str_value.size(), allocator);
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(), valueVal,
                     allocator);
  } else if (value.type() == DataTypeId::kInterval) {
    std::string interval_str = value.as_interval().to_string();
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     rapidjson::Value(interval_str.c_str(), allocator).Move(),
                     allocator);
  } else {
    THROW_RUNTIME_ERROR("Unsupported property type for key: " + key);
  }
}

void add_prop_member(rapidjson::Value& object,
                     rapidjson::Document::AllocatorType& allocator,
                     const std::string& key, Property value) {
  if (value.type() == DataTypeId::kInt32) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.as_int32(), allocator);
  } else if (value.type() == DataTypeId::kUInt32) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.as_uint32(), allocator);
  } else if (value.type() == DataTypeId::kInt64) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.as_int64(), allocator);
  } else if (value.type() == DataTypeId::kUInt64) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.as_uint64(), allocator);
  } else if (value.type() == DataTypeId::kFloat) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.as_float(), allocator);
  } else if (value.type() == DataTypeId::kDouble) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.as_double(), allocator);
  } else if (value.type() == DataTypeId::kDate) {
    std::string date = value.as_date().to_string();
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     rapidjson::Value(date.c_str(), allocator).Move(),
                     allocator);
  } else if (value.type() == DataTypeId::kTimestampMs) {
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     value.as_datetime().milli_second, allocator);
  } else if (value.type() == DataTypeId::kInterval) {
    std::string interval_str = value.as_interval().to_string();
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(),
                     rapidjson::Value(interval_str.c_str(), allocator).Move(),
                     allocator);
  } else if (value.type() == DataTypeId::kVarchar) {
    rapidjson::Value valueVal;
    auto str_value = value.as_string_view();
    valueVal.SetString(str_value.data(), str_value.size(), allocator);
    object.AddMember(rapidjson::Value(key.c_str(), allocator).Move(), valueVal,
                     allocator);
  } else {
    THROW_NOT_IMPLEMENTED_EXCEPTION("Unsupported property type for key: " +
                                    key);
  }
}

rapidjson::Value build_vertex_object(
    label_t label, vid_t vid, const StorageReadInterface& graph,
    rapidjson::Document::AllocatorType& allocator) {
  rapidjson::Value vertex_object(rapidjson::kObjectType);
  std::string internal_id_key = "_ID";
  std::string encoded_id_str =
      std::to_string(label) + ":" + std::to_string(vid);
  Property encoded_id = Property::from_string_view(encoded_id_str);
  add_member(vertex_object, allocator, internal_id_key, encoded_id);
  std::string internal_label_key = "_LABEL";
  std::string label_name_str = graph.schema().get_vertex_label_name(label);
  Property label_name = Property::from_string_view(label_name_str);
  add_member(vertex_object, allocator, internal_label_key, label_name);
  std::string primary_key = graph.schema().get_vertex_primary_key_name(label);
  add_member(vertex_object, allocator, primary_key,
             graph.GetVertexId(label, vid));
  auto property_names = graph.schema().get_vertex_property_names(label);
  for (size_t i = 0; i < property_names.size(); i++) {
    add_member(vertex_object, allocator, property_names[i],
               graph.GetVertexProperty(label, vid, i));
  }
  return vertex_object;
}

std::string vertex_to_json_string(label_t label, vid_t vid,
                                  const StorageReadInterface& graph) {
  rapidjson::Document doc;
  auto& allocator = doc.GetAllocator();
  rapidjson::Value vertex_object =
      build_vertex_object(label, vid, graph, allocator);
  vertex_object.Swap(doc);
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);
  return buffer.GetString();
}

rapidjson::Value build_edge_object(
    const EdgeRecord& edge, const StorageReadInterface& graph,
    rapidjson::Document::AllocatorType& allocator) {
  rapidjson::Value edge_object(rapidjson::kObjectType);
  label_t src_label = edge.label.src_label;
  label_t dst_label = edge.label.dst_label;
  label_t edge_label = edge.label.edge_label;
  std::string internal_src_id = "_SRC";
  std::string encoded_src_id_str =
      std::to_string(src_label) + ":" + std::to_string(edge.src);
  Property encoded_src_id = Property::from_string_view(encoded_src_id_str);
  add_member(edge_object, allocator, internal_src_id, encoded_src_id);

  std::string internal_dst_id = "_DST";
  std::string encoded_dst_id_str =
      std::to_string(dst_label) + ":" + std::to_string(edge.dst);
  Property encoded_dst_id = Property::from_string_view(encoded_dst_id_str);
  add_member(edge_object, allocator, internal_dst_id, encoded_dst_id);

  std::string internal_src_label_key = "_SRC_LABEL";
  Property src_label_name = Property::from_string_view(
      graph.schema().get_vertex_label_name(src_label));
  add_member(edge_object, allocator, internal_src_label_key, src_label_name);

  std::string internal_dst_label_key = "_DST_LABEL";
  Property dst_label_name = Property::from_string_view(
      graph.schema().get_vertex_label_name(dst_label));
  add_member(edge_object, allocator, internal_dst_label_key, dst_label_name);

  std::string internal_label_key = "_LABEL";
  Property edge_label_name = Property::from_string_view(
      graph.schema().get_edge_label_name(edge_label));
  add_member(edge_object, allocator, internal_label_key, edge_label_name);

  auto property_names =
      graph.schema().get_edge_property_names(src_label, dst_label, edge_label);
  for (size_t i = 0; i < property_names.size(); i++) {
    auto ed_accessor =
        graph.GetEdgeDataAccessor(src_label, dst_label, edge_label, i);
    add_prop_member(edge_object, allocator, property_names[i],
                    ed_accessor.get_data_from_ptr(edge.prop));
  }
  return edge_object;
}

std::string edge_to_json_string(const EdgeRecord& edge,
                                const StorageReadInterface& graph) {
  rapidjson::Document doc;
  auto& allocator = doc.GetAllocator();
  auto edge_object = build_edge_object(edge, graph, allocator);
  edge_object.Swap(doc);
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);
  return buffer.GetString();
}

std::string path_to_json_string(Path& path, const StorageReadInterface& graph) {
  rapidjson::Document doc;
  doc.SetObject();
  auto& allocator = doc.GetAllocator();
  rapidjson::Value vertex_array(rapidjson::kArrayType);
  rapidjson::Value edge_array(rapidjson::kArrayType);
  auto path_vertices = path.nodes();
  auto path_edges = path.relationships();
  for (size_t i = 0; i < path_vertices.size(); i++) {
    auto vertex_object = build_vertex_object(
        path_vertices[i].label_, path_vertices[i].vid_, graph, allocator);
    vertex_array.PushBack(vertex_object, allocator);
    if (i > 0) {
      rapidjson::Value edge_object(rapidjson::kObjectType);
      std::string internal_src_label_key = "_SRC_LABEL";
      Property src_label_name = Property::from_string_view(
          graph.schema().get_vertex_label_name(path_vertices[i - 1].label_));
      add_member(edge_object, allocator, internal_src_label_key,
                 src_label_name);
      std::string internal_dst_label_key = "_DST_LABEL";
      Property dst_label_name = Property::from_string_view(
          graph.schema().get_vertex_label_name(path_vertices[i].label_));
      add_member(edge_object, allocator, internal_dst_label_key,
                 dst_label_name);
      std::string internal_label_key = "_LABEL";
      Property edge_label_name =
          Property::from_string_view(graph.schema().get_edge_label_name(
              path_edges[i - 1].label.edge_label));
      add_member(edge_object, allocator, internal_label_key, edge_label_name);
      edge_array.PushBack(edge_object, allocator);
    }
  }
  doc.AddMember("_nodes", vertex_array, allocator);
  doc.AddMember("_rels", edge_array, allocator);
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);
  return buffer.GetString();
}

std::vector<std::shared_ptr<IRecordBatchSupplier>>
create_record_batch_supplier_from_arrow_array_column(
    const Context& ctx,
    const std::vector<std::pair<int32_t, std::string>>& prop_mappings) {
  std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers;
  std::vector<std::vector<std::shared_ptr<arrow::Array>>> arrays;
  std::vector<std::shared_ptr<arrow::Field>> fields;

  arrays.resize(prop_mappings.size());
  for (size_t i = 0; i < prop_mappings.size(); ++i) {
    auto mapping = prop_mappings[i];
    auto tag_id = mapping.first;
    auto prop_name = mapping.second;
    auto column = ctx.get(tag_id);
    if (column == nullptr) {
      LOG(FATAL) << "Column not found for tag id: " << tag_id;
    }
    auto arrow_column =
        std::dynamic_pointer_cast<ArrowArrayContextColumn>(column);
    if (!arrow_column) {
      LOG(FATAL) << "Invalid column type for tag id: " << tag_id;
    }

    auto& column_arrays = arrow_column->GetColumns();
    // arrays[i].emplace(column_arrays.begin(), column_arrays.end());
    for (auto& array : column_arrays) {
      arrays[i].emplace_back(array);
    }
    fields.emplace_back(std::make_shared<arrow::Field>(
        prop_name, arrow_column->GetArrowType(), true));
  }
  if (!arrays.empty()) {
    size_t batch_size = arrays[0].size();
    for (size_t i = 1; i < arrays.size(); ++i) {
      auto& array = arrays[i];
      if (array.size() != batch_size) {
        LOG(FATAL) << "Array size mismatch for tag id: "
                   << prop_mappings[i].first;
      }
    }
  }
  auto schema = std::make_shared<arrow::Schema>(fields);
  suppliers.emplace_back(
      std::make_shared<ArrowRecordBatchArraySupplier>(arrays, schema));
  return suppliers;
}

std::vector<std::shared_ptr<IRecordBatchSupplier>>
create_record_batch_supplier_from_arrow_stream_column(
    const Context& ctx,
    const std::vector<std::pair<int32_t, std::string>>& prop_mappings) {
  for (const auto& mapping : prop_mappings) {
    auto tag_id = mapping.first;
    auto column = ctx.get(tag_id);
    if (column == nullptr) {
      LOG(ERROR) << "Column not found for tag id: " << tag_id;
      THROW_RUNTIME_ERROR("Column not found for tag id: " +
                          std::to_string(tag_id));
    }
    if (column->column_type() != ContextColumnType::kArrowStream) {
      LOG(ERROR) << "Invalid column type for tag id: " << tag_id;
      THROW_RUNTIME_ERROR("Invalid column type for tag id: " +
                          std::to_string(tag_id));
    }
    auto casted_column =
        std::dynamic_pointer_cast<ArrowStreamContextColumn>(column);
    if (!casted_column) {
      LOG(ERROR) << "Failed to cast column for tag id: " << tag_id;
      THROW_RUNTIME_ERROR("Failed to cast column for tag id: " +
                          std::to_string(tag_id));
    }
    return casted_column->GetSuppliers();
  }
  LOG(ERROR) << "No valid column mappings found.";
  THROW_RUNTIME_ERROR("No valid column mappings found.");
}

std::vector<std::shared_ptr<IRecordBatchSupplier>> create_record_batch_supplier(
    const Context& ctx,
    const std::vector<std::pair<int32_t, std::string>>& prop_mappings) {
  // We expect all columns are of same type.
  ContextColumnType column_type = ContextColumnType::kNone;
  for (const auto& mapping : prop_mappings) {
    auto tag_id = mapping.first;
    auto column = ctx.get(tag_id);
    if (column == nullptr) {
      LOG(ERROR) << "Column not found for tag id: " << tag_id;
      THROW_RUNTIME_ERROR("Column not found for tag id: " +
                          std::to_string(tag_id));
    }
    if (column_type == ContextColumnType::kNone) {
      column_type = column->column_type();
    } else if (column_type != column->column_type()) {
      LOG(ERROR) << "Column type mismatch for tag id: " << tag_id;
      THROW_RUNTIME_ERROR("Column type mismatch for tag id: " +
                          std::to_string(tag_id));
    }
  }
  if (column_type == ContextColumnType::kArrowArray) {
    return create_record_batch_supplier_from_arrow_array_column(ctx,
                                                                prop_mappings);
  } else if (column_type == ContextColumnType::kArrowStream) {
    return create_record_batch_supplier_from_arrow_stream_column(ctx,
                                                                 prop_mappings);
  } else {
    LOG(ERROR) << "Unsupported column type: " << static_cast<int>(column_type);
    THROW_RUNTIME_ERROR("Unsupported column type: " +
                        std::to_string(static_cast<int>(column_type)));
  }
}

void to_arrow_csv_options(
    const std::string& file_path,
    const std::unordered_map<std::string, std::string>& csv_options,
    const std::vector<DataType>& column_types,
    arrow::csv::ConvertOptions& convert_options,
    arrow::csv::ReadOptions& read_options,
    arrow::csv::ParseOptions& parse_options) {
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCTimeStampParser>());
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCLongDateParser>());
  convert_options.timestamp_parsers.emplace_back(
      arrow::TimestampParser::MakeISO8601());
  // BOOLEAN parser
  put_boolean_option(convert_options);
  if (csv_options.find(CSV_DELIMITER_KEY) != csv_options.end()) {
    put_delimiter_option(csv_options.at(CSV_DELIMITER_KEY), parse_options);
  } else if (csv_options.find(CSV_DELIM_KEY) != csv_options.end()) {
    put_delimiter_option(csv_options.at(CSV_DELIM_KEY), parse_options);
  } else {
    VLOG(10) << "Using default CSV delimiter: " << DEFAULT_CSV_DELIMITER;
    put_delimiter_option(DEFAULT_CSV_DELIMITER, parse_options);
  }
  if (csv_options.find(CSV_ESCAPE_KEY) != csv_options.end()) {
    if (csv_options.at(CSV_ESCAPE_KEY).size() == 1) {
      parse_options.escaping = true;
      parse_options.escape_char = csv_options.at(CSV_ESCAPE_KEY)[0];
    } else {
      LOG(ERROR) << "Invalid escape char: "
                 << csv_options.at(CSV_ESCAPE_KEY)[0];
      parse_options.escaping = false;
    }
  }
  if (csv_options.find(CSV_QUOTE_KEY) != csv_options.end()) {
    if (csv_options.at(CSV_QUOTE_KEY).size() == 1) {
      parse_options.quoting = true;
      parse_options.double_quote = false;
      parse_options.quote_char = csv_options.at(CSV_QUOTE_KEY)[0];
      VLOG(10) << "Using CSV quote char: " << csv_options.at(CSV_QUOTE_KEY)[0];
    } else {
      LOG(ERROR) << "Invalid quote char: " << csv_options.at(CSV_QUOTE_KEY);
      parse_options.quoting = false;
    }
  }

  if (csv_options.find(CSV_DOUBLE_QUOTE_KEY) != csv_options.end()) {
    if (!parse_options.quoting) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "CSV quoting must be enabled for double quotes");
    }
    auto value = csv_options.at(CSV_DOUBLE_QUOTE_KEY);
    if (value == "true" || value == "1" || value == "TRUE") {
      parse_options.double_quote = true;
      VLOG(10) << "using double quote";
    } else {
      LOG(ERROR) << "Invalid double quote config: " << value;
      parse_options.double_quote = false;
    }
  }

  bool header_row = true;
  if (csv_options.find(CSV_HEADER_KEY) != csv_options.end()) {
    // check lower-case
    auto val = to_lower_copy(csv_options.at(CSV_HEADER_KEY));
    if (val == "false" || val == "0") {
      header_row = false;
    } else if (val != "true" && val != "1") {
      LOG(WARNING) << "Invalid value for CSV_HEADER_KEY: "
                   << csv_options.at(CSV_HEADER_KEY)
                   << ". Defaulting to true (header row enabled).";
    }
  } else {
    VLOG(10) << "Using default CSV header row: true";
  }
  put_column_names_option(header_row, file_path, parse_options.delimiter,
                          parse_options.quoting, parse_options.quote_char,
                          parse_options.escaping, parse_options.escape_char,
                          read_options, column_types.size());
  if (read_options.column_names.size() != column_types.size()) {
    THROW_SCHEMA_MISMATCH("Schema mismatch: column names size (" +
                          std::to_string(read_options.column_names.size()) +
                          ") does not match column types size (" +
                          std::to_string(column_types.size()) + ")");
  }
  // Currently we assume the column_types are corresponding to column names
  put_column_types_option(column_types, read_options.column_names,
                          convert_options);

  if (header_row) {
    read_options.skip_rows = 1;
  }

  if (csv_options.find(CSV_SKIP_KEY) != csv_options.end()) {
    LOG(WARNING) << "The parameter \"" << ops::CSV_SKIP_KEY
                 << "\" is currently not supported.";
  }

  if (csv_options.find(CSV_PARALLEL_KEY) != csv_options.end()) {
    LOG(WARNING) << "The parameter \"" << ops::CSV_PARALLEL_KEY
                 << "\" is currently not supported.";
  }

  if (csv_options.find(CSV_NULL_STRINGS_KEY) != csv_options.end()) {
    LOG(WARNING) << "The parameter \"" << ops::CSV_NULL_STRINGS_KEY
                 << "\" is currently not supported.";
  }

  // TODO(zhanglei): support selecting included columns.
}

std::vector<std::string> match_files_with_pattern(
    const std::string& file_path) {
  std::vector<std::string> result;
  if (file_path.find('*') != std::string::npos ||
      file_path.find('?') != std::string::npos) {
    glob_t glob_result;
    int flags = GLOB_TILDE | GLOB_MARK;
    int ret = glob(file_path.c_str(), flags, nullptr, &glob_result);
    if (ret == 0) {
      for (size_t i = 0; i < glob_result.gl_pathc; ++i) {
        result.push_back(glob_result.gl_pathv[i]);
      }
    }
    globfree(&glob_result);
  } else {
    std::filesystem::path p = std::filesystem::absolute(file_path);
    if (!std::filesystem::exists(p)) {
      THROW_IO_EXCEPTION("Provided path is not a file: " + file_path + ".");
    }
    result.emplace_back(file_path);
  }
  return result;
}

std::vector<std::shared_ptr<IRecordBatchSupplier>> create_csv_record_suppliers(
    const std::string& file_path, const std::vector<DataType>& column_types,
    const std::unordered_map<std::string, std::string> csv_options) {
  std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers;
  std::vector<std::string> file_paths = match_files_with_pattern(file_path);

  for (auto& path : file_paths) {
    arrow::csv::ConvertOptions convert_options;
    arrow::csv::ReadOptions read_options;
    arrow::csv::ParseOptions parse_options;
    to_arrow_csv_options(path, csv_options, column_types, convert_options,
                         read_options, parse_options);

    bool stream_reader = true;
    if (csv_options.find(CSV_STREAM_READER) != csv_options.end()) {
      // check lower-case
      auto val = to_lower_copy(csv_options.at(CSV_STREAM_READER));
      if (val == "false" || val == "0") {
        stream_reader = false;
      } else if (val != "true" && val != "1") {
        LOG(WARNING) << "Invalid value for CSV_STREAM_READER: "
                     << csv_options.at(CSV_STREAM_READER)
                     << ". Defaulting to true (stream reader enabled).";
      }
    }

    if (stream_reader) {
      suppliers.emplace_back(std::dynamic_pointer_cast<IRecordBatchSupplier>(
          std::make_shared<CSVStreamRecordBatchSupplier>(
              path, convert_options, read_options, parse_options)));
    } else {
      suppliers.emplace_back(std::dynamic_pointer_cast<IRecordBatchSupplier>(
          std::make_shared<CSVTableRecordBatchSupplier>(
              path, convert_options, read_options, parse_options)));
    }
  }
  return suppliers;
}

void parse_property_mappings(
    const google::protobuf::RepeatedPtrField<physical::PropertyMapping>&
        property_mappings,
    std::vector<std::pair<int32_t, std::string>>& prop_mappings) {
  for (const auto& mapping : property_mappings) {
    if (mapping.has_property() && mapping.property().has_key()) {
      auto prop_name = mapping.property().key().name();
      if (mapping.data().operators_size() != 1 ||
          !mapping.data().operators(0).has_var()) {
        LOG(FATAL) << "Invalid property mapping: " << prop_name;
      }
      auto tag_id = mapping.data().operators(0).var().tag().id();
      prop_mappings.emplace_back(tag_id, prop_name);
    }
  }
}
}  // namespace ops

}  // namespace execution

}  // namespace neug
