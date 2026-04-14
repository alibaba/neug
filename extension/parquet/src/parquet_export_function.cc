/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "parquet_export_function.h"

#include <algorithm>
#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <glog/logging.h>
#include <parquet/properties.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "neug/compiler/main/metadata_registry.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/types.h"
#include "neug/utils/writer/writer.h"
#include "parquet_options.h"

namespace neug {
namespace writer {

// Parse writer options and build WriterProperties
static std::shared_ptr<parquet::WriterProperties> buildWriterProperties(
    const common::case_insensitive_map_t<std::string>& options) {
  reader::ParquetExportOptions export_options;
  
  // Parse compression
  std::string codec = export_options.compression.get(options);
  std::transform(codec.begin(), codec.end(), codec.begin(), ::tolower);
  
  arrow::Compression::type compression;
  if (codec == "none" || codec == "uncompressed") {
    compression = arrow::Compression::UNCOMPRESSED;
  } else if (codec == "snappy") {
    compression = arrow::Compression::SNAPPY;
  } else if (codec == "zlib" || codec == "gzip") {
    compression = arrow::Compression::GZIP;
  } else if (codec == "zstd" || codec == "zstandard") {
    compression = arrow::Compression::ZSTD;
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Unsupported compression codec: " + codec + 
        ". Supported: none, snappy, zlib, zstd");
  }
  
  // Parse row group size
  int64_t row_group_size = export_options.row_group_size.get(options);
  
  if (row_group_size < 1024) {
    LOG(WARNING) << "Very small row_group_size (" << row_group_size 
                 << ") may result in many small row groups and poor compression.";
  } else if (row_group_size > 10000000) {
    LOG(WARNING) << "Very large row_group_size (" << row_group_size 
                 << ") may increase memory usage significantly.";
  }
  
  // Parse dictionary encoding
  bool dictionary_encoding = export_options.dictionary_encoding.get(options);
  
  LOG(INFO) << "Parquet export options: compression=" << compression 
            << ", row_group_size=" << row_group_size 
            << ", dictionary_encoding=" << dictionary_encoding;
  
  // Build WriterProperties
  parquet::WriterProperties::Builder builder;
  builder.compression(compression);
  builder.max_row_group_length(row_group_size);
  
  if (dictionary_encoding) {
    builder.enable_dictionary();
  } else {
    builder.disable_dictionary();
  }
  
  return builder.build();
}

// Helper function to infer Arrow type from JSON value
static std::shared_ptr<arrow::DataType> inferArrowTypeFromJsonValue(
    const rapidjson::Value& json_val) {
  if (json_val.IsNull()) {
    return arrow::null();
  } else if (json_val.IsBool()) {
    return arrow::boolean();
  } else if (json_val.IsInt() || json_val.IsInt64()) {
    return arrow::int64();
  } else if (json_val.IsUint() || json_val.IsUint64()) {
    return arrow::int64();
  } else if (json_val.IsDouble()) {
    return arrow::float64();
  } else if (json_val.IsString()) {
    return arrow::utf8();
  } else if (json_val.IsArray()) {
    // For arrays, infer from first element
    if (json_val.Empty()) {
      return arrow::list(arrow::null());
    }
    auto elem_type = inferArrowTypeFromJsonValue(json_val[0]);
    return arrow::list(elem_type);
  } else if (json_val.IsObject()) {
    // For objects, create struct type
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto it = json_val.MemberBegin(); it != json_val.MemberEnd(); ++it) {
      std::string field_name = it->name.GetString();
      auto field_type = inferArrowTypeFromJsonValue(it->value);
      fields.push_back(arrow::field(field_name, field_type));
    }
    return arrow::struct_(fields);
  }
  return arrow::null();
}

// Helper function to convert JSON value to Arrow array element
// This is used for building StructArray from JSON
static arrow::Status AppendJsonValueToBuilder(
    const rapidjson::Value& json_val,
    const std::shared_ptr<arrow::DataType>& expected_type,
    arrow::ArrayBuilder* builder) {
  
  if (json_val.IsNull()) {
    return builder->AppendNull();
  }
  
  switch (expected_type->id()) {
    case arrow::Type::BOOL: {
      auto* bool_builder = static_cast<arrow::BooleanBuilder*>(builder);
      return bool_builder->Append(json_val.GetBool());
    }
    case arrow::Type::INT64: {
      auto* int_builder = static_cast<arrow::Int64Builder*>(builder);
      return int_builder->Append(json_val.GetInt64());
    }
    case arrow::Type::UINT64: {
      auto* uint_builder = static_cast<arrow::UInt64Builder*>(builder);
      return uint_builder->Append(json_val.GetUint64());
    }
    case arrow::Type::DOUBLE: {
      auto* double_builder = static_cast<arrow::DoubleBuilder*>(builder);
      return double_builder->Append(json_val.GetDouble());
    }
    case arrow::Type::STRING: {
      auto* string_builder = static_cast<arrow::StringBuilder*>(builder);
      return string_builder->Append(json_val.GetString());
    }
    case arrow::Type::STRUCT: {
      auto* struct_builder = static_cast<arrow::StructBuilder*>(builder);
      auto struct_type = std::static_pointer_cast<arrow::StructType>(expected_type);
      
      for (int i = 0; i < struct_type->num_fields(); ++i) {
        std::string field_name = struct_type->field(i)->name();
        if (json_val.HasMember(field_name.c_str())) {
          const auto& field_val = json_val[field_name.c_str()];
          auto field_type = struct_type->field(i)->type();
          auto field_builder = struct_builder->field_builder(i);
          ARROW_RETURN_NOT_OK(
              AppendJsonValueToBuilder(field_val, field_type, field_builder));
        } else {
          ARROW_RETURN_NOT_OK(struct_builder->field_builder(i)->AppendNull());
        }
      }
      return struct_builder->Append();
    }
    case arrow::Type::LIST: {
      auto* list_builder = static_cast<arrow::ListBuilder*>(builder);
      auto list_type = std::static_pointer_cast<arrow::ListType>(expected_type);
      auto elem_type = list_type->value_type();
      auto elem_builder = list_builder->value_builder();
      
      for (size_t i = 0; i < json_val.Size(); ++i) {
        ARROW_RETURN_NOT_OK(
            AppendJsonValueToBuilder(json_val[i], elem_type, elem_builder));
      }
      return list_builder->Append();
    }
    default:
      return arrow::Status::NotImplemented(
          "Unsupported JSON value type: " + expected_type->ToString());
  }
}

// Helper function to convert graph JSON array (Vertex/Edge/Path) to Arrow StructArray
static std::shared_ptr<arrow::Array> convertGraphJsonToArray(
    const Array& proto_array,
    const std::shared_ptr<arrow::DataType>& arrow_type,
    arrow::MemoryPool* pool) {
  int num_rows = 0;
  const std::string* validity = nullptr;
  
  // Determine which graph type we're dealing with
  std::function<std::string(int)> get_json;
  if (proto_array.has_vertex_array()) {
    const auto& arr = proto_array.vertex_array();
    num_rows = arr.values_size();
    validity = &arr.validity();
    get_json = [&arr](int i) { return arr.values(i); };
  } else if (proto_array.has_edge_array()) {
    const auto& arr = proto_array.edge_array();
    num_rows = arr.values_size();
    validity = &arr.validity();
    get_json = [&arr](int i) { return arr.values(i); };
  } else if (proto_array.has_path_array()) {
    const auto& arr = proto_array.path_array();
    num_rows = arr.values_size();
    validity = &arr.validity();
    get_json = [&arr](int i) { return arr.values(i); };
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION("Expected vertex/edge/path array for STRUCT type");
  }
  
  if (num_rows == 0) {
    return std::make_shared<arrow::StructArray>(arrow_type, 0, std::vector<std::shared_ptr<arrow::Array>>{});
  }
  
  // Parse first JSON to infer structure
  rapidjson::Document first_doc;
  first_doc.Parse(get_json(0).c_str());
  auto struct_type = inferArrowTypeFromJsonValue(first_doc);
  
  // Build arrays for each row
  auto builder_result = arrow::MakeBuilder(struct_type, pool);
  if (!builder_result.ok()) {
    THROW_RUNTIME_ERROR("Failed to create builder: " + builder_result.status().ToString());
  }
  auto& builder = *builder_result;
  
  for (int i = 0; i < num_rows; ++i) {
    if (writer::StringFormatBuffer::validateProtoValue(*validity, i)) {
      rapidjson::Document doc;
      doc.Parse(get_json(i).c_str());
      auto status = AppendJsonValueToBuilder(doc, struct_type, builder.get());
      if (!status.ok()) {
        THROW_RUNTIME_ERROR("Failed to append JSON: " + status.ToString());
      }
    } else {
      auto status = builder->AppendNull();
      if (!status.ok()) {
        THROW_RUNTIME_ERROR("Failed to append null: " + status.ToString());
      }
    }
  }
  
  std::shared_ptr<arrow::Array> array;
  auto status = builder->Finish(&array);
  if (!status.ok()) {
    THROW_RUNTIME_ERROR("Failed to finish array: " + status.ToString());
  }
  return array;
}

// Infer Arrow type from protobuf Array structure
static std::shared_ptr<arrow::DataType> inferArrowTypeFromArray(
    const Array& proto_array) {
  if (proto_array.has_int32_array()) {
    return arrow::int32();
  } else if (proto_array.has_int64_array()) {
    return arrow::int64();
  } else if (proto_array.has_uint32_array()) {
    return arrow::uint32();
  } else if (proto_array.has_uint64_array()) {
    return arrow::uint64();
  } else if (proto_array.has_float_array()) {
    return arrow::float32();
  } else if (proto_array.has_double_array()) {
    return arrow::float64();
  } else if (proto_array.has_bool_array()) {
    return arrow::boolean();
  } else if (proto_array.has_string_array()) {
    return arrow::large_utf8();
  } else if (proto_array.has_date_array()) {
    return arrow::date64();
  } else if (proto_array.has_timestamp_array()) {
    return arrow::timestamp(arrow::TimeUnit::MICRO);
  } else if (proto_array.has_list_array()) {
    // Recursively infer element type
    const auto& list_arr = proto_array.list_array();
    if (list_arr.has_elements()) {
      auto element_type = inferArrowTypeFromArray(list_arr.elements());
      return arrow::list(element_type);
    }
    return arrow::list(arrow::large_utf8());  // Default to string list
  } else if (proto_array.has_struct_array()) {
    // Struct type: infer types from each field
    const auto& struct_arr = proto_array.struct_array();
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (int i = 0; i < struct_arr.fields_size(); ++i) {
      auto field_type = inferArrowTypeFromArray(struct_arr.fields(i));
      fields.push_back(arrow::field("field_" + std::to_string(i), field_type));
    }
    return arrow::struct_(fields);
  } else if (proto_array.has_vertex_array() || 
             proto_array.has_edge_array() || 
             proto_array.has_path_array()) {
    // Vertex/Edge/Path are JSON strings in protobuf, but we parse them
    // and convert to native Arrow structures
    // Parse the first JSON value to infer the structure
    std::string first_json;
    if (proto_array.has_vertex_array()) {
      const auto& arr = proto_array.vertex_array();
      if (arr.values_size() > 0) {
        first_json = arr.values(0);
      }
    } else if (proto_array.has_edge_array()) {
      const auto& arr = proto_array.edge_array();
      if (arr.values_size() > 0) {
        first_json = arr.values(0);
      }
    } else if (proto_array.has_path_array()) {
      const auto& arr = proto_array.path_array();
      if (arr.values_size() > 0) {
        first_json = arr.values(0);
      }
    }
    
    if (!first_json.empty()) {
      rapidjson::Document doc;
      doc.Parse(first_json.c_str());
      if (!doc.HasParseError()) {
        return inferArrowTypeFromJsonValue(doc);
      }
    }
    // Fallback to string if we can't parse
    return arrow::large_utf8();
  } else if (proto_array.has_interval_array()) {
    // Interval type: convert to string for Parquet compatibility
    return arrow::large_utf8();
  } else {
    LOG(WARNING) << "Unknown protobuf array type, defaulting to large_utf8";
    return arrow::large_utf8();
  }
}

// Macro for primitive array conversion (proto-type based dispatch)
#define TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(PROTO_FIELD, BUILDER_TYPE, VALUES_FIELD) \
  { \
    auto& arr = proto_array.PROTO_FIELD(); \
    BUILDER_TYPE builder(pool); \
    for (int i = 0; i < arr.values_size(); ++i) { \
      if (writer::StringFormatBuffer::validateProtoValue(arr.validity(), i)) { \
        auto status = builder.Append(arr.VALUES_FIELD(i)); \
        if (!status.ok()) { \
          THROW_RUNTIME_ERROR("Failed to append value: " + status.ToString()); \
        } \
      } else { \
        auto status = builder.AppendNull(); \
        if (!status.ok()) { \
          THROW_RUNTIME_ERROR("Failed to append null: " + status.ToString()); \
        } \
      } \
    } \
    std::shared_ptr<arrow::Array> result; \
    auto status = builder.Finish(&result); \
    if (!status.ok()) { \
      THROW_RUNTIME_ERROR("Failed to finish array: " + status.ToString()); \
    } \
    return result; \
  }

// Convert protobuf Array to Arrow Array
static std::shared_ptr<arrow::Array> protoArrayToArrowArray(
    const Array& proto_array, const std::shared_ptr<arrow::DataType>& arrow_type,
    int row_count) {
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  
  // First, dispatch based on proto array type
  if (proto_array.has_int32_array()) {
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(int32_array, arrow::Int32Builder, values)
  } else if (proto_array.has_int64_array()) {
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(int64_array, arrow::Int64Builder, values)
  } else if (proto_array.has_uint32_array()) {
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(uint32_array, arrow::UInt32Builder, values)
  } else if (proto_array.has_uint64_array()) {
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(uint64_array, arrow::UInt64Builder, values)
  } else if (proto_array.has_float_array()) {
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(float_array, arrow::FloatBuilder, values)
  } else if (proto_array.has_double_array()) {
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(double_array, arrow::DoubleBuilder, values)
  } else if (proto_array.has_bool_array()) {
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(bool_array, arrow::BooleanBuilder, values)
  } else if (proto_array.has_string_array()) {
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(string_array, arrow::LargeStringBuilder, values)
  } else if (proto_array.has_date_array()) {
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(date_array, arrow::Date64Builder, values)
  } else if (proto_array.has_interval_array()) {
    // Interval: convert to string for Parquet compatibility
    TYPED_PRIMITIVE_ARRAY_TO_ARROW_IMPL(interval_array, arrow::LargeStringBuilder, values)
  } else if (proto_array.has_timestamp_array()) {
    auto& arr = proto_array.timestamp_array();
    arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::MICRO), pool);
    for (int i = 0; i < arr.values_size(); ++i) {
      if (writer::StringFormatBuffer::validateProtoValue(arr.validity(), i)) {
        auto status = builder.Append(arr.values(i));
        if (!status.ok()) {
          THROW_RUNTIME_ERROR("Failed to append timestamp value: " + status.ToString());
        }
      } else {
        auto status = builder.AppendNull();
        if (!status.ok()) {
          THROW_RUNTIME_ERROR("Failed to append null: " + status.ToString());
        }
      }
    }
    std::shared_ptr<arrow::Array> result;
    auto status = builder.Finish(&result);
    if (!status.ok()) {
      THROW_RUNTIME_ERROR("Failed to finish timestamp array: " + status.ToString());
    }
    return result;
  } else if (proto_array.has_list_array()) {
    // Handle List type - build native Arrow ListArray
    const auto& list_arr = proto_array.list_array();
    auto list_type = std::static_pointer_cast<arrow::ListType>(arrow_type);
    auto element_type = list_type->value_type();
    
    // Recursively convert all elements
    auto elements_array = protoArrayToArrowArray(
        list_arr.elements(), element_type, 0);
    
    // Build offsets buffer
    int num_rows = list_arr.offsets_size() - 1;
    
    // Create offsets buffer directly from protobuf
    auto offsets_buffer = arrow::Buffer::Wrap(
        reinterpret_cast<const uint8_t*>(list_arr.offsets().data()),
        list_arr.offsets_size() * sizeof(int32_t));
    
    // For now, don't pass validity buffer to avoid Arrow's limitation
    // "Lists with non-zero length null components are not supported"
    // If all values are valid (no nulls), we can safely omit validity buffer
    std::shared_ptr<arrow::Buffer> validity_buffer = nullptr;
    
    // Create ListArray directly
    auto list_array = std::make_shared<arrow::ListArray>(
        list_type,
        num_rows,
        offsets_buffer,
        elements_array,
        validity_buffer);
    
    return list_array;
  } else if (proto_array.has_struct_array()) {
    // Handle StructArray
    const auto& struct_arr = proto_array.struct_array();
    auto struct_type = std::static_pointer_cast<arrow::StructType>(arrow_type);
    
    // Recursively convert each field
    std::vector<std::shared_ptr<arrow::Array>> field_arrays;
    for (int i = 0; i < struct_arr.fields_size(); ++i) {
      auto field_type = struct_type->field(i)->type();
      auto field_array = protoArrayToArrowArray(
          struct_arr.fields(i), field_type, row_count);
      field_arrays.push_back(field_array);
    }
    
    // Build validity buffer
    auto null_bitmap = struct_arr.validity();
    std::shared_ptr<arrow::Buffer> validity_buffer;
    if (!null_bitmap.empty()) {
      validity_buffer = std::make_shared<arrow::Buffer>(
          reinterpret_cast<const uint8_t*>(null_bitmap.data()),
          null_bitmap.size());
    }
    
    // Create StructArray - num_rows should match the field arrays' length
    int num_rows = field_arrays.empty() ? 0 : field_arrays[0]->length();
    
    auto struct_array = std::make_shared<arrow::StructArray>(
        struct_type,
        num_rows,
        field_arrays,
        validity_buffer);
    
    return struct_array;
  } else if (proto_array.has_vertex_array() || 
             proto_array.has_edge_array() || 
             proto_array.has_path_array()) {
    // Handle Vertex/Edge/Path types (JSON strings -> native Arrow structures)
    return convertGraphJsonToArray(proto_array, arrow_type, pool);
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Unsupported protobuf array type for conversion");
  }
}

neug::Status ArrowParquetExportWriter::writeTable(const QueryResponse* table) {
  if (!table || table->row_count() == 0) {
    return neug::Status::OK();
  }
  
  try {
    // 1. Create Arrow schema from QueryResponse (infer types from protobuf arrays)
    std::vector<std::shared_ptr<arrow::Field>> fields;
    int num_columns = table->arrays_size();
    
    for (int i = 0; i < num_columns; ++i) {
      // Get column name from QueryResponse schema or entry_schema_
      std::string column_name;
      if (i < table->schema().name_size()) {
        column_name = table->schema().name(i);
      } else if (entry_schema_ && i < static_cast<int>(entry_schema_->columnNames.size())) {
        column_name = entry_schema_->columnNames[i];
      } else {
        column_name = "col_" + std::to_string(i);
      }
      
      // Infer Arrow type from protobuf array structure
      const auto& proto_array = table->arrays(i);
      auto arrow_type = inferArrowTypeFromArray(proto_array);
      
      fields.push_back(arrow::field(column_name, arrow_type));
    }
    
    auto arrow_schema = arrow::schema(fields);
    
    // 2. Open output file
    auto result = fileSystem_->OpenOutputStream(schema_.paths[0]);
    if (!result.ok()) {
      return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to open output file: " + result.status().ToString());
    }
    auto outfile = result.ValueOrDie();
    
    // 3. Create Parquet writer with options
    auto properties = buildWriterProperties(schema_.options);
    
    auto writer_result = parquet::arrow::FileWriter::Open(
        *arrow_schema, arrow::default_memory_pool(), outfile, properties);
    if (!writer_result.ok()) {
      return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to create Parquet writer: " + writer_result.status().ToString());
    }
    auto writer = std::move(writer_result.ValueOrDie());
    
    // 4. Convert protobuf Arrays to Arrow Arrays
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (int i = 0; i < num_columns; ++i) {
      const auto& proto_array = table->arrays(i);
      auto arrow_type = arrow_schema->field(i)->type();
      auto arrow_array = protoArrayToArrowArray(proto_array, arrow_type, table->row_count());
      arrays.push_back(arrow_array);
    }
    
    // 5. Create Arrow Table and write
    auto arrow_table = arrow::Table::Make(arrow_schema, arrays);
    
    auto write_status = writer->WriteTable(*arrow_table, arrow_table->num_rows());
    if (!write_status.ok()) {
      return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to write Parquet table: " + write_status.ToString());
    }
    
    // 6. Close writer to flush and write footer
    auto close_status = writer->Close();
    if (!close_status.ok()) {
      return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to close Parquet writer: " + close_status.ToString());
    }
    
    // 7. Close output stream
    auto outfile_close_status = outfile->Close();
    if (!outfile_close_status.ok()) {
      return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to close output stream: " + outfile_close_status.ToString());
    }
    
    return neug::Status::OK();
  } catch (const std::exception& e) {
    return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                        std::string("Failed to write Parquet table: ") + e.what());
  }
}

}  // namespace writer

namespace function {

// Export function execution
static execution::Context parquetExecFunc(
    neug::execution::Context& ctx, reader::FileSchema& schema,
    const std::shared_ptr<reader::EntrySchema>& entry_schema,
    const neug::StorageReadInterface& graph) {
  if (schema.paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Schema paths is empty");
  }
  
  const auto& vfs = neug::main::MetadataRegistry::getVFS();
  const auto& fs = vfs->Provide(schema);
  
  auto writer = std::make_shared<neug::writer::ArrowParquetExportWriter>(
      schema, fs->toArrowFileSystem(), entry_schema);
  
  auto status = writer->write(ctx, graph);
  if (!status.ok()) {
    THROW_IO_EXCEPTION("Parquet export failed: " + status.ToString());
  }
  LOG(INFO) << "[Parquet Export] Export completed successfully";
  ctx.clear();
  return ctx;
}

// Bind function
static std::unique_ptr<ExportFuncBindData> bindFunc(
    ExportFuncBindInput& bindInput) {
  return std::make_unique<ExportFuncBindData>(
      bindInput.columnNames, bindInput.filePath, bindInput.parsingOptions);
}

function_set ExportParquetFunction::getFunctionSet() {
  function_set functionSet;
  auto exportFunc = std::make_unique<ExportFunction>(name);
  exportFunc->bind = bindFunc;
  exportFunc->execFunc = parquetExecFunc;
  functionSet.push_back(std::move(exportFunc));
  return functionSet;
}

}  // namespace function
}  // namespace neug
