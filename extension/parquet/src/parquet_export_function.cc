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
    // Vertex/Edge/Path are exported as JSON strings (not StructArray)
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
    // Vertex/Edge/Path are stored as JSON strings in protobuf.
    // We export them directly as JSON strings (not StructArray).
    //
    // NOTE: Why not convert to StructArray?
    // Parquet StructArray has columnar advantages (compression, predicate pushdown, etc.),
    // but requires ALL rows to have the SAME schema. Mixed-type vertices/edges have different
    // properties, causing schema conflicts and sparse data.
    
    // Extract JSON strings from the appropriate field
    arrow::LargeStringBuilder builder(pool);
    
    auto append_json_strings = [&](const auto& arr) {
      for (int i = 0; i < arr.values_size(); ++i) {
        if (writer::StringFormatBuffer::validateProtoValue(arr.validity(), i)) {
          auto status = builder.Append(arr.values(i));
          if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to append JSON string: " + status.ToString());
          }
        } else {
          auto status = builder.AppendNull();
          if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to append null: " + status.ToString());
          }
        }
      }
    };
    
    if (proto_array.has_vertex_array()) {
      append_json_strings(proto_array.vertex_array());
    } else if (proto_array.has_edge_array()) {
      append_json_strings(proto_array.edge_array());
    } else {
      append_json_strings(proto_array.path_array());
    }
    
    std::shared_ptr<arrow::Array> result;
    auto status = builder.Finish(&result);
    if (!status.ok()) {
      THROW_RUNTIME_ERROR("Failed to finish JSON string array: " + status.ToString());
    }
    return result;
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
