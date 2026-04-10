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

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/table.h>
#include <arrow/type.h>

#include "neug/compiler/main/metadata_registry.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace writer {

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
  } else {
    LOG(WARNING) << "Unknown protobuf array type, defaulting to large_utf8";
    return arrow::large_utf8();
  }
}

// Convert protobuf Array to Arrow Array
static std::shared_ptr<arrow::Array> protoArrayToArrowArray(
    const Array& proto_array, const std::shared_ptr<arrow::DataType>& arrow_type,
    int row_count) {
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  
  // Handle different array types based on Arrow type
  if (arrow_type->Equals(arrow::int32())) {
    arrow::Int32Builder builder(pool);
    if (proto_array.has_int32_array()) {
      const auto& arr = proto_array.int32_array();
      for (int i = 0; i < arr.values_size(); ++i) {
        if (arr.validity().empty() || 
            (static_cast<uint8_t>(arr.validity()[i >> 3]) >> (i & 7)) & 1) {
          auto status = builder.Append(arr.values(i));
          if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to append int32 value: " + status.ToString());
          }
        } else {
          auto status = builder.AppendNull();
          if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to append null: " + status.ToString());
          }
        }
      }
    }
    std::shared_ptr<arrow::Array> result;
    auto status = builder.Finish(&result);
    if (!status.ok()) {
      THROW_RUNTIME_ERROR("Failed to finish int32 array: " + status.ToString());
    }
    return result;
  } else if (arrow_type->Equals(arrow::int64())) {
    arrow::Int64Builder builder(pool);
    if (proto_array.has_int64_array()) {
      const auto& arr = proto_array.int64_array();
      for (int i = 0; i < arr.values_size(); ++i) {
        if (arr.validity().empty() || 
            (static_cast<uint8_t>(arr.validity()[i >> 3]) >> (i & 7)) & 1) {
          auto status = builder.Append(arr.values(i));
          if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to append int64 value: " + status.ToString());
          }
        } else {
          auto status = builder.AppendNull();
          if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to append null: " + status.ToString());
          }
        }
      }
    }
    std::shared_ptr<arrow::Array> result;
    auto status = builder.Finish(&result);
    if (!status.ok()) {
      THROW_RUNTIME_ERROR("Failed to finish int64 array: " + status.ToString());
    }
    return result;
  } else if (arrow_type->Equals(arrow::float32())) {
    arrow::FloatBuilder builder(pool);
    if (proto_array.has_float_array()) {
      const auto& arr = proto_array.float_array();
      for (int i = 0; i < arr.values_size(); ++i) {
        if (arr.validity().empty() || 
            (static_cast<uint8_t>(arr.validity()[i >> 3]) >> (i & 7)) & 1) {
          auto status = builder.Append(arr.values(i));
          if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to append float value: " + status.ToString());
          }
        } else {
          auto status = builder.AppendNull();
          if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to append null: " + status.ToString());
          }
        }
      }
    }
    std::shared_ptr<arrow::Array> result;
    auto status = builder.Finish(&result);
    if (!status.ok()) {
      THROW_RUNTIME_ERROR("Failed to finish float array: " + status.ToString());
    }
    return result;
  } else if (arrow_type->Equals(arrow::float64())) {
    arrow::DoubleBuilder builder(pool);
    if (proto_array.has_double_array()) {
      const auto& arr = proto_array.double_array();
      for (int i = 0; i < arr.values_size(); ++i) {
        if (arr.validity().empty() || 
            (static_cast<uint8_t>(arr.validity()[i >> 3]) >> (i & 7)) & 1) {
          auto status = builder.Append(arr.values(i));
          if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to append double value: " + status.ToString());
          }
        } else {
          auto status = builder.AppendNull();
          if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to append null: " + status.ToString());
          }
        }
      }
    }
    std::shared_ptr<arrow::Array> result;
    auto status = builder.Finish(&result);
    if (!status.ok()) {
      THROW_RUNTIME_ERROR("Failed to finish double array: " + status.ToString());
    }
    return result;
  } else if (arrow_type->Equals(arrow::boolean())) {
    arrow::BooleanBuilder builder(pool);
    if (proto_array.has_bool_array()) {
      const auto& arr = proto_array.bool_array();
      for (int i = 0; i < arr.values_size(); ++i) {
        if (arr.validity().empty() || 
            (static_cast<uint8_t>(arr.validity()[i >> 3]) >> (i & 7)) & 1) {
          auto status = builder.Append(arr.values(i));
          if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to append bool value: " + status.ToString());
          }
        } else {
          auto status = builder.AppendNull();
          if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to append null: " + status.ToString());
          }
        }
      }
    }
    std::shared_ptr<arrow::Array> result;
    auto status = builder.Finish(&result);
    if (!status.ok()) {
      THROW_RUNTIME_ERROR("Failed to finish bool array: " + status.ToString());
    }
    return result;
  } else if (arrow_type->Equals(arrow::utf8()) || 
             arrow_type->Equals(arrow::large_utf8())) {
    arrow::LargeStringBuilder builder(pool);
    if (proto_array.has_string_array()) {
      const auto& arr = proto_array.string_array();
      for (int i = 0; i < arr.values_size(); ++i) {
        if (arr.validity().empty() || 
            (static_cast<uint8_t>(arr.validity()[i >> 3]) >> (i & 7)) & 1) {
          auto status = builder.Append(arr.values(i));
          if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to append string value: " + status.ToString());
          }
        } else {
          auto status = builder.AppendNull();
          if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to append null: " + status.ToString());
          }
        }
      }
    }
    std::shared_ptr<arrow::Array> result;
    auto status = builder.Finish(&result);
    if (!status.ok()) {
      THROW_RUNTIME_ERROR("Failed to finish string array: " + status.ToString());
    }
    return result;
  }
  
  THROW_INVALID_ARGUMENT_EXCEPTION(
      "Unsupported Arrow type for conversion: " + arrow_type->ToString());
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
    
    // 3. Create Parquet writer
    parquet::WriterProperties::Builder builder;
    builder.compression(arrow::Compression::SNAPPY);
    builder.enable_dictionary();
    auto properties = builder.build();
    
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

// Bind function (reuse pattern from JSON export)
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
