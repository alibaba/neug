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
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <glog/logging.h>
#include <parquet/properties.h>
#include <algorithm>
#include <cstring>

#include "neug/common/columns/list_columns.h"
#include "neug/common/columns/struct_columns.h"
#include "neug/common/columns/value_columns.h"
#include "neug/common/export/export_result.h"
#include "neug/common/types/array_columns.h"
#include "neug/common/types/data_chunk.h"
#include "neug/common/types/property_types.h"
#include "neug/common/types/value.h"
#include "neug/compiler/main/metadata_registry.h"
#include "neug/utils/exception/exception.h"
#include "parquet/arrow_fs_resolver.h"
#include "parquet_options.h"

namespace neug {
namespace writer {

static std::shared_ptr<::parquet::WriterProperties> buildWriterProperties(
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
        ". Supported: none, snappy, gzip (zlib), zstd");
  }

  // Parse row group size
  int64_t row_group_size = export_options.row_group_size.get(options);

  if (row_group_size < 1024) {
    LOG(WARNING)
        << "Very small row_group_size (" << row_group_size
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
  ::parquet::WriterProperties::Builder builder;
  builder.compression(compression);
  builder.max_row_group_length(row_group_size);

  if (dictionary_encoding) {
    builder.enable_dictionary();
  } else {
    builder.disable_dictionary();
  }

  return builder.build();
}

static std::shared_ptr<arrow::DataType> inferArrowTypeFromDataType(
    const DataType& type) {
  switch (type.id()) {
  case DataTypeId::kBoolean:
    return arrow::boolean();
  case DataTypeId::kInt32:
    return arrow::int32();
  case DataTypeId::kInt64:
    return arrow::int64();
  case DataTypeId::kUInt32:
    return arrow::uint32();
  case DataTypeId::kUInt64:
    return arrow::uint64();
  case DataTypeId::kFloat:
    return arrow::float32();
  case DataTypeId::kDouble:
    return arrow::float64();
  case DataTypeId::kVarchar:
    return arrow::large_utf8();
  case DataTypeId::kDate:
    return arrow::date64();
  case DataTypeId::kTimestampMs:
    return arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
  case DataTypeId::kInterval:
    return arrow::large_utf8();
  case DataTypeId::kList:
    return arrow::list(
        inferArrowTypeFromDataType(ListType::GetChildType(type)));
  case DataTypeId::kArray:
    return arrow::list(
        inferArrowTypeFromDataType(ArrayType::GetChildType(type)));
  case DataTypeId::kStruct: {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    const auto& child_types = StructType::GetChildTypes(type);
    const auto& field_names = StructType::GetFieldNames(type);
    for (size_t i = 0; i < child_types.size(); ++i) {
      const auto& name = i < field_names.size()
                             ? field_names[i]
                             : ("field_" + std::to_string(i));
      fields.push_back(
          arrow::field(name, inferArrowTypeFromDataType(child_types[i])));
    }
    return arrow::struct_(fields);
  }
  default:
    LOG(WARNING) << "Unknown DataType for Parquet export, defaulting to "
                    "large_utf8: "
                 << type.ToString();
    return arrow::large_utf8();
  }
}

template <typename T, typename Builder>
static std::shared_ptr<arrow::Array> buildPrimitiveArray(
    const std::shared_ptr<IContextColumn>& col) {
  auto typed = std::dynamic_pointer_cast<ValueColumn<T>>(col);
  if (!typed) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Expected ValueColumn for type " +
                                     col->column_info());
  }
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  Builder builder(pool);
  for (size_t i = 0; i < typed->size(); ++i) {
    if (col->is_optional() && !col->has_value(i)) {
      auto status = builder.AppendNull();
      if (!status.ok()) {
        THROW_RUNTIME_ERROR("Failed to append null: " + status.ToString());
      }
    } else {
      auto status = builder.Append(typed->get_value(i));
      if (!status.ok()) {
        THROW_RUNTIME_ERROR("Failed to append value: " + status.ToString());
      }
    }
  }
  std::shared_ptr<arrow::Array> result;
  auto status = builder.Finish(&result);
  if (!status.ok()) {
    THROW_RUNTIME_ERROR("Failed to finish array: " + status.ToString());
  }
  return result;
}

static std::shared_ptr<arrow::Buffer> boolVectorToArrowBitmap(
    const vector_t<bool>& validity, size_t length) {
  if (validity.empty()) {
    return nullptr;
  }
  auto buffer_result =
      arrow::AllocateBuffer(static_cast<int64_t>((length + 7) / 8));
  if (!buffer_result.ok()) {
    THROW_RUNTIME_ERROR("Failed to allocate validity buffer: " +
                        buffer_result.status().ToString());
  }
  auto buffer = std::move(buffer_result.ValueOrDie());
  memset(buffer->mutable_data(), 0, static_cast<size_t>(buffer->size()));
  for (size_t i = 0; i < length; ++i) {
    if (validity[i]) {
      buffer->mutable_data()[i / 8] |= static_cast<uint8_t>(1U << (i % 8));
    }
  }
  return buffer;
}

static std::shared_ptr<arrow::Array> contextColumnToArrowArray(
    const std::shared_ptr<IContextColumn>& col,
    const std::shared_ptr<arrow::DataType>& arrow_type) {
  if (!col || col->size() == 0) {
    return arrow::MakeArrayOfNull(arrow_type, 0).ValueOrDie();
  }

  switch (col->elem_type().id()) {
  case DataTypeId::kBoolean:
    return buildPrimitiveArray<bool, arrow::BooleanBuilder>(col);
  case DataTypeId::kInt32:
    return buildPrimitiveArray<int32_t, arrow::Int32Builder>(col);
  case DataTypeId::kInt64:
    return buildPrimitiveArray<int64_t, arrow::Int64Builder>(col);
  case DataTypeId::kUInt32:
    return buildPrimitiveArray<uint32_t, arrow::UInt32Builder>(col);
  case DataTypeId::kUInt64:
    return buildPrimitiveArray<uint64_t, arrow::UInt64Builder>(col);
  case DataTypeId::kFloat:
    return buildPrimitiveArray<float, arrow::FloatBuilder>(col);
  case DataTypeId::kDouble:
    return buildPrimitiveArray<double, arrow::DoubleBuilder>(col);
  case DataTypeId::kVarchar:
    return buildPrimitiveArray<std::string, arrow::LargeStringBuilder>(col);
  case DataTypeId::kDate: {
    auto typed = std::dynamic_pointer_cast<ValueColumn<Date>>(col);
    if (!typed) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Expected ValueColumn<Date>");
    }
    arrow::Date64Builder builder(arrow::default_memory_pool());
    for (size_t i = 0; i < typed->size(); ++i) {
      if (col->is_optional() && !col->has_value(i)) {
        auto status = builder.AppendNull();
        if (!status.ok()) {
          THROW_RUNTIME_ERROR("Failed to append null: " + status.ToString());
        }
      } else {
        auto status = builder.Append(typed->get_value(i).to_timestamp());
        if (!status.ok()) {
          THROW_RUNTIME_ERROR("Failed to append date: " + status.ToString());
        }
      }
    }
    std::shared_ptr<arrow::Array> result;
    auto finish_status = builder.Finish(&result);
    if (!finish_status.ok()) {
      THROW_RUNTIME_ERROR("Failed to finish date array: " +
                          finish_status.ToString());
    }
    return result;
  }
  case DataTypeId::kTimestampMs: {
    auto typed = std::dynamic_pointer_cast<ValueColumn<DateTime>>(col);
    if (!typed) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Expected ValueColumn<DateTime>");
    }
    arrow::TimestampBuilder builder(
        arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
        arrow::default_memory_pool());
    for (size_t i = 0; i < typed->size(); ++i) {
      if (col->is_optional() && !col->has_value(i)) {
        auto status = builder.AppendNull();
        if (!status.ok()) {
          THROW_RUNTIME_ERROR("Failed to append null: " + status.ToString());
        }
      } else {
        auto status = builder.Append(typed->get_value(i).milli_second * 1000);
        if (!status.ok()) {
          THROW_RUNTIME_ERROR("Failed to append timestamp: " +
                              status.ToString());
        }
      }
    }
    std::shared_ptr<arrow::Array> result;
    auto finish_status = builder.Finish(&result);
    if (!finish_status.ok()) {
      THROW_RUNTIME_ERROR("Failed to finish timestamp array: " +
                          finish_status.ToString());
    }
    return result;
  }
  case DataTypeId::kInterval: {
    auto typed = std::dynamic_pointer_cast<ValueColumn<Interval>>(col);
    if (!typed) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Expected ValueColumn<Interval>");
    }
    arrow::LargeStringBuilder builder(arrow::default_memory_pool());
    for (size_t i = 0; i < typed->size(); ++i) {
      if (col->is_optional() && !col->has_value(i)) {
        auto status = builder.AppendNull();
        if (!status.ok()) {
          THROW_RUNTIME_ERROR("Failed to append null: " + status.ToString());
        }
      } else {
        auto status = builder.Append(typed->get_value(i).to_string());
        if (!status.ok()) {
          THROW_RUNTIME_ERROR("Failed to append interval: " +
                              status.ToString());
        }
      }
    }
    std::shared_ptr<arrow::Array> result;
    auto finish_status = builder.Finish(&result);
    if (!finish_status.ok()) {
      THROW_RUNTIME_ERROR("Failed to finish interval array: " +
                          finish_status.ToString());
    }
    return result;
  }
  case DataTypeId::kList: {
    auto list_col = std::dynamic_pointer_cast<ListColumn>(col);
    if (!list_col) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Expected ListColumn");
    }
    auto reordered_list =
        std::dynamic_pointer_cast<ListColumn>(list_col->reorder());
    auto list_type = std::static_pointer_cast<arrow::ListType>(arrow_type);
    auto elements_array = contextColumnToArrowArray(
        reordered_list->data_column(), list_type->value_type());

    arrow::Int32Builder offsets_builder(arrow::default_memory_pool());
    const auto& items = reordered_list->items();
    for (const auto& item : items) {
      auto status = offsets_builder.Append(static_cast<int32_t>(item.offset));
      if (!status.ok()) {
        THROW_RUNTIME_ERROR("Failed to append list offset: " +
                            status.ToString());
      }
    }
    if (!items.empty()) {
      const auto& last = items.back();
      auto status = offsets_builder.Append(
          static_cast<int32_t>(last.offset + last.length));
      if (!status.ok()) {
        THROW_RUNTIME_ERROR("Failed to append list offset: " +
                            status.ToString());
      }
    } else {
      auto status = offsets_builder.Append(0);
      if (!status.ok()) {
        THROW_RUNTIME_ERROR("Failed to append list offset: " +
                            status.ToString());
      }
    }
    std::shared_ptr<arrow::Array> offsets_array;
    auto finish_status = offsets_builder.Finish(&offsets_array);
    if (!finish_status.ok()) {
      THROW_RUNTIME_ERROR("Failed to finish list offsets: " +
                          finish_status.ToString());
    }
    return std::make_shared<arrow::ListArray>(
        list_type, static_cast<int64_t>(items.size()),
        offsets_array->data()->buffers[1], elements_array, nullptr);
  }
  case DataTypeId::kArray: {
    auto array_col = std::dynamic_pointer_cast<ContextArrayColumn>(col);
    if (!array_col) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Expected ContextArrayColumn");
    }
    auto list_type = std::static_pointer_cast<arrow::ListType>(arrow_type);
    auto elements_array = contextColumnToArrowArray(array_col->data_column(),
                                                    list_type->value_type());
    const auto array_size = array_col->array_size();
    const auto num_rows = array_col->size();

    arrow::Int32Builder offsets_builder(arrow::default_memory_pool());
    for (size_t i = 0; i < num_rows; ++i) {
      auto status =
          offsets_builder.Append(static_cast<int32_t>(i * array_size));
      if (!status.ok()) {
        THROW_RUNTIME_ERROR("Failed to append array offset: " +
                            status.ToString());
      }
    }
    auto last_status =
        offsets_builder.Append(static_cast<int32_t>(num_rows * array_size));
    if (!last_status.ok()) {
      THROW_RUNTIME_ERROR("Failed to append array offset: " +
                          last_status.ToString());
    }
    std::shared_ptr<arrow::Array> offsets_array;
    auto finish_status = offsets_builder.Finish(&offsets_array);
    if (!finish_status.ok()) {
      THROW_RUNTIME_ERROR("Failed to finish array offsets: " +
                          finish_status.ToString());
    }
    return std::make_shared<arrow::ListArray>(
        list_type, static_cast<int64_t>(num_rows),
        offsets_array->data()->buffers[1], elements_array, nullptr);
  }
  case DataTypeId::kStruct: {
    auto struct_col = std::dynamic_pointer_cast<StructColumn>(col);
    if (!struct_col) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Expected StructColumn");
    }
    auto struct_type = std::static_pointer_cast<arrow::StructType>(arrow_type);
    std::vector<std::shared_ptr<arrow::Array>> field_arrays;
    const auto& children = struct_col->children();
    for (size_t i = 0; i < children.size(); ++i) {
      field_arrays.push_back(contextColumnToArrowArray(
          children[i], struct_type->field(static_cast<int>(i))->type()));
    }
    std::shared_ptr<arrow::Buffer> validity_buffer = nullptr;
    if (struct_col->is_optional()) {
      validity_buffer =
          boolVectorToArrowBitmap(struct_col->validity_bitmap(), col->size());
    }
    return std::make_shared<arrow::StructArray>(
        struct_type, static_cast<int64_t>(col->size()), field_arrays,
        validity_buffer);
  }
  default: {
    arrow::LargeStringBuilder builder(arrow::default_memory_pool());
    for (size_t i = 0; i < col->size(); ++i) {
      if (col->is_optional() && !col->has_value(i)) {
        auto status = builder.AppendNull();
        if (!status.ok()) {
          THROW_RUNTIME_ERROR("Failed to append null: " + status.ToString());
        }
      } else {
        auto status = builder.Append(col->get_elem(i).to_string());
        if (!status.ok()) {
          THROW_RUNTIME_ERROR("Failed to append string: " + status.ToString());
        }
      }
    }
    std::shared_ptr<arrow::Array> result;
    auto finish_status = builder.Finish(&result);
    if (!finish_status.ok()) {
      THROW_RUNTIME_ERROR("Failed to finish string array: " +
                          finish_status.ToString());
    }
    return result;
  }
  }
}

static neug::Status writeArrowTable(
    const std::shared_ptr<arrow::Schema>& arrow_schema,
    const std::vector<std::shared_ptr<arrow::Array>>& arrays,
    const reader::FileSchema& schema,
    const std::shared_ptr<arrow::fs::FileSystem>& file_system) {
  auto result = file_system->OpenOutputStream(schema.paths[0]);
  if (!result.ok()) {
    return neug::Status(
        neug::StatusCode::ERR_IO_ERROR,
        "Failed to open output file: " + result.status().ToString());
  }
  auto outfile = result.ValueOrDie();

  auto properties = buildWriterProperties(schema.options);
  auto writer_result = ::parquet::arrow::FileWriter::Open(
      *arrow_schema, arrow::default_memory_pool(), outfile, properties);
  if (!writer_result.ok()) {
    return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                        "Failed to create Parquet writer: " +
                            writer_result.status().ToString());
  }
  auto writer = std::move(writer_result.ValueOrDie());

  auto arrow_table = arrow::Table::Make(arrow_schema, arrays);
  auto write_status = writer->WriteTable(*arrow_table, arrow_table->num_rows());
  if (!write_status.ok()) {
    return neug::Status(
        neug::StatusCode::ERR_IO_ERROR,
        "Failed to write Parquet table: " + write_status.ToString());
  }

  auto close_status = writer->Close();
  if (!close_status.ok()) {
    return neug::Status(
        neug::StatusCode::ERR_IO_ERROR,
        "Failed to close Parquet writer: " + close_status.ToString());
  }

  auto outfile_close_status = outfile->Close();
  if (!outfile_close_status.ok()) {
    return neug::Status(
        neug::StatusCode::ERR_IO_ERROR,
        "Failed to close output stream: " + outfile_close_status.ToString());
  }
  return neug::Status::OK();
}

neug::Status ArrowParquetExportWriter::write(
    const DataChunk& chunk, const std::vector<DataType>& /*source_types*/) {
  if (chunk.row_num() == 0) {
    return neug::Status::OK();
  }
  if (!entry_schema_) {
    return neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "entry_schema is null");
  }

  try {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(chunk.col_num());
    for (size_t i = 0; i < chunk.col_num(); ++i) {
      if (chunk.columns[i] == nullptr) {
        continue;
      }
      std::string column_name = i < entry_schema_->columnNames.size()
                                    ? entry_schema_->columnNames[i]
                                    : ("col_" + std::to_string(i));
      fields.push_back(arrow::field(
          column_name,
          inferArrowTypeFromDataType(chunk.columns[i]->elem_type())));
    }

    auto arrow_schema = arrow::schema(fields);
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(fields.size());
    for (size_t i = 0; i < chunk.col_num(); ++i) {
      if (chunk.columns[i] == nullptr) {
        continue;
      }
      arrays.push_back(contextColumnToArrowArray(
          chunk.columns[i],
          arrow_schema->field(static_cast<int>(arrays.size()))->type()));
    }

    return writeArrowTable(arrow_schema, arrays, schema_, fileSystem_);
  } catch (const std::exception& e) {
    return neug::Status(
        neug::StatusCode::ERR_IO_ERROR,
        std::string("Failed to write Parquet table: ") + e.what());
  }
}

}  // namespace writer

namespace function {

static execution::Context parquetExecFunc(
    neug::execution::Context& ctx, reader::FileSchema& schema,
    const std::shared_ptr<reader::EntrySchema>& entry_schema,
    const neug::StorageReadInterface& graph) {
  if (schema.paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Schema paths is empty");
  }

  const auto& vfs = neug::main::MetadataRegistry::getVFS();
  const auto& fs = vfs->Provide(schema);

  auto arrowFs = neug::parquet::resolveArrowFileSystem(*fs);
  auto writer = std::make_shared<neug::writer::ArrowParquetExportWriter>(
      schema, std::move(arrowFs), entry_schema);

  auto export_result = neug::materialize_result_for_export(ctx, graph);
  auto status = writer->write(export_result.chunk, export_result.source_types);
  if (!status.ok()) {
    THROW_IO_EXCEPTION("Parquet export failed: " + status.ToString());
  }
  LOG(INFO) << "[Parquet Export] Export completed successfully";
  ctx.clear();
  return ctx;
}

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
