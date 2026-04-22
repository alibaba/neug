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

#include "json_export_function.h"

#include <arrow/result.h>
#include <arrow/status.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/writer.h>

#include <string>

#include "neug/compiler/function/read_function.h"
#include "neug/compiler/main/metadata_registry.h"
#include "neug/generated/proto/response/response.pb.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"
#include "neug/utils/writer/writer.h"

namespace neug {
namespace writer {

#define TYPED_PRIMITIVE_ARRAY_TO_JSON_VALUE(CASE_ENUM, GETTER_METHOD, TYPE) \
  case neug::Array::TypedArrayCase::CASE_ENUM: {                            \
    auto& typed_array = arr.GETTER_METHOD();                                \
    if (!StringFormatBuffer::validateProtoValue(typed_array.validity(),     \
                                                rowIdx)) {                  \
      RETURN_STATUS_ERROR(                                                  \
          neug::StatusCode::ERR_INVALID_ARGUMENT,                           \
          "Value is invalid, rowIdx=" + std::to_string(rowIdx));            \
    }                                                                       \
    rapidjson::Value v(static_cast<TYPE>(typed_array.values(rowIdx)));      \
    return v;                                                               \
  }

static neug::result<rapidjson::Value> parseJsonStringToValue(
    const std::string& json_str, int rowIdx, rapidjson::Document& parse_doc,
    const char* type_name) {
  if (json_str.empty()) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "Empty JSON string for " + std::string(type_name) +
                            " at row " + std::to_string(rowIdx));
  }
  rapidjson::Document temp_doc(&parse_doc.GetAllocator());
  temp_doc.Parse(json_str.data(), json_str.size());
  if (temp_doc.HasParseError()) {
    RETURN_STATUS_ERROR(
        neug::StatusCode::ERR_INVALID_ARGUMENT,
        "Invalid JSON for " + std::string(type_name) + " at row " +
            std::to_string(rowIdx) + ": " +
            rapidjson::GetParseError_En(temp_doc.GetParseError()) +
            " at offset " + std::to_string(temp_doc.GetErrorOffset()));
  }
  rapidjson::Value v;
  // use swap to avoid memory allocation
  v.Swap(temp_doc);
  return v;
}

// return `rapidjson::Value` directly will not lead to any memory allocation,
// it's a move operation
static neug::result<rapidjson::Value> formatValueToJson(
    const neug::Array& arr, int rowIdx, rapidjson::Document& doc) {
  auto& allocator = doc.GetAllocator();
  switch (arr.typed_array_case()) {
    TYPED_PRIMITIVE_ARRAY_TO_JSON_VALUE(kBoolArray, bool_array, bool)
    TYPED_PRIMITIVE_ARRAY_TO_JSON_VALUE(kInt32Array, int32_array, int32_t)
    TYPED_PRIMITIVE_ARRAY_TO_JSON_VALUE(kInt64Array, int64_array, int64_t)
    TYPED_PRIMITIVE_ARRAY_TO_JSON_VALUE(kUint32Array, uint32_array, uint32_t)
    TYPED_PRIMITIVE_ARRAY_TO_JSON_VALUE(kUint64Array, uint64_array, uint64_t)
    TYPED_PRIMITIVE_ARRAY_TO_JSON_VALUE(kFloatArray, float_array, float)
    TYPED_PRIMITIVE_ARRAY_TO_JSON_VALUE(kDoubleArray, double_array, double)
  case neug::Array::TypedArrayCase::kStringArray: {
    auto& string_array = arr.string_array();
    if (!StringFormatBuffer::validateProtoValue(string_array.validity(),
                                                rowIdx)) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                          "Value is invalid, rowIdx=" + std::to_string(rowIdx));
    }
    const auto& str = string_array.values(rowIdx);
    rapidjson::Value v;
    v.SetString(str.c_str(), static_cast<rapidjson::SizeType>(str.size()),
                allocator);
    return v;
  }
  case neug::Array::TypedArrayCase::kDateArray: {
    auto& date32_arr = arr.date_array();
    if (!StringFormatBuffer::validateProtoValue(date32_arr.validity(),
                                                rowIdx)) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                          "Value is invalid, rowIdx=" + std::to_string(rowIdx));
    }
    Date date_value;
    date_value.from_timestamp(date32_arr.values(rowIdx));
    const auto& s = date_value.to_string();
    rapidjson::Value v;
    v.SetString(s.c_str(), static_cast<rapidjson::SizeType>(s.size()),
                allocator);
    return v;
  }
  case neug::Array::TypedArrayCase::kTimestampArray: {
    auto& timestamp_array = arr.timestamp_array();
    if (!StringFormatBuffer::validateProtoValue(timestamp_array.validity(),
                                                rowIdx)) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                          "Value is invalid, rowIdx=" + std::to_string(rowIdx));
    }
    DateTime dt_value(timestamp_array.values(rowIdx));
    const auto& s = dt_value.to_string();
    rapidjson::Value v;
    v.SetString(s.c_str(), static_cast<rapidjson::SizeType>(s.size()),
                allocator);
    return v;
  }
  case neug::Array::TypedArrayCase::kIntervalArray: {
    auto& interval_array = arr.interval_array();
    if (!StringFormatBuffer::validateProtoValue(interval_array.validity(),
                                                rowIdx)) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                          "Value is invalid, rowIdx=" + std::to_string(rowIdx));
    }
    const auto& s = interval_array.values(rowIdx);
    rapidjson::Value v;
    v.SetString(s.c_str(), static_cast<rapidjson::SizeType>(s.size()),
                allocator);
    return v;
  }
  case neug::Array::TypedArrayCase::kListArray: {
    auto& list_array = arr.list_array();
    if (!StringFormatBuffer::validateProtoValue(list_array.validity(),
                                                rowIdx)) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                          "Value is invalid, rowIdx=" + std::to_string(rowIdx));
    }
    rapidjson::Value arr_val(rapidjson::kArrayType);
    uint32_t list_size =
        list_array.offsets(rowIdx + 1) - list_array.offsets(rowIdx);
    size_t offset = list_array.offsets(rowIdx);
    for (uint32_t i = 0; i < list_size; ++i) {
      rapidjson::Value elem;
      GS_ASSIGN(elem, formatValueToJson(list_array.elements(),
                                        static_cast<int>(offset + i), doc));
      arr_val.PushBack(std::move(elem), allocator);
    }
    return arr_val;
  }
  case neug::Array::TypedArrayCase::kStructArray: {
    auto& struct_arr = arr.struct_array();
    if (!StringFormatBuffer::validateProtoValue(struct_arr.validity(),
                                                rowIdx)) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                          "Value is invalid, rowIdx=" + std::to_string(rowIdx));
    }
    rapidjson::Value arr_val(rapidjson::kArrayType);
    for (int i = 0; i < struct_arr.fields_size(); ++i) {
      const auto& field = struct_arr.fields(i);
      rapidjson::Value elem;
      GS_ASSIGN(elem, formatValueToJson(field, rowIdx, doc));
      arr_val.PushBack(std::move(elem), allocator);
    }
    return arr_val;
  }
  case neug::Array::TypedArrayCase::kVertexArray: {
    auto& vertex_array = arr.vertex_array();
    if (!StringFormatBuffer::validateProtoValue(vertex_array.validity(),
                                                rowIdx)) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                          "Value is invalid, rowIdx=" + std::to_string(rowIdx));
    }
    return parseJsonStringToValue(vertex_array.values(rowIdx), rowIdx, doc,
                                  "vertex");
  }
  case neug::Array::TypedArrayCase::kEdgeArray: {
    auto& edge_array = arr.edge_array();
    if (!StringFormatBuffer::validateProtoValue(edge_array.validity(),
                                                rowIdx)) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                          "Value is invalid, rowIdx=" + std::to_string(rowIdx));
    }
    return parseJsonStringToValue(edge_array.values(rowIdx), rowIdx, doc,
                                  "edge");
  }
  case neug::Array::TypedArrayCase::kPathArray: {
    auto& path_array = arr.path_array();
    if (!StringFormatBuffer::validateProtoValue(path_array.validity(),
                                                rowIdx)) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                          "Value is invalid, rowIdx=" + std::to_string(rowIdx));
    }
    return parseJsonStringToValue(path_array.values(rowIdx), rowIdx, doc,
                                  "path");
  }
  default:
    RETURN_STATUS_ERROR(
        neug::StatusCode::ERR_INVALID_ARGUMENT,
        "Unsupported type: " + std::to_string(arr.typed_array_case()));
  }
}

static std::string getColumnName(const reader::EntrySchema& entry_schema,
                                 size_t colIdx) {
  if (colIdx < entry_schema.columnNames.size()) {
    return entry_schema.columnNames[colIdx];
  }
  LOG(WARNING) << "Column index out of range: colIdx=" << colIdx
               << ", using default column name";
  return "col_" + std::to_string(colIdx);
}

JsonArrayStringFormatBuffer::JsonArrayStringFormatBuffer(
    const neug::QueryResponse* response, const reader::FileSchema& schema,
    const reader::EntrySchema& entry_schema)
    : StringFormatBuffer(response, schema), entry_schema_(entry_schema) {
  buffer_.SetArray();
  current_line_.SetObject();
}

void JsonArrayStringFormatBuffer::addValue(int rowIdx, int colIdx) {
  if (!validateIndex(response_, rowIdx, colIdx)) {
    THROW_IO_EXCEPTION(
        "Value index out of range: rowIdx=" + std::to_string(rowIdx) +
        ", colIdx=" + std::to_string(colIdx));
  }
  const neug::Array& column = response_->arrays(colIdx);
  auto jsonResult = formatValueToJson(column, rowIdx, document_);
  auto& allocator = document_.GetAllocator();
  WriteOptions writeOpts;
  bool ignoreErrors = writeOpts.ignore_errors.get(schema_.options);
  if (!jsonResult && !ignoreErrors) {
    THROW_IO_EXCEPTION(
        "Format value to JSON failed, rowIdx=" + std::to_string(rowIdx) +
        ", colIdx=" + std::to_string(colIdx) +
        ", error=" + jsonResult.error().ToString());
  }
  const auto& columnName = getColumnName(entry_schema_, colIdx);
  rapidjson::Value key(columnName.c_str(),
                       static_cast<rapidjson::SizeType>(columnName.size()),
                       allocator);
  if (jsonResult) {
    current_line_.AddMember(key, std::move(*jsonResult), allocator);
  } else {
    // add null value to ignore errors
    current_line_.AddMember(key, rapidjson::Value(rapidjson::kNullType),
                            allocator);
  }
  if (colIdx == static_cast<int>(response_->arrays_size()) - 1) {
    buffer_.PushBack(std::move(current_line_), allocator);
    current_line_.SetObject();
  }
}

neug::Status JsonArrayStringFormatBuffer::flush(
    std::shared_ptr<arrow::io::OutputStream> stream) {
  if (buffer_.IsArray() && buffer_.Empty()) {
    return neug::Status::OK();
  }
  const auto& jsonStr = rapidjson_stringify(buffer_);
  buffer_.Clear();
  auto writer_res = stream->Write(jsonStr.c_str(), jsonStr.size());
  if (writer_res.ok()) {
    return neug::Status::OK();
  }
  return neug::Status(
      neug::StatusCode::ERR_IO_ERROR,
      "Failed to write JSON to stream: " + writer_res.ToString());
}

JsonLStringFormatBuffer::JsonLStringFormatBuffer(
    const neug::QueryResponse* response, const reader::FileSchema& schema,
    const reader::EntrySchema& entry_schema)
    : StringFormatBuffer(response, schema), entry_schema_(entry_schema) {
  current_line_.SetObject();
  WriteOptions writeOpts;
  size_t batchSize = writeOpts.batch_rows.get(schema.options);
  if (batchSize > 0 && response->row_count() > 0) {
    buffer_.reserve(batchSize);
  }
}

void JsonLStringFormatBuffer::addValue(int rowIdx, int colIdx) {
  if (!validateIndex(response_, rowIdx, colIdx)) {
    THROW_IO_EXCEPTION(
        "Value index out of range: rowIdx=" + std::to_string(rowIdx) +
        ", colIdx=" + std::to_string(colIdx));
  }
  const neug::Array& column = response_->arrays(colIdx);
  auto jsonResult = formatValueToJson(column, rowIdx, document_);
  auto& allocator = document_.GetAllocator();
  WriteOptions writeOpts;
  bool ignoreErrors = writeOpts.ignore_errors.get(schema_.options);
  if (!jsonResult && !ignoreErrors) {
    THROW_IO_EXCEPTION(
        "Format value to JSON failed, rowIdx=" + std::to_string(rowIdx) +
        ", colIdx=" + std::to_string(colIdx) +
        ", error=" + jsonResult.error().ToString());
  }
  const auto& columnName = getColumnName(entry_schema_, colIdx);
  rapidjson::Value key(columnName.c_str(),
                       static_cast<rapidjson::SizeType>(columnName.size()),
                       allocator);
  if (jsonResult) {
    current_line_.AddMember(key, std::move(*jsonResult), allocator);
  } else {
    current_line_.AddMember(key, rapidjson::Value(rapidjson::kNullType),
                            allocator);
  }
  if (colIdx == static_cast<int>(response_->arrays_size()) - 1) {
    buffer_.push_back(std::move(current_line_));
    current_line_.SetObject();
  }
}

neug::Status JsonLStringFormatBuffer::flush(
    std::shared_ptr<arrow::io::OutputStream> stream) {
  for (const auto& val : buffer_) {
    const auto& jsonStr = rapidjson_stringify(val);
    auto ar_status = stream->Write(jsonStr.c_str(), jsonStr.size());
    if (!ar_status.ok()) {
      return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to write JSON line: " + ar_status.ToString());
    }
    ar_status = stream->Write(DEFAULT_JSON_NEWLINE, sizeof(char));
    if (!ar_status.ok()) {
      return neug::Status(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to write newline: " + ar_status.ToString());
    }
  }
  buffer_.clear();
  return neug::Status::OK();
}

static Status writeTableWithBuffer(
    StringFormatBuffer& buffer, const reader::FileSchema& schema,
    const std::shared_ptr<arrow::fs::FileSystem>& fileSystem,
    const neug::QueryResponse* table, size_t batchSize) {
  if (schema.paths.empty()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT, "Schema paths is empty");
  }
  auto stream_result = fileSystem->OpenOutputStream(schema.paths[0]);
  if (!stream_result.ok()) {
    return Status(
        StatusCode::ERR_IO_ERROR,
        "Failed to open file stream: " + stream_result.status().ToString());
  }
  auto stream = stream_result.ValueOrDie();

  if (batchSize == 0) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Batch size should be positive");
  }

  for (size_t i = 0; i < table->row_count(); ++i) {
    for (size_t j = 0; j < table->arrays_size(); ++j) {
      buffer.addValue(static_cast<int>(i), static_cast<int>(j));
    }
    if ((i + 1) % static_cast<size_t>(batchSize) == 0) {
      auto status = buffer.flush(stream);
      if (!status.ok()) {
        (void) stream->Close();
        return Status(StatusCode::ERR_IO_ERROR,
                      "Failed to flush JSON buffer: " + status.ToString());
      }
    }
  }

  auto status = buffer.flush(stream);
  if (!status.ok()) {
    (void) stream->Close();
    return Status(StatusCode::ERR_IO_ERROR,
                  "Failed to flush JSON buffer: " + status.ToString());
  }
  auto close_status = stream->Close();
  if (!close_status.ok()) {
    return Status(StatusCode::ERR_IO_ERROR,
                  "Failed to close output stream: " + close_status.ToString());
  }
  return Status::OK();
}

Status ArrowJsonArrayExportWriter::writeTable(
    const neug::QueryResponse* table) {
  if (!entry_schema_) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT, "entry_schema is null");
  }
  JsonArrayStringFormatBuffer buffer(table, schema_, *entry_schema_);
  size_t batchSize = table->row_count();
  if (batchSize == 0) {
    batchSize = 1;
  }
  // JSON Array is one single array; only flush once at the end.
  return writeTableWithBuffer(buffer, schema_, fileSystem_, table, batchSize);
}

Status ArrowJsonLExportWriter::writeTable(const neug::QueryResponse* table) {
  if (!entry_schema_) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT, "entry_schema is null");
  }
  JsonLStringFormatBuffer buffer(table, schema_, *entry_schema_);
  WriteOptions writeOpts;
  size_t batchSize = writeOpts.batch_rows.get(schema_.options);
  // JSONL: each line is a separate JSON object; safe to flush per batch.
  return writeTableWithBuffer(buffer, schema_, fileSystem_, table, batchSize);
}
}  // namespace writer

namespace function {
// write json in array format
static execution::Context jsonExecFunc(
    neug::execution::Context& ctx, reader::FileSchema& schema,
    const std::shared_ptr<reader::EntrySchema>& entry_schema,
    const neug::StorageReadInterface& graph) {
  if (schema.paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Schema paths is empty");
  }
  const auto& vfs = neug::main::MetadataRegistry::getVFS();
  const auto& fs = vfs->Provide(schema);
  auto writer = std::make_shared<neug::writer::ArrowJsonArrayExportWriter>(
      schema, fs->toArrowFileSystem(), entry_schema);
  auto status = writer->write(ctx, graph);
  if (!status.ok()) {
    THROW_IO_EXCEPTION("Export failed: " + status.ToString());
  }
  ctx.clear();
  return ctx;
}

static std::unique_ptr<ExportFuncBindData> bindFunc(
    ExportFuncBindInput& bindInput) {
  return std::make_unique<ExportFuncBindData>(
      bindInput.columnNames, bindInput.filePath, bindInput.parsingOptions);
}

function_set ExportJsonFunction::getFunctionSet() {
  function_set functionSet;
  auto exportFunc = std::make_unique<ExportFunction>(name);
  exportFunc->bind = bindFunc;
  exportFunc->execFunc = jsonExecFunc;
  functionSet.push_back(std::move(exportFunc));
  return functionSet;
}
}  // namespace function

namespace function {
// write json in newline-delimited format
static execution::Context jsonLExecFunc(
    neug::execution::Context& ctx, reader::FileSchema& schema,
    const std::shared_ptr<reader::EntrySchema>& entry_schema,
    const neug::StorageReadInterface& graph) {
  if (schema.paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Schema paths is empty");
  }
  const auto& vfs = neug::main::MetadataRegistry::getVFS();
  const auto& fs = vfs->Provide(schema);
  auto writer = std::make_shared<neug::writer::ArrowJsonLExportWriter>(
      schema, fs->toArrowFileSystem(), entry_schema);
  auto status = writer->write(ctx, graph);
  if (!status.ok()) {
    THROW_IO_EXCEPTION("Export failed: " + status.ToString());
  }
  ctx.clear();
  return ctx;
}

function_set ExportJsonLFunction::getFunctionSet() {
  function_set functionSet;
  auto exportFunc = std::make_unique<ExportFunction>(name);
  exportFunc->bind = bindFunc;
  exportFunc->execFunc = jsonLExecFunc;
  functionSet.push_back(std::move(exportFunc));
  return functionSet;
}
}  // namespace function
}  // namespace neug
