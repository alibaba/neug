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

#include "neug/compiler/function/export/json_export_function.h"

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/writer.h>
#include "neug/common/export/export_result.h"
#include "neug/common/types/data_chunk.h"
#include "neug/common/types/value.h"
#include "neug/utils/io/stream/output_stream.h"

#include <string>

#include "neug/common/types/property_types.h"
#include "neug/compiler/function/read_function.h"
#include "neug/compiler/main/metadata_registry.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/result.h"

namespace neug {
namespace writer {

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

static neug::result<rapidjson::Value> valueToJsonValue(
    const Value& value, rapidjson::Document& doc) {
  auto& allocator = doc.GetAllocator();
  if (value.IsNull()) {
    return rapidjson::Value(rapidjson::kNullType);
  }
  switch (value.type().id()) {
  case DataTypeId::kBoolean:
    return rapidjson::Value(value.GetValue<bool>());
  case DataTypeId::kInt32:
    return rapidjson::Value(value.GetValue<int32_t>());
  case DataTypeId::kInt64:
    return rapidjson::Value(value.GetValue<int64_t>());
  case DataTypeId::kUInt32:
    return rapidjson::Value(value.GetValue<uint32_t>());
  case DataTypeId::kUInt64:
    return rapidjson::Value(value.GetValue<uint64_t>());
  case DataTypeId::kFloat:
    return rapidjson::Value(value.GetValue<float>());
  case DataTypeId::kDouble:
    return rapidjson::Value(value.GetValue<double>());
  case DataTypeId::kVarchar: {
    const auto& str = StringValue::Get(value);
    rapidjson::Value v;
    v.SetString(str.c_str(), static_cast<rapidjson::SizeType>(str.size()),
                allocator);
    return v;
  }
  case DataTypeId::kDate: {
    const auto& s = value.GetValue<date_t>().to_string();
    rapidjson::Value v;
    v.SetString(s.c_str(), static_cast<rapidjson::SizeType>(s.size()),
                allocator);
    return v;
  }
  case DataTypeId::kTimestampMs: {
    const auto& s = value.GetValue<timestamp_ms_t>().to_string();
    rapidjson::Value v;
    v.SetString(s.c_str(), static_cast<rapidjson::SizeType>(s.size()),
                allocator);
    return v;
  }
  case DataTypeId::kInterval: {
    const auto& s = value.GetValue<interval_t>().to_string();
    rapidjson::Value v;
    v.SetString(s.c_str(), static_cast<rapidjson::SizeType>(s.size()),
                allocator);
    return v;
  }
  case DataTypeId::kList: {
    rapidjson::Value arr(rapidjson::kArrayType);
    const auto& children = ListValue::GetChildren(value);
    for (const auto& child : children) {
      auto child_json = valueToJsonValue(child, doc);
      if (!child_json) {
        return tl::make_unexpected(child_json.error());
      }
      arr.PushBack(std::move(*child_json), allocator);
    }
    return arr;
  }
  case DataTypeId::kArray: {
    rapidjson::Value arr(rapidjson::kArrayType);
    const auto& children = ArrayValue::GetChildren(value);
    for (const auto& child : children) {
      auto child_json = valueToJsonValue(child, doc);
      if (!child_json) {
        return tl::make_unexpected(child_json.error());
      }
      arr.PushBack(std::move(*child_json), allocator);
    }
    return arr;
  }
  case DataTypeId::kStruct: {
    rapidjson::Value obj(rapidjson::kObjectType);
    const auto& children = StructValue::GetChildren(value);
    const auto& field_names = StructType::GetFieldNames(value.type());
    for (size_t i = 0; i < children.size(); ++i) {
      const auto field_name = i < field_names.size()
                                  ? field_names[i]
                                  : ("field_" + std::to_string(i));
      rapidjson::Value key(field_name.c_str(),
                           static_cast<rapidjson::SizeType>(field_name.size()),
                           allocator);
      auto child_json = valueToJsonValue(children[i], doc);
      if (!child_json) {
        return tl::make_unexpected(child_json.error());
      }
      obj.AddMember(key, std::move(*child_json), allocator);
    }
    return obj;
  }
  default: {
    const auto& s = value.to_string();
    rapidjson::Value v;
    v.SetString(s.c_str(), static_cast<rapidjson::SizeType>(s.size()),
                allocator);
    return v;
  }
  }
}

static bool isSerializedGraphJson(DataTypeId id) {
  return id == DataTypeId::kVertex || id == DataTypeId::kEdge ||
         id == DataTypeId::kPath;
}

static const DataType& exportSourceType(
    const std::vector<DataType>& source_types, size_t col) {
  static const DataType kDefaultVarchar(DataTypeId::kVarchar);
  if (col < source_types.size()) {
    return source_types[col];
  }
  return kDefaultVarchar;
}

// Convert one cell to a rapidjson value. When source_types[col] is a graph
// type, the VARCHAR payload is pre-serialized JSON and is parsed back into a
// structured value for inline emission.
static neug::result<rapidjson::Value> cellToJsonValue(
    const IContextColumn& column, size_t row, const DataType& source_type,
    rapidjson::Document& doc) {
  Value value = column.get_elem(row);
  if (isSerializedGraphJson(source_type.id())) {
    const auto& str = StringValue::Get(value);
    if (str.empty()) {
      return rapidjson::Value(rapidjson::kNullType);
    }
    return parseJsonStringToValue(str, static_cast<int>(row), doc,
                                  source_type.ToString().c_str());
  }
  return valueToJsonValue(value, doc);
}

static Status writeChunkAsJsonArray(const DataChunk& chunk,
                                    const reader::FileSchema& schema,
                                    const reader::EntrySchema& entry_schema,
                                    const std::vector<DataType>& source_types) {
  if (schema.paths.empty()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT, "Schema paths is empty");
  }
  auto stream = io::openLocalOutputStream(schema.paths[0]);
  if (!stream) {
    return Status(StatusCode::ERR_IO_ERROR, "Failed to open output file");
  }

  rapidjson::Document doc;
  doc.SetArray();
  auto& allocator = doc.GetAllocator();
  for (size_t row = 0; row < chunk.row_num(); ++row) {
    rapidjson::Value line(rapidjson::kObjectType);
    for (size_t col = 0; col < chunk.col_num(); ++col) {
      if (chunk.columns[col] == nullptr) {
        continue;
      }
      const auto& column_name = col < entry_schema.columnNames.size()
                                    ? entry_schema.columnNames[col]
                                    : ("col_" + std::to_string(col));
      rapidjson::Value key(column_name.c_str(),
                           static_cast<rapidjson::SizeType>(column_name.size()),
                           allocator);
      if (!chunk.columns[col]->has_value(row)) {
        line.AddMember(key, rapidjson::Value(rapidjson::kNullType), allocator);
        continue;
      }
      auto json_val = cellToJsonValue(*chunk.columns[col], row,
                                      exportSourceType(source_types, col), doc);
      if (!json_val) {
        (void) stream->Close();
        return json_val.error();
      }
      line.AddMember(key, std::move(*json_val), allocator);
    }
    doc.PushBack(line, allocator);
  }

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);
  auto status =
      stream->Write(reinterpret_cast<const uint8_t*>(buffer.GetString()),
                    static_cast<int64_t>(buffer.GetSize()));
  if (!status.ok()) {
    (void) stream->Close();
    return status;
  }
  return stream->Close();
}

static Status writeChunkAsJsonL(const DataChunk& chunk,
                                const reader::FileSchema& schema,
                                const reader::EntrySchema& entry_schema,
                                const std::vector<DataType>& source_types) {
  if (schema.paths.empty()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT, "Schema paths is empty");
  }
  auto stream = io::openLocalOutputStream(schema.paths[0]);
  if (!stream) {
    return Status(StatusCode::ERR_IO_ERROR, "Failed to open output file");
  }

  for (size_t row = 0; row < chunk.row_num(); ++row) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    for (size_t col = 0; col < chunk.col_num(); ++col) {
      if (chunk.columns[col] == nullptr) {
        continue;
      }
      const auto& column_name = col < entry_schema.columnNames.size()
                                    ? entry_schema.columnNames[col]
                                    : ("col_" + std::to_string(col));
      rapidjson::Value key(column_name.c_str(),
                           static_cast<rapidjson::SizeType>(column_name.size()),
                           allocator);
      if (!chunk.columns[col]->has_value(row)) {
        doc.AddMember(key, rapidjson::Value(rapidjson::kNullType), allocator);
        continue;
      }
      auto json_val = cellToJsonValue(*chunk.columns[col], row,
                                      exportSourceType(source_types, col), doc);
      if (!json_val) {
        (void) stream->Close();
        return json_val.error();
      }
      doc.AddMember(key, std::move(*json_val), allocator);
    }
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    auto status =
        stream->Write(reinterpret_cast<const uint8_t*>(buffer.GetString()),
                      static_cast<int64_t>(buffer.GetSize()));
    if (!status.ok()) {
      (void) stream->Close();
      return status;
    }
    status = stream->Write(
        reinterpret_cast<const uint8_t*>(DEFAULT_JSON_NEWLINE), sizeof(char));
    if (!status.ok()) {
      (void) stream->Close();
      return status;
    }
  }
  return stream->Close();
}

Status JsonArrayExportWriter::write(const DataChunk& chunk,
                                    const std::vector<DataType>& source_types) {
  if (!entry_schema_) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT, "entry_schema is null");
  }
  if (!source_types.empty() && source_types.size() != chunk.col_num()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "source_types size mismatch: expected " +
                      std::to_string(chunk.col_num()) + ", got " +
                      std::to_string(source_types.size()));
  }
  return writeChunkAsJsonArray(chunk, schema_, *entry_schema_, source_types);
}

Status JsonLExportWriter::write(const DataChunk& chunk,
                                const std::vector<DataType>& source_types) {
  if (!entry_schema_) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT, "entry_schema is null");
  }
  if (!source_types.empty() && source_types.size() != chunk.col_num()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "source_types size mismatch: expected " +
                      std::to_string(chunk.col_num()) + ", got " +
                      std::to_string(source_types.size()));
  }
  return writeChunkAsJsonL(chunk, schema_, *entry_schema_, source_types);
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
  auto writer = std::make_shared<neug::writer::JsonArrayExportWriter>(
      schema, entry_schema);
  auto export_result = neug::materialize_result_for_export(ctx, graph);
  auto status = writer->write(export_result.chunk, export_result.source_types);
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
  auto writer =
      std::make_shared<neug::writer::JsonLExportWriter>(schema, entry_schema);
  auto export_result = neug::materialize_result_for_export(ctx, graph);
  auto status = writer->write(export_result.chunk, export_result.source_types);
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
