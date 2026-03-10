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

#include "neug/utils/writer/writer.h"
#include <arrow/json/options.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>
#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_binary.h"
#include "arrow/scalar.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "neug/execution/common/operators/retrieve/sink.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/reader/options.h"
#include "neug/utils/service_utils.h"

#include "arrow/util/formatting.h"

namespace neug {
namespace writer {
namespace {

// Format temporal (date32/date64/timestamp) to string using Arrow's
// StringFormatter, same as arrow::csv::WriteCSV (Cast to utf8). Returns
// ISO-style string or error.
template <typename Formatter, typename ValueType>
static arrow::Result<std::string> formatTemporalWithArrow(Formatter& formatter,
                                                          ValueType value) {
  std::string out;
  auto appender = [&out](std::string_view v) -> arrow::Status {
    out.append(v.data(), v.size());
    return arrow::Status::OK();
  };
  ARROW_RETURN_NOT_OK(formatter(value, std::move(appender)));
  return out;
}

// Internal implementation with allocator. Caller must use the returned Value
// immediately (e.g. stringify) before next call.
arrow::Result<rapidjson::Value> arrowValueToJsonImpl(
    const std::shared_ptr<arrow::Array>& column, int64_t index,
    rapidjson::Document::AllocatorType& allocator) {
  const arrow::Array& arr = *column;
  if (arr.IsNull(index)) {
    return rapidjson::Value(rapidjson::kNullType);
  }

  switch (arr.type()->id()) {
  case arrow::Type::BOOL:
    return rapidjson::Value(
        std::static_pointer_cast<arrow::BooleanArray>(column)->Value(index));
  case arrow::Type::INT32:
    return rapidjson::Value(
        std::static_pointer_cast<arrow::Int32Array>(column)->Value(index));
  case arrow::Type::INT64:
    return rapidjson::Value(
        std::static_pointer_cast<arrow::Int64Array>(column)->Value(index));
  case arrow::Type::UINT32:
    return rapidjson::Value(
        std::static_pointer_cast<arrow::UInt32Array>(column)->Value(index));
  case arrow::Type::UINT64:
    return rapidjson::Value(
        std::static_pointer_cast<arrow::UInt64Array>(column)->Value(index));
  case arrow::Type::FLOAT:
    return rapidjson::Value(
        std::static_pointer_cast<arrow::FloatArray>(column)->Value(index));
  case arrow::Type::DOUBLE:
    return rapidjson::Value(
        std::static_pointer_cast<arrow::DoubleArray>(column)->Value(index));
  case arrow::Type::STRING: {
    auto view =
        std::static_pointer_cast<arrow::StringArray>(column)->GetView(index);
    return rapidjson::Value(
        view.data(), static_cast<rapidjson::SizeType>(view.size()), allocator);
  }
  case arrow::Type::LARGE_STRING: {
    auto view =
        std::static_pointer_cast<arrow::LargeStringArray>(column)->GetView(
            index);
    return rapidjson::Value(
        view.data(), static_cast<rapidjson::SizeType>(view.size()), allocator);
  }
  case arrow::Type::DATE32: {
    auto date32_arr = std::static_pointer_cast<arrow::Date32Array>(column);
    arrow::internal::StringFormatter<arrow::Date32Type> formatter(
        date32_arr->type().get());
    ARROW_ASSIGN_OR_RAISE(
        std::string iso,
        formatTemporalWithArrow(formatter, date32_arr->Value(index)));
    return rapidjson::Value(
        iso.c_str(), static_cast<rapidjson::SizeType>(iso.size()), allocator);
  }
  case arrow::Type::DATE64: {
    auto date64_arr = std::static_pointer_cast<arrow::Date64Array>(column);
    arrow::internal::StringFormatter<arrow::Date64Type> formatter(
        date64_arr->type().get());
    ARROW_ASSIGN_OR_RAISE(
        std::string iso,
        formatTemporalWithArrow(formatter, date64_arr->Value(index)));
    return rapidjson::Value(
        iso.c_str(), static_cast<rapidjson::SizeType>(iso.size()), allocator);
  }
  case arrow::Type::TIMESTAMP: {
    auto ts_arr = std::static_pointer_cast<arrow::TimestampArray>(column);
    arrow::internal::StringFormatter<arrow::TimestampType> formatter(
        ts_arr->type().get());
    ARROW_ASSIGN_OR_RAISE(
        std::string iso,
        formatTemporalWithArrow(formatter, ts_arr->Value(index)));
    return rapidjson::Value(
        iso.c_str(), static_cast<rapidjson::SizeType>(iso.size()), allocator);
  }
  case arrow::Type::NA:
    return rapidjson::Value(rapidjson::kNullType);
  case arrow::Type::LIST: {
    auto list_arr = std::static_pointer_cast<arrow::ListArray>(column);
    rapidjson::Value arr_val(rapidjson::kArrayType);
    auto values = list_arr->values();
    int64_t offset = list_arr->value_offset(index);
    int64_t length = list_arr->value_length(index);
    for (int64_t j = 0; j < length; ++j) {
      ARROW_ASSIGN_OR_RAISE(
          auto elem, arrowValueToJsonImpl(values, offset + j, allocator));
      arr_val.PushBack(elem, allocator);
    }
    return arr_val;
  }
  case arrow::Type::LARGE_LIST: {
    auto list_arr = std::static_pointer_cast<arrow::LargeListArray>(column);
    rapidjson::Value arr_val(rapidjson::kArrayType);
    auto values = list_arr->values();
    int64_t offset = list_arr->value_offset(index);
    int64_t length = list_arr->value_length(index);
    for (int64_t j = 0; j < length; ++j) {
      ARROW_ASSIGN_OR_RAISE(
          auto elem, arrowValueToJsonImpl(values, offset + j, allocator));
      arr_val.PushBack(elem, allocator);
    }
    return arr_val;
  }
  case arrow::Type::MAP: {
    auto map_arr = std::static_pointer_cast<arrow::MapArray>(column);
    rapidjson::Value obj(rapidjson::kObjectType);
    auto keys = map_arr->keys();
    auto items = map_arr->items();
    int64_t offset = map_arr->value_offset(index);
    int64_t length = map_arr->value_length(index);
    for (int64_t j = 0; j < length; ++j) {
      ARROW_ASSIGN_OR_RAISE(auto k,
                            arrowValueToJsonImpl(keys, offset + j, allocator));
      ARROW_ASSIGN_OR_RAISE(auto v,
                            arrowValueToJsonImpl(items, offset + j, allocator));
      if (k.IsString()) {
        obj.AddMember(k, v, allocator);
      } else {
        rapidjson::Value key_str(rapidjson::kStringType);
        key_str.SetString(neug::rapidjson_stringify(k).c_str(), allocator);
        obj.AddMember(key_str, v, allocator);
      }
    }
    return obj;
  }
  case arrow::Type::STRUCT: {
    auto struct_arr = std::static_pointer_cast<arrow::StructArray>(column);
    rapidjson::Value obj(rapidjson::kObjectType);
    const auto& type =
        arrow::internal::checked_cast<const arrow::StructType&>(*arr.type());
    for (int i = 0; i < type.num_fields(); ++i) {
      auto field_arr = struct_arr->field(i);
      ARROW_ASSIGN_OR_RAISE(auto v,
                            arrowValueToJsonImpl(field_arr, index, allocator));
      rapidjson::Value name(
          type.field(i)->name().c_str(),
          static_cast<rapidjson::SizeType>(type.field(i)->name().size()),
          allocator);
      obj.AddMember(name, v, allocator);
    }
    return obj;
  }
  case arrow::Type::SPARSE_UNION: {
    auto union_arr = std::static_pointer_cast<arrow::SparseUnionArray>(column);
    int child_pos = union_arr->child_id(index);
    auto child = union_arr->field(child_pos);
    return arrowValueToJsonImpl(child, index, allocator);
  }
  case arrow::Type::DENSE_UNION: {
    auto union_arr = std::static_pointer_cast<arrow::DenseUnionArray>(column);
    int child_pos = union_arr->child_id(index);
    auto child = union_arr->field(child_pos);
    int64_t child_index = union_arr->value_offset(index);
    return arrowValueToJsonImpl(child, child_index, allocator);
  }
  default: {
    return arrow::Status::Invalid("Unsupported type: ", arr.type()->ToString());
  }
  }
}

}  // namespace

// Convert arrow value to json value. Supported types: BOOL, INT32, INT64,
// UINT32, UINT64, FLOAT, DOUBLE, STRING, LARGE_STRING, DATE32, DATE64,
// TIMESTAMP, LIST, MAP, STRUCT, SPARSE_UNION, NA. Returned Value is only
// valid until the next call (uses thread-local Document).
arrow::Result<rapidjson::Value> arrowValueToJson(
    std::shared_ptr<arrow::Array> column, size_t index) {
  if (!column) {
    return arrow::Status::Invalid("column is null");
  }
  if (index >= static_cast<size_t>(column->length())) {
    return arrow::Status::IndexError("index out of range");
  }
  thread_local rapidjson::Document doc;
  doc.Clear();
  return arrowValueToJsonImpl(column, static_cast<int64_t>(index),
                              doc.GetAllocator());
}

// Replace double-quotes in JSON string with single-quotes so that when written
// to CSV, the writer does not escape them (no "" in output). Output is not
// standard JSON but is readable and avoids CSV quote doubling.
static std::string jsonStrForCsv(std::string json_str) {
  for (auto& ch : json_str) {
    if (ch == '"') {
      ch = '\'';
    }
  }
  return json_str;
}

// Convert chunked array with composite types (LIST, STRUCT, MAP, UNION) to
// chunked array with string type.
arrow::Result<std::shared_ptr<arrow::ChunkedArray>> chunkedArrayToString(
    const arrow::ChunkedArray& chunked) {
  arrow::StringBuilder builder(arrow::default_memory_pool());
  size_t total_rows = chunked.length();
  if (total_rows > 0) {
    auto first_arr = chunked.chunk(0);
    ARROW_ASSIGN_OR_RAISE(auto first_row, arrowValueToJson(first_arr, 0));
    std::string first_str = neug::rapidjson_stringify(first_row);
    first_str = jsonStrForCsv(std::move(first_str));
    size_t first_row_size = first_str.size();
    // reserve total rows and total bytes
    ARROW_RETURN_NOT_OK(builder.Reserve(static_cast<int64_t>(total_rows)));
    ARROW_RETURN_NOT_OK(
        builder.ReserveData(static_cast<int64_t>(first_row_size * total_rows)));
  }
  for (size_t c = 0; c < chunked.num_chunks(); ++c) {
    auto arr = chunked.chunk(c);
    for (size_t i = 0; i < arr->length(); ++i) {
      ARROW_ASSIGN_OR_RAISE(auto json, arrowValueToJson(arr, i));
      std::string json_str = neug::rapidjson_stringify(json);
      json_str = jsonStrForCsv(std::move(json_str));
      ARROW_RETURN_NOT_OK(builder.Append(std::move(json_str)));
    }
  }
  ARROW_ASSIGN_OR_RAISE(auto str_array, builder.Finish());
  return std::make_shared<arrow::ChunkedArray>(str_array);
}

// Try to convert the arrow table to a table that is supported by the arrow::CSV
// writer.
arrow::Result<std::shared_ptr<arrow::Table>> convertToCsvWritableTable(
    const arrow::Table& table) {
  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (size_t column = 0; column < table.num_columns(); ++column) {
    const auto& col = table.column(column);
    arrow::Type::type id = col->type()->id();
    switch (id) {
    case arrow::Type::BOOL:
    case arrow::Type::INT32:
    case arrow::Type::INT64:
    case arrow::Type::UINT32:
    case arrow::Type::UINT64:
    case arrow::Type::FLOAT:
    case arrow::Type::DOUBLE:
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
    case arrow::Type::DATE32:
    case arrow::Type::DATE64:
    case arrow::Type::TIMESTAMP: {
      columns.push_back(col);
      fields.push_back(table.schema()->field(column));
      break;
    }
    case arrow::Type::LIST:
    case arrow::Type::MAP:
    case arrow::Type::STRUCT:
    case arrow::Type::SPARSE_UNION: {
      ARROW_ASSIGN_OR_RAISE(auto str_chunked, chunkedArrayToString(*col));
      columns.push_back(str_chunked);
      fields.push_back(arrow::field(table.schema()->field(column)->name(),
                                    arrow::utf8(),
                                    table.schema()->field(column)->nullable()));
      break;
    }
    default: {
      return arrow::Status::Invalid(
          "The column type: ",
          col->type()->ToString() + " cannot be written to csv");
    }
    }
  }
  auto schema = arrow::schema(fields);
  return arrow::Table::Make(schema, columns);
}

Status ArrowExportWriter::write(const execution::Context& context,
                                const StorageReadInterface& graph) {
  auto result = execution::Sink::sink_neug(context, graph);
  auto table = result.table();
  if (!table) {
    return Status(StatusCode::ERR_IO_ERROR,
                  "Failed to get table from query result, table is null");
  }

  std::shared_ptr<arrow::Table> table_to_write = table;
  if (entry_schema_ && !entry_schema_->columnNames.empty()) {
    auto rename_result = table->RenameColumns(entry_schema_->columnNames);
    if (!rename_result.ok()) {
      return Status(StatusCode::ERR_IO_ERROR,
                    "Failed to set table field names: " +
                        rename_result.status().ToString());
    }
    table_to_write = rename_result.ValueOrDie();
  }

  return writeTable(table_to_write);
}

Status ArrowCsvExportWriter::writeTable(
    const std::shared_ptr<arrow::Table>& table) {
  // build csv options
  arrow::csv::WriteOptions writeOpts = arrow::csv::WriteOptions::Defaults();
  reader::CSVParseOptions csvParseOpts;
  WriteOptions csvWriteOpts;
  writeOpts.delimiter = csvParseOpts.delimiter.get(schema_.options);
  writeOpts.batch_size = csvWriteOpts.batch_rows.get(schema_.options);
  writeOpts.include_header = csvParseOpts.has_header.get(schema_.options);
  writeOpts.eol = "\n";

  if (schema_.paths.empty()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT, "Schema paths is empty");
  }
  auto stream_result = fileSystem_->OpenOutputStream(schema_.paths[0]);
  if (!stream_result.ok()) {
    return Status(
        StatusCode::ERR_IO_ERROR,
        "Failed to open file stream: " + stream_result.status().ToString());
  }
  auto stream = stream_result.ValueOrDie();

  // Arrow CSV writer does not support struct/list/map/union etc, convert those
  // columns to json string before writing.
  auto table_result = convertToCsvWritableTable(*table);
  if (!table_result.ok()) {
    return Status(
        StatusCode::ERR_IO_ERROR,
        "Failed to prepare table for CSV: " + table_result.status().ToString());
  }
  auto table_to_write = table_result.ValueOrDie();

  auto write_result =
      arrow::csv::WriteCSV(*table_to_write, writeOpts, stream.get());
  if (!write_result.ok()) {
    return Status(StatusCode::ERR_IO_ERROR,
                  "Failed to write table to file: " + write_result.ToString());
  }
  return Status::OK();
}

}  // namespace writer
}  // namespace neug
