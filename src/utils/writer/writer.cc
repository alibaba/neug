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
#include <arrow/result.h>
#include <arrow/status.h>
#include "neug/execution/common/operators/retrieve/sink.h"
#include "neug/generated/proto/response/response.pb.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/types.h"
#include "neug/utils/reader/options.h"

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <string>

namespace neug {
namespace writer {

bool StringFormatBuffer::validateIndex(const neug::QueryResponse* response,
                                       int rowIdx, int colIdx) {
  if (response == nullptr)
    return false;
  if (rowIdx < 0 || rowIdx >= response->row_count())
    return false;
  if (colIdx < 0 || static_cast<size_t>(colIdx) >= response->arrays_size()) {
    return false;
  }
  return true;
}

bool StringFormatBuffer::validateProtoValue(const std::string& validity,
                                            int rowIdx) {
  return validity.empty() ||
         (static_cast<uint8_t>(validity[static_cast<size_t>(rowIdx) >> 3]) >>
          (rowIdx & 7)) &
             1;
}

#define TYPED_PRIMITIVE_ARRAY_TO_JSON(CASE_ENUM, GETTER_METHOD)   \
  case neug::Array::TypedArrayCase::CASE_ENUM: {                  \
    auto typed_array = arr.GETTER_METHOD();                       \
    if (!validateProtoValue(typed_array.validity(), rowIdx)) {    \
      return arrow::Status::Invalid("Value is invalid, rowIdx=" + \
                                    std::to_string(rowIdx));      \
    }                                                             \
    return std::to_string(typed_array.values(rowIdx));            \
  }

CSVStringFormatBuffer::CSVStringFormatBuffer(
    const neug::QueryResponse* response, const reader::FileSchema& schema,
    const reader::EntrySchema& entry_schema)
    : StringFormatBuffer(response, schema), entry_schema_(entry_schema) {
  capacity_ = DEFAULT_CAPACITY;
  WriteOptions writeOpts;
  size_t batchSize = writeOpts.batch_rows.get(schema.options);
  if (batchSize > 0 && response->arrays_size() > 0 &&
      response->row_count() > 0) {
    capacity_ = DEFAULT_CAPACITY * batchSize * response->arrays_size();
  }
  blob_.data = std::make_unique<uint8_t[]>(capacity_);
  blob_.size = 0;
  data_ = blob_.data.get();
}

arrow::Result<std::string> CSVStringFormatBuffer::formatValueToStr(
    const neug::Array& arr, int rowIdx) {
  switch (arr.typed_array_case()) {
    TYPED_PRIMITIVE_ARRAY_TO_JSON(kBoolArray, bool_array)
    TYPED_PRIMITIVE_ARRAY_TO_JSON(kInt32Array, int32_array)
    TYPED_PRIMITIVE_ARRAY_TO_JSON(kInt64Array, int64_array)
    TYPED_PRIMITIVE_ARRAY_TO_JSON(kUint32Array, uint32_array)
    TYPED_PRIMITIVE_ARRAY_TO_JSON(kUint64Array, uint64_array)
    TYPED_PRIMITIVE_ARRAY_TO_JSON(kFloatArray, float_array)
    TYPED_PRIMITIVE_ARRAY_TO_JSON(kDoubleArray, double_array)
  case neug::Array::TypedArrayCase::kStringArray: {
    auto string_array = arr.string_array();
    if (!validateProtoValue(string_array.validity(), rowIdx)) {
      return arrow::Status::Invalid("Value is invalid, rowIdx=" +
                                    std::to_string(rowIdx));
    }
    return string_array.values(rowIdx);
  }
  case neug::Array::TypedArrayCase::kDateArray: {
    auto date32_arr = arr.date_array();
    if (!validateProtoValue(date32_arr.validity(), rowIdx)) {
      return arrow::Status::Invalid("Value is invalid, rowIdx=" +
                                    std::to_string(rowIdx));
    }
    Date date_value;
    date_value.from_timestamp(date32_arr.values(rowIdx));
    return date_value.to_string();
  }
  case neug::Array::TypedArrayCase::kTimestampArray: {
    auto timestamp_array = arr.timestamp_array();
    if (!validateProtoValue(timestamp_array.validity(), rowIdx)) {
      return arrow::Status::Invalid("Value is invalid, rowIdx=" +
                                    std::to_string(rowIdx));
    }
    DateTime dt_value(timestamp_array.values(rowIdx));
    return dt_value.to_string();
  }
  case neug::Array::TypedArrayCase::kIntervalArray: {
    auto interval_array = arr.interval_array();
    if (!validateProtoValue(interval_array.validity(), rowIdx)) {
      return arrow::Status::Invalid("Value is invalid, rowIdx=" +
                                    std::to_string(rowIdx));
    }
    return interval_array.values(rowIdx);
  }
  case neug::Array::TypedArrayCase::kListArray: {
    auto list_array = arr.list_array();
    if (!validateProtoValue(list_array.validity(), rowIdx)) {
      return arrow::Status::Invalid("Value is invalid, rowIdx=" +
                                    std::to_string(rowIdx));
    }
    std::string list_val;
    list_val.append("[");
    uint32_t list_size =
        list_array.offsets(rowIdx + 1) - list_array.offsets(rowIdx);
    size_t offset = list_array.offsets(rowIdx);
    for (uint32_t i = 0; i < list_size; ++i) {
      ARROW_ASSIGN_OR_RAISE(
          auto elem, formatValueToStr(list_array.elements(), offset + i));
      if (i > 0) {
        list_val.append(",");
      }
      list_val.append(elem);
    }
    list_val.append("]");
    return list_val;
  }
  case neug::Array::TypedArrayCase::kStructArray: {
    auto struct_arr = arr.struct_array();
    std::string list_val;
    list_val.append("[");
    for (int i = 0; i < struct_arr.fields_size(); ++i) {
      const auto& field = struct_arr.fields(i);
      ARROW_ASSIGN_OR_RAISE(auto elem, formatValueToStr(field, rowIdx));
      if (i > 0) {
        list_val.append(",");
      }
      list_val.append(elem);
    }
    list_val.append("]");
    return list_val;
  }
  case neug::Array::TypedArrayCase::kVertexArray: {
    auto vertex_array = arr.vertex_array();
    if (!validateProtoValue(vertex_array.validity(), rowIdx)) {
      return arrow::Status::Invalid("Value is invalid, rowIdx=" +
                                    std::to_string(rowIdx));
    }
    return vertex_array.values(rowIdx);
  }
  case neug::Array::TypedArrayCase::kEdgeArray: {
    auto edge_array = arr.edge_array();
    if (!validateProtoValue(edge_array.validity(), rowIdx)) {
      return arrow::Status::Invalid("Value is invalid, rowIdx=" +
                                    std::to_string(rowIdx));
    }
    return edge_array.values(rowIdx);
  }
  case neug::Array::TypedArrayCase::kPathArray: {
    auto path_array = arr.path_array();
    if (!validateProtoValue(path_array.validity(), rowIdx)) {
      return arrow::Status::Invalid("Value is invalid, rowIdx=" +
                                    std::to_string(rowIdx));
    }
    return path_array.values(rowIdx);
  }
  default: {
    return arrow::Status::Invalid("Unsupported type: " +
                                  std::to_string(arr.typed_array_case()));
  }
  }
}

std::string CSVStringFormatBuffer::addEscapes(char toEscape, char escape,
                                              const std::string& val) {
  uint64_t i = 0;
  std::string escapedStr = "";
  auto found = val.find(toEscape);

  while (found != std::string::npos) {
    while (i < found) {
      escapedStr += val[i];
      i++;
    }
    escapedStr += escape;
    found = val.find(toEscape, found + sizeof(escape));
  }
  while (i < val.length()) {
    escapedStr += val[i];
    i++;
  }
  return escapedStr;
}

void CSVStringFormatBuffer::write(const uint8_t* buffer, uint64_t len) {
  if (len == 0) {
    return;
  }
  if (buffer == nullptr) {
    THROW_IO_EXCEPTION("CSVStringFormatBuffer::write called with null buffer");
  }
  // Overflow-safe: need grow when (blob.size + len > capacity) without
  // computing blob.size + len (which can overflow).
  const bool need_grow = (len > capacity_) || (blob_.size > capacity_ - len);
  if (need_grow) {
    size_t old_capacity = capacity_;
    do {
      if (capacity_ > SIZE_MAX / 2) {
        THROW_IO_EXCEPTION("CSV buffer capacity overflow");
      }
      capacity_ *= 2;
    } while ((len > capacity_) || (blob_.size > capacity_ - len));
    auto new_data = std::make_unique<uint8_t[]>(capacity_);
    // Copy only up to old capacity to avoid reading past old buffer
    size_t copy_len = (blob_.size < old_capacity) ? blob_.size : old_capacity;
    if (copy_len > 0 && data_ != nullptr) {
      memcpy(new_data.get(), data_, copy_len);
    }
    blob_.size = copy_len;
    blob_.data = std::move(new_data);
    data_ = blob_.data.get();
  }

  memcpy(data_ + blob_.size, buffer, len);
  blob_.size += len;
}

void CSVStringFormatBuffer::addValue(int rowIdx, int colIdx) {
  if (!validateIndex(response_, rowIdx, colIdx)) {
    THROW_IO_EXCEPTION(
        "Value index out of range: rowIdx=" + std::to_string(rowIdx) +
        ", colIdx=" + std::to_string(colIdx));
  }
  // write with header
  WriteOptions writeOpts;
  if (rowIdx == 0 && colIdx == 0 && writeOpts.has_header.get(schema_.options)) {
    auto& columnNames = entry_schema_.columnNames;
    if (!columnNames.empty()) {
      char delim = writeOpts.delimiter.get(schema_.options);
      for (size_t col = 0; col < columnNames.size(); ++col) {
        if (col > 0) {
          write(reinterpret_cast<const uint8_t*>(&delim), sizeof(char));
        }
        const auto& name = columnNames[col];
        write(reinterpret_cast<const uint8_t*>(name.c_str()), name.length());
      }
      write(reinterpret_cast<const uint8_t*>(DEFAULT_CSV_NEWLINE),
            sizeof(char));
    }
  }

  const neug::Array& column = response_->arrays(colIdx);
  auto strResult = formatValueToStr(column, rowIdx);
  if (!strResult.ok() && !writeOpts.ignore_errors.get(schema_.options)) {
    THROW_IO_EXCEPTION(
        "Format value to string failed, rowIdx=" + std::to_string(rowIdx) +
        ", colIdx=" + std::to_string(colIdx) +
        ", error=" + strResult.status().ToString());
  }
  if (colIdx > 0) {
    char delim = writeOpts.delimiter.get(schema_.options);
    write(reinterpret_cast<const uint8_t*>(&delim), sizeof(char));
  }
  if (strResult.ok()) {
    auto str = strResult.ValueOrDie();
    if (!str.empty()) {
      // add quotes for string type values
      if (column.has_string_array()) {
        char escapeChar = writeOpts.escape_char.get(schema_.options);
        char quoteChar = writeOpts.quote_char.get(schema_.options);
        str = addEscapes(escapeChar, escapeChar, str);
        if (escapeChar != quoteChar) {
          str = addEscapes(quoteChar, escapeChar, str);
        }
        write(reinterpret_cast<const uint8_t*>(&quoteChar), sizeof(char));
        write(reinterpret_cast<const uint8_t*>(str.c_str()), str.length());
        write(reinterpret_cast<const uint8_t*>(&quoteChar), sizeof(char));
      } else {
        write(reinterpret_cast<const uint8_t*>(str.c_str()), str.length());
      }
    }
  } else {
    write(reinterpret_cast<const uint8_t*>(DEFAULT_NULL_STR),
          strlen(DEFAULT_NULL_STR));
  }
  if (colIdx == response_->arrays_size() - 1) {
    // the last column, add newline
    write(reinterpret_cast<const uint8_t*>(DEFAULT_CSV_NEWLINE), sizeof(char));
  }
}

arrow::Status CSVStringFormatBuffer::flush(
    std::shared_ptr<arrow::io::OutputStream> stream) {
  if (blob_.size > 0) {
    auto status = stream->Write(data_, blob_.size);
    blob_.size = 0;
    return status;
  }
  return arrow::Status::OK();
}

Status ArrowExportWriter::write(const execution::Context& context,
                                const StorageReadInterface& graph) {
  neug::QueryResponse response;
  execution::Sink::sink_results(context, graph, &response);
  return writeTable(&response);
}

Status ArrowCsvExportWriter::writeTable(const neug::QueryResponse* table) {
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

  WriteOptions writeOpts;
  auto batchSize = writeOpts.batch_rows.get(schema_.options);
  if (batchSize <= 0) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Batch size should be positive");
  }
  auto csvBuffer = CSVStringFormatBuffer(table, schema_, *entry_schema_);
  for (size_t i = 0; i < table->row_count(); ++i) {
    for (size_t j = 0; j < table->arrays_size(); ++j) {
      csvBuffer.addValue(i, j);
    }
    if (i % batchSize == batchSize - 1) {
      auto status = csvBuffer.flush(stream);
      if (!status.ok()) {
        return Status(StatusCode::ERR_IO_ERROR,
                      "Failed to flush CSV buffer: " + status.ToString());
      }
    }
  }

  auto status = csvBuffer.flush(stream);
  if (!status.ok()) {
    return Status(StatusCode::ERR_IO_ERROR,
                  "Failed to flush CSV buffer: " + status.ToString());
  }
  return Status::OK();
}

}  // namespace writer
}  // namespace neug
