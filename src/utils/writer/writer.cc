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
#include "neug/execution/common/operators/retrieve/sink.h"
#include "neug/generated/proto/response/response.pb.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/types.h"
#include "neug/utils/reader/options.h"

#include <climits>
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

#define TYPED_PRIMITIVE_ARRAY_TO_JSON(CASE_ENUM, GETTER_METHOD)       \
  case neug::Array::TypedArrayCase::CASE_ENUM: {                      \
    auto& typed_array = arr.GETTER_METHOD();                          \
    if (!validateProtoValue(typed_array.validity(), rowIdx)) {        \
      return neug::Status(                                            \
          StatusCode::ERR_INVALID_ARGUMENT,                           \
          "Value is invalid, rowIdx=" + std::to_string(rowIdx));      \
    }                                                                 \
    const auto& str = std::to_string(typed_array.values(rowIdx));     \
    write(reinterpret_cast<const uint8_t*>(str.c_str()), str.size()); \
    return neug::Status::OK();                                        \
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
    size_t ncol = static_cast<size_t>(response->arrays_size());
    if (batchSize <= SIZE_MAX / DEFAULT_CAPACITY &&
        ncol <= SIZE_MAX / (DEFAULT_CAPACITY * batchSize)) {
      capacity_ = DEFAULT_CAPACITY * batchSize * ncol;
    } else {
      LOG(WARNING) << "CSV buffer capacity overflow, batchSize=" << batchSize
                   << ", ncol=" << ncol
                   << ", using default capacity: " << capacity_;
    }
  }
  has_header_ = writeOpts.has_header.get(schema.options);
  delimiter_ = writeOpts.delimiter.get(schema.options);
  ignore_errors_ = writeOpts.ignore_errors.get(schema.options);
  escape_char_ = writeOpts.escape_char.get(schema.options);
  quote_char_ = writeOpts.quote_char.get(schema.options);
  blob_.data = std::make_unique<uint8_t[]>(capacity_);
  blob_.size = 0;
  data_ = blob_.data.get();
}

void CSVStringFormatBuffer::addHeader() {
  // Emit header at init so empty result sets still get a header row when
  // HEADER = true.
  if (has_header_ && !entry_schema_.columnNames.empty()) {
    for (size_t col = 0; col < entry_schema_.columnNames.size(); ++col) {
      if (col > 0) {
        write(reinterpret_cast<const uint8_t*>(&delimiter_), sizeof(char));
      }
      const auto& name = entry_schema_.columnNames[col];
      write(reinterpret_cast<const uint8_t*>(name.c_str()), name.size());
    }
    write(reinterpret_cast<const uint8_t*>(DEFAULT_CSV_NEWLINE), sizeof(char));
  }
}

neug::Status CSVStringFormatBuffer::formatValueToStr(const neug::Array& arr,
                                                     int rowIdx) {
  switch (arr.typed_array_case()) {
    TYPED_PRIMITIVE_ARRAY_TO_JSON(kBoolArray, bool_array)
    TYPED_PRIMITIVE_ARRAY_TO_JSON(kInt32Array, int32_array)
    TYPED_PRIMITIVE_ARRAY_TO_JSON(kInt64Array, int64_array)
    TYPED_PRIMITIVE_ARRAY_TO_JSON(kUint32Array, uint32_array)
    TYPED_PRIMITIVE_ARRAY_TO_JSON(kUint64Array, uint64_array)
    TYPED_PRIMITIVE_ARRAY_TO_JSON(kFloatArray, float_array)
    TYPED_PRIMITIVE_ARRAY_TO_JSON(kDoubleArray, double_array)
  case neug::Array::TypedArrayCase::kStringArray: {
    auto& string_array = arr.string_array();
    if (!validateProtoValue(string_array.validity(), rowIdx)) {
      return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "Value is invalid, rowIdx=" + std::to_string(rowIdx));
    }
    const auto& str = string_array.values(rowIdx);
    // add quotes for string type values
    write(reinterpret_cast<const uint8_t*>(&quote_char_), sizeof(char));
    // espace special characters
    char escapeChars[] = {escape_char_, quote_char_};
    writeWithEscapes(escapeChars, escape_char_, str);
    write(reinterpret_cast<const uint8_t*>(&quote_char_), sizeof(char));
    return neug::Status::OK();
  }
  case neug::Array::TypedArrayCase::kDateArray: {
    auto& date32_arr = arr.date_array();
    if (!validateProtoValue(date32_arr.validity(), rowIdx)) {
      return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "Value is invalid, rowIdx=" + std::to_string(rowIdx));
    }
    Date date_value;
    date_value.from_timestamp(date32_arr.values(rowIdx));
    const auto& date_str = date_value.to_string();
    write(reinterpret_cast<const uint8_t*>(date_str.c_str()), date_str.size());
    return neug::Status::OK();
  }
  case neug::Array::TypedArrayCase::kTimestampArray: {
    auto& timestamp_array = arr.timestamp_array();
    if (!validateProtoValue(timestamp_array.validity(), rowIdx)) {
      return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "Value is invalid, rowIdx=" + std::to_string(rowIdx));
    }
    DateTime dt_value(timestamp_array.values(rowIdx));
    const auto& dt_str = dt_value.to_string();
    write(reinterpret_cast<const uint8_t*>(dt_str.c_str()), dt_str.size());
    return neug::Status::OK();
  }
  case neug::Array::TypedArrayCase::kIntervalArray: {
    auto& interval_array = arr.interval_array();
    if (!validateProtoValue(interval_array.validity(), rowIdx)) {
      return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "Value is invalid, rowIdx=" + std::to_string(rowIdx));
    }
    const auto& interval_str = interval_array.values(rowIdx);
    write(reinterpret_cast<const uint8_t*>(interval_str.c_str()),
          interval_str.size());
    return neug::Status::OK();
  }
  case neug::Array::TypedArrayCase::kListArray: {
    auto& list_array = arr.list_array();
    if (!validateProtoValue(list_array.validity(), rowIdx)) {
      return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "Value is invalid, rowIdx=" + std::to_string(rowIdx));
    }
    write(reinterpret_cast<const uint8_t*>(&LIST_ARRAY_CHAR[0]), sizeof(char));
    uint32_t list_size =
        list_array.offsets(rowIdx + 1) - list_array.offsets(rowIdx);
    size_t offset = list_array.offsets(rowIdx);
    for (uint32_t i = 0; i < list_size; ++i) {
      if (i > 0) {
        write(reinterpret_cast<const uint8_t*>(COMMA_CHAR), sizeof(char));
      }
      RETURN_IF_NOT_OK(formatValueToStr(list_array.elements(), offset + i));
    }
    write(reinterpret_cast<const uint8_t*>(&LIST_ARRAY_CHAR[1]), sizeof(char));
    return neug::Status::OK();
  }
  case neug::Array::TypedArrayCase::kStructArray: {
    auto& struct_arr = arr.struct_array();
    if (!validateProtoValue(struct_arr.validity(), rowIdx)) {
      return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "Value is invalid, rowIdx=" + std::to_string(rowIdx));
    }
    write(reinterpret_cast<const uint8_t*>(&LIST_ARRAY_CHAR[0]), sizeof(char));
    for (int i = 0; i < struct_arr.fields_size(); ++i) {
      if (i > 0) {
        write(reinterpret_cast<const uint8_t*>(COMMA_CHAR), sizeof(char));
      }
      const auto& field = struct_arr.fields(i);
      RETURN_IF_NOT_OK(formatValueToStr(field, rowIdx));
    }
    write(reinterpret_cast<const uint8_t*>(&LIST_ARRAY_CHAR[1]), sizeof(char));
    return neug::Status::OK();
  }
  case neug::Array::TypedArrayCase::kVertexArray: {
    auto vertex_array = arr.vertex_array();
    if (!validateProtoValue(vertex_array.validity(), rowIdx)) {
      return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "Value is invalid, rowIdx=" + std::to_string(rowIdx));
    }
    const auto& vertex_str = vertex_array.values(rowIdx);
    write(reinterpret_cast<const uint8_t*>(vertex_str.c_str()),
          vertex_str.size());
    return neug::Status::OK();
  }
  case neug::Array::TypedArrayCase::kEdgeArray: {
    auto edge_array = arr.edge_array();
    if (!validateProtoValue(edge_array.validity(), rowIdx)) {
      return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "Value is invalid, rowIdx=" + std::to_string(rowIdx));
    }
    const auto& edge_str = edge_array.values(rowIdx);
    write(reinterpret_cast<const uint8_t*>(edge_str.c_str()), edge_str.size());
    return neug::Status::OK();
  }
  case neug::Array::TypedArrayCase::kPathArray: {
    auto path_array = arr.path_array();
    if (!validateProtoValue(path_array.validity(), rowIdx)) {
      return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "Value is invalid, rowIdx=" + std::to_string(rowIdx));
    }
    const auto& path_str = path_array.values(rowIdx);
    write(reinterpret_cast<const uint8_t*>(path_str.c_str()), path_str.size());
    return neug::Status::OK();
  }
  default: {
    return neug::Status(
        StatusCode::ERR_NOT_SUPPORTED,
        "Unsupported type: " + std::to_string(arr.typed_array_case()));
  }
  }
}

void CSVStringFormatBuffer::writeWithEscapes(char* toEscape, char escape,
                                             const std::string& val) {
  uint64_t i = 0;
  auto found = val.find_first_of(toEscape, 0, 2);

  while (found != std::string::npos) {
    while (i < found) {
      write(reinterpret_cast<const uint8_t*>(&val[i]), sizeof(char));
      i++;
    }
    write(reinterpret_cast<const uint8_t*>(&escape), sizeof(char));
    found = val.find_first_of(toEscape, found + sizeof(escape), 2);
  }
  while (i < val.length()) {
    write(reinterpret_cast<const uint8_t*>(&val[i]), sizeof(char));
    i++;
  }
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
  if (colIdx > 0) {
    write(reinterpret_cast<const uint8_t*>(&delimiter_), sizeof(char));
  }
  const neug::Array& column = response_->arrays(colIdx);
  auto strResult = formatValueToStr(column, rowIdx);
  if (!strResult.ok()) {
    if (!ignore_errors_) {
      THROW_IO_EXCEPTION(
          "Format value to string failed, rowIdx=" + std::to_string(rowIdx) +
          ", colIdx=" + std::to_string(colIdx) +
          ", error=" + strResult.ToString());
    } else {
      write(reinterpret_cast<const uint8_t*>(DEFAULT_NULL_STR),
            strlen(DEFAULT_NULL_STR));
    }
  }
  if (colIdx == response_->arrays_size() - 1) {
    // the last column, add newline
    write(reinterpret_cast<const uint8_t*>(DEFAULT_CSV_NEWLINE), sizeof(char));
  }
}

neug::Status CSVStringFormatBuffer::flush(
    std::shared_ptr<arrow::io::OutputStream> stream) {
  if (blob_.size > 0) {
    auto status = stream->Write(data_, blob_.size);
    blob_.size = 0;
    if (!status.ok()) {
      return neug::Status(StatusCode::ERR_IO_ERROR, status.ToString());
    }
  }
  return neug::Status::OK();
}

neug::Status QueryExportWriter::write(const execution::Context& context,
                                      const StorageReadInterface& graph) {
  neug::QueryResponse response;
  execution::Sink::sink_results(context, graph, &response);
  return writeTable(&response);
}

neug::Status CsvQueryExportWriter::writeTable(
    const neug::QueryResponse* table) {
  if (schema_.paths.empty()) {
    return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "Schema paths is empty");
  }
  if (!entry_schema_) {
    return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "entry_schema is null");
  }
  auto stream_result = fileSystem_->OpenOutputStream(schema_.paths[0]);
  if (!stream_result.ok()) {
    return neug::Status(
        StatusCode::ERR_IO_ERROR,
        "Failed to open file stream: " + stream_result.status().ToString());
  }
  auto stream = stream_result.ValueOrDie();

  WriteOptions writeOpts;
  auto batchSize = writeOpts.batch_rows.get(schema_.options);
  if (batchSize <= 0) {
    return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "Batch size should be positive");
  }
  auto csvBuffer = CSVStringFormatBuffer(table, schema_, *entry_schema_);
  csvBuffer.addHeader();
  for (size_t i = 0; i < table->row_count(); ++i) {
    for (size_t j = 0; j < table->arrays_size(); ++j) {
      csvBuffer.addValue(i, j);
    }
    if (i % batchSize == batchSize - 1) {
      auto status = csvBuffer.flush(stream);
      if (!status.ok()) {
        (void) stream->Close();
        return neug::Status(StatusCode::ERR_IO_ERROR,
                            "Failed to flush CSV buffer: " + status.ToString());
      }
    }
  }

  auto status = csvBuffer.flush(stream);
  if (!status.ok()) {
    (void) stream->Close();
    return neug::Status(StatusCode::ERR_IO_ERROR,
                        "Failed to flush CSV buffer: " + status.ToString());
  }
  auto close_status = stream->Close();
  if (!close_status.ok()) {
    return neug::Status(
        StatusCode::ERR_IO_ERROR,
        "Failed to close output stream: " + close_status.ToString());
  }
  return neug::Status::OK();
}

}  // namespace writer
}  // namespace neug
