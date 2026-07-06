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

#include "neug/utils/io/write/writer.h"
#include "neug/common/types/property_types.h"
#include "neug/common/types/value.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/stream/output_stream.h"

#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <string>

namespace neug {
namespace writer {

DataChunkCSVStringFormatBuffer::DataChunkCSVStringFormatBuffer(
    const DataChunk& chunk, const reader::FileSchema& schema,
    const reader::EntrySchema& entry_schema)
    : chunk_(chunk),
      schema_(schema),
      entry_schema_(entry_schema),
      capacity_(DEFAULT_CAPACITY) {
  WriteOptions write_opts;
  size_t batch_size = write_opts.batch_rows.get(schema.options);
  if (batch_size > 0 && chunk.col_num() > 0 && chunk.row_num() > 0) {
    size_t ncol = chunk.col_num();
    if (batch_size <= SIZE_MAX / DEFAULT_CAPACITY &&
        ncol <= SIZE_MAX / (DEFAULT_CAPACITY * batch_size)) {
      capacity_ = DEFAULT_CAPACITY * batch_size * ncol;
    }
  }
  has_header_ = write_opts.has_header.get(schema.options);
  delimiter_ = write_opts.delimiter.get(schema.options);
  ignore_errors_ = write_opts.ignore_errors.get(schema.options);
  escape_char_ = write_opts.escape_char.get(schema.options);
  quote_char_ = write_opts.quote_char.get(schema.options);
  blob_.data = std::make_unique<uint8_t[]>(capacity_);
  blob_.size = 0;
  data_ = blob_.data.get();
}

void DataChunkCSVStringFormatBuffer::addHeader() {
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

void DataChunkCSVStringFormatBuffer::writeWithEscapes(char* to_escape,
                                                      char escape,
                                                      const std::string& val) {
  uint64_t i = 0;
  auto found = val.find_first_of(to_escape, 0, 2);
  while (found != std::string::npos) {
    while (i < found) {
      write(reinterpret_cast<const uint8_t*>(&val[i]), sizeof(char));
      ++i;
    }
    write(reinterpret_cast<const uint8_t*>(&escape), sizeof(char));
    found = val.find_first_of(to_escape, found + sizeof(escape), 2);
  }
  while (i < val.length()) {
    write(reinterpret_cast<const uint8_t*>(&val[i]), sizeof(char));
    ++i;
  }
}

void DataChunkCSVStringFormatBuffer::write(const uint8_t* buffer,
                                           uint64_t len) {
  if (len == 0) {
    return;
  }
  if (buffer == nullptr) {
    THROW_IO_EXCEPTION(
        "DataChunkCSVStringFormatBuffer::write called with null buffer");
  }
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

neug::Status DataChunkCSVStringFormatBuffer::formatValueToStr(
    const Value& value, size_t row_idx) {
  if (value.IsNull()) {
    return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "Value is invalid, rowIdx=" + std::to_string(row_idx));
  }
  const auto type_id = value.type().id();
  if (type_id == DataTypeId::kVarchar) {
    const auto& str = StringValue::Get(value);
    write(reinterpret_cast<const uint8_t*>(&quote_char_), sizeof(char));
    char escape_chars[] = {escape_char_, quote_char_};
    writeWithEscapes(escape_chars, escape_char_, str);
    write(reinterpret_cast<const uint8_t*>(&quote_char_), sizeof(char));
    return neug::Status::OK();
  }
  const auto& str = value.to_string();
  write(reinterpret_cast<const uint8_t*>(str.c_str()), str.size());
  return neug::Status::OK();
}

void DataChunkCSVStringFormatBuffer::addValue(size_t row_idx, size_t col_idx) {
  if (col_idx >= chunk_.col_num() || chunk_.columns[col_idx] == nullptr) {
    THROW_IO_EXCEPTION("Column index out of range: colIdx=" +
                       std::to_string(col_idx));
  }
  const auto& column = chunk_.columns[col_idx];
  if (row_idx >= column->size()) {
    THROW_IO_EXCEPTION("Row index out of range: rowIdx=" +
                       std::to_string(row_idx));
  }
  if (col_idx > 0) {
    write(reinterpret_cast<const uint8_t*>(&delimiter_), sizeof(char));
  }
  if (!column->has_value(row_idx)) {
    if (!ignore_errors_) {
      THROW_IO_EXCEPTION("Value is invalid, rowIdx=" + std::to_string(row_idx) +
                         ", colIdx=" + std::to_string(col_idx));
    }
    write(reinterpret_cast<const uint8_t*>(DEFAULT_NULL_STR),
          strlen(DEFAULT_NULL_STR));
  } else {
    auto str_result = formatValueToStr(column->get_elem(row_idx), row_idx);
    if (!str_result.ok()) {
      if (!ignore_errors_) {
        THROW_IO_EXCEPTION(
            "Format value to string failed, rowIdx=" + std::to_string(row_idx) +
            ", colIdx=" + std::to_string(col_idx) +
            ", error=" + str_result.ToString());
      }
      write(reinterpret_cast<const uint8_t*>(DEFAULT_NULL_STR),
            strlen(DEFAULT_NULL_STR));
    }
  }
  if (col_idx + 1 == chunk_.col_num()) {
    write(reinterpret_cast<const uint8_t*>(DEFAULT_CSV_NEWLINE), sizeof(char));
  }
}

neug::Status DataChunkCSVStringFormatBuffer::flush(io::OutputStream& stream) {
  if (blob_.size > 0) {
    auto status = stream.Write(data_, static_cast<int64_t>(blob_.size));
    blob_.size = 0;
    if (!status.ok()) {
      return status;
    }
  }
  return neug::Status::OK();
}

neug::Status CsvQueryExportWriter::write(
    const DataChunk& chunk, const std::vector<DataType>& /*source_types*/) {
  if (schema_.paths.empty()) {
    return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "Schema paths is empty");
  }
  if (!entry_schema_) {
    return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "entry_schema is null");
  }
  auto stream = io::openLocalOutputStream(schema_.paths[0]);
  if (!stream) {
    return neug::Status(StatusCode::ERR_IO_ERROR, "Failed to open output file");
  }

  WriteOptions write_opts;
  auto batch_size = write_opts.batch_rows.get(schema_.options);
  if (batch_size <= 0) {
    return neug::Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "Batch size should be positive");
  }
  auto csv_buffer =
      DataChunkCSVStringFormatBuffer(chunk, schema_, *entry_schema_);
  csv_buffer.addHeader();
  for (size_t i = 0; i < chunk.row_num(); ++i) {
    for (size_t j = 0; j < chunk.col_num(); ++j) {
      csv_buffer.addValue(i, j);
    }
    if (i % batch_size == batch_size - 1) {
      auto status = csv_buffer.flush(*stream);
      if (!status.ok()) {
        (void) stream->Close();
        return neug::Status(StatusCode::ERR_IO_ERROR,
                            "Failed to flush CSV buffer: " + status.ToString());
      }
    }
  }

  auto status = csv_buffer.flush(*stream);
  if (!status.ok()) {
    (void) stream->Close();
    return neug::Status(StatusCode::ERR_IO_ERROR,
                        "Failed to flush CSV buffer: " + status.ToString());
  }
  return stream->Close();
}

}  // namespace writer
}  // namespace neug
