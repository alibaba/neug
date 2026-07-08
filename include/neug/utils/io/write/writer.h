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
#pragma once

#include <memory>
#include <string>
#include <utility>

#include "neug/common/types.h"
#include "neug/common/types/data_chunk.h"
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/stream/output_stream.h"

namespace neug {

namespace writer {

struct WriteOptions {
  // maximum number of rows to write in a single batch
  reader::Option<int64_t> batch_rows =
      reader::Option<int64_t>::Int64Option("batch_size", 1024);
  reader::Option<char> delimiter =
      reader::Option<char>::CharOption("delim", '|');
  reader::Option<bool> has_header =
      reader::Option<bool>::BoolOption("header", true);
  reader::Option<char> quote_char =
      reader::Option<char>::CharOption("quote", '"');
  reader::Option<char> escape_char =
      reader::Option<char>::CharOption("escape", '\\');
  reader::Option<bool> ignore_errors =
      reader::Option<bool>::BoolOption("ignore_errors", true);
};

class ExportWriter {
 public:
  ExportWriter(const reader::FileSchema& schema,
               std::shared_ptr<reader::EntrySchema> entry_schema = nullptr)
      : schema_(schema), entry_schema_(std::move(entry_schema)) {}

  virtual ~ExportWriter() = default;

  virtual neug::Status write(
      const DataChunk& chunk,
      const std::vector<DataType>& source_types = {}) = 0;

 protected:
  const reader::FileSchema& schema_;
  std::shared_ptr<reader::EntrySchema> entry_schema_;
};

struct BinaryData {
  std::unique_ptr<uint8_t[]> data;
  uint64_t size = 0;
};

class DataChunkCSVStringFormatBuffer {
 public:
  DataChunkCSVStringFormatBuffer(const DataChunk& chunk,
                                 const reader::FileSchema& schema,
                                 const reader::EntrySchema& entry_schema,
                                 const std::vector<DataType>& source_types);
  ~DataChunkCSVStringFormatBuffer() = default;

  void addValue(size_t row_idx, size_t col_idx);
  void addHeader();
  neug::Status flush(io::OutputStream& stream);

 private:
  neug::Status formatValueToStr(const Value& value, size_t row_idx,
                                size_t col_idx);
  bool shouldWriteRawString(size_t col_idx) const;
  void writeWithEscapes(char* toEscape, char escape, const std::string& str);
  void write(const uint8_t* buffer, uint64_t len);

  const DataChunk& chunk_;
  const reader::FileSchema& schema_;
  const reader::EntrySchema& entry_schema_;
  const std::vector<DataType>& source_types_;
  BinaryData blob_;
  size_t capacity_;
  uint8_t* data_;
  bool has_header_;
  char delimiter_;
  bool ignore_errors_;
  char escape_char_;
  char quote_char_;

  static constexpr const char* DEFAULT_CSV_NEWLINE = "\n";
  static constexpr const char* DEFAULT_NULL_STR = "";
  static constexpr size_t DEFAULT_CAPACITY = 64;
};

class QueryExportWriter : public ExportWriter {
 public:
  explicit QueryExportWriter(
      const reader::FileSchema& schema,
      std::shared_ptr<reader::EntrySchema> entry_schema = nullptr)
      : ExportWriter(schema, std::move(entry_schema)) {}
  ~QueryExportWriter() override = default;
};

class CsvQueryExportWriter : public QueryExportWriter {
 public:
  explicit CsvQueryExportWriter(
      const reader::FileSchema& schema,
      std::shared_ptr<reader::EntrySchema> entry_schema = nullptr)
      : QueryExportWriter(schema, std::move(entry_schema)) {}
  ~CsvQueryExportWriter() override = default;

  neug::Status write(const DataChunk& chunk,
                     const std::vector<DataType>& source_types = {}) override;
};

}  // namespace writer
}  // namespace neug
