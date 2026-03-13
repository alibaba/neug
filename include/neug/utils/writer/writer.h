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

#include "neug/execution/execute/ops/batch/batch_update_utils.h"
#include "neug/generated/proto/response/response.pb.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/reader/options.h"
#include "neug/utils/reader/schema.h"

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

  virtual neug::Status write(const execution::Context& context,
                             const StorageReadInterface& graph) = 0;

 protected:
  const reader::FileSchema& schema_;
  std::shared_ptr<reader::EntrySchema> entry_schema_;
};

class StringFormatBuffer {
 public:
  StringFormatBuffer(const neug::QueryResponse* response,
                     const reader::FileSchema& schema)
      : response_(response), schema_(schema) {}
  ~StringFormatBuffer() {}
  virtual void addValue(int rowIdx, int colIdx) = 0;
  virtual neug::Status flush(
      std::shared_ptr<arrow::io::OutputStream> stream) = 0;

 protected:
  const neug::QueryResponse* response_;
  const reader::FileSchema& schema_;

 protected:
  bool validateIndex(const neug::QueryResponse* response, int rowIdx,
                     int colIdx);
  bool validateProtoValue(const std::string& validity, int rowIdx);
};

struct BinaryData {
  std::unique_ptr<uint8_t[]> data;
  uint64_t size = 0;
};

class CSVStringFormatBuffer : public StringFormatBuffer {
 public:
  CSVStringFormatBuffer(const neug::QueryResponse* response,
                        const reader::FileSchema& schema,
                        const reader::EntrySchema& entry_schema);
  ~CSVStringFormatBuffer() {}
  void addValue(int rowIdx, int colIdx) override;
  void addHeader();
  neug::Status flush(std::shared_ptr<arrow::io::OutputStream> stream) override;

 private:
  BinaryData blob_;
  size_t capacity_;
  uint8_t* data_;
  const reader::EntrySchema& entry_schema_;
  // Cached WriteOptions resolved once at construction to avoid per-cell lookup.
  bool has_header_;
  char delimiter_;
  bool ignore_errors_;
  char escape_char_;
  char quote_char_;

 private:
  // write the current value to string buffer, return error status if value is
  // invalid
  neug::Status formatValueToStr(const neug::Array& arr, int rowIdx);
  void writeWithEscapes(char* toEscape, char escape, const std::string& str);
  void write(const uint8_t* buffer, uint64_t len);

 private:
  static constexpr const char* DEFAULT_CSV_NEWLINE = "\n";
  static constexpr const char* DEFAULT_NULL_STR = "";
  static constexpr size_t DEFAULT_CAPACITY = 64;
  static constexpr const char* LIST_ARRAY_CHAR = "[]";
  static constexpr const char* COMMA_CHAR = ",";
};

class QueryExportWriter : public ExportWriter {
 public:
  QueryExportWriter(
      const reader::FileSchema& schema,
      std::shared_ptr<arrow::fs::FileSystem>
          fileSystem,  // use arrow file system to open output stream
      std::shared_ptr<reader::EntrySchema> entry_schema = nullptr)
      : ExportWriter(schema, std::move(entry_schema)),
        fileSystem_(fileSystem) {}
  ~QueryExportWriter() {}

  virtual neug::Status write(const execution::Context& context,
                             const StorageReadInterface& graph) override;

  virtual neug::Status writeTable(const QueryResponse* table) = 0;

 protected:
  std::shared_ptr<arrow::fs::FileSystem> fileSystem_;
};

class CsvQueryExportWriter : public QueryExportWriter {
 public:
  CsvQueryExportWriter(
      const reader::FileSchema& schema,
      std::shared_ptr<arrow::fs::FileSystem> fileSystem,
      std::shared_ptr<reader::EntrySchema> entry_schema = nullptr)
      : QueryExportWriter(schema, fileSystem, std::move(entry_schema)) {}
  ~CsvQueryExportWriter() {}

  virtual neug::Status writeTable(const QueryResponse* table) override;
};

}  // namespace writer
}  // namespace neug
