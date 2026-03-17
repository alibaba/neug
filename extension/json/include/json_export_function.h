/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

#include <vector>

#include "neug/compiler/function/export/export_function.h"
#include "neug/utils/result.h"
#include "neug/utils/writer/writer.h"
#include "rapidjson/document.h"

namespace neug {
namespace writer {

static constexpr const char* DEFAULT_JSON_NEWLINE = "\n";
class JsonArrayStringFormatBuffer : public StringFormatBuffer {
 public:
  JsonArrayStringFormatBuffer(const neug::QueryResponse* response,
                              const reader::FileSchema& schema,
                              const reader::EntrySchema& entry_schema);
  ~JsonArrayStringFormatBuffer() = default;
  void addValue(int rowIdx, int colIdx) override;
  neug::Status flush(std::shared_ptr<arrow::io::OutputStream> stream) override;

 private:
  const reader::EntrySchema& entry_schema_;
  rapidjson::Value current_line_;
  rapidjson::Value buffer_;
  // get allocator from document
  rapidjson::Document document_;
};

class JsonLStringFormatBuffer : public StringFormatBuffer {
 public:
  JsonLStringFormatBuffer(const neug::QueryResponse* response,
                          const reader::FileSchema& schema,
                          const reader::EntrySchema& entry_schema);
  ~JsonLStringFormatBuffer() = default;
  void addValue(int rowIdx, int colIdx) override;
  neug::Status flush(std::shared_ptr<arrow::io::OutputStream> stream) override;

 private:
  const reader::EntrySchema& entry_schema_;
  rapidjson::Value current_line_;
  std::vector<rapidjson::Value> buffer_;
  // get allocator from document
  rapidjson::Document document_;
};

class ArrowJsonArrayExportWriter : public QueryExportWriter {
 public:
  ArrowJsonArrayExportWriter(
      const reader::FileSchema& schema,
      std::shared_ptr<arrow::fs::FileSystem> fileSystem,
      std::shared_ptr<reader::EntrySchema> entry_schema = nullptr)
      : QueryExportWriter(schema, fileSystem, std::move(entry_schema)) {}
  ~ArrowJsonArrayExportWriter() override = default;

  neug::Status writeTable(const QueryResponse* table) override;
};

class ArrowJsonLExportWriter : public QueryExportWriter {
 public:
  ArrowJsonLExportWriter(
      const reader::FileSchema& schema,
      std::shared_ptr<arrow::fs::FileSystem> fileSystem,
      std::shared_ptr<reader::EntrySchema> entry_schema = nullptr)
      : QueryExportWriter(schema, fileSystem, std::move(entry_schema)) {}
  ~ArrowJsonLExportWriter() override = default;

  neug::Status writeTable(const QueryResponse* table) override;
};
}  // namespace writer

namespace function {
struct ExportJsonFunction : public ExportFunction {
  static constexpr const char* name = "COPY_JSON";

  static function_set getFunctionSet();
};

struct ExportJsonLFunction : public ExportFunction {
  static constexpr const char* name = "COPY_JSONL";

  static function_set getFunctionSet();
};
}  // namespace function
}  // namespace neug
