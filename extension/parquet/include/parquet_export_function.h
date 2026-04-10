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

#pragma once

#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>
#include <memory>

#include "neug/compiler/function/export/export_function.h"
#include "neug/utils/writer/writer.h"

namespace neug {
namespace writer {

class ArrowParquetExportWriter : public QueryExportWriter {
 public:
  ArrowParquetExportWriter(
      const reader::FileSchema& schema,
      std::shared_ptr<arrow::fs::FileSystem> fileSystem,
      std::shared_ptr<reader::EntrySchema> entry_schema = nullptr)
      : QueryExportWriter(schema, fileSystem, std::move(entry_schema)) {}
  ~ArrowParquetExportWriter() override = default;

  neug::Status writeTable(const QueryResponse* table) override;
};

}  // namespace writer

namespace function {

struct ExportParquetFunction : public ExportFunction {
  static constexpr const char* name = "COPY_PARQUET";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace neug
