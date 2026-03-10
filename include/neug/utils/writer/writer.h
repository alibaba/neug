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

#include <arrow/chunked_array.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/table.h>
#include <rapidjson/document.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/execute/ops/batch/batch_update_utils.h"
#include "neug/generated/proto/plan/type.pb.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/reader/options.h"
#include "neug/utils/reader/schema.h"
#include "neug/utils/result.h"

namespace neug {

namespace writer {

struct WriteOptions {
  // maximum number of rows to write in a single batch
  reader::Option<int64_t> batch_rows =
      reader::Option<int64_t>::Int64Option("batch_size", 1024);
};

class ExportWriter {
 public:
  ExportWriter(const reader::FileSchema& schema,
               std::shared_ptr<reader::EntrySchema> entry_schema = nullptr)
      : schema_(schema), entry_schema_(std::move(entry_schema)) {}

  virtual ~ExportWriter() = default;

  virtual Status write(const execution::Context& context,
                       const StorageReadInterface& graph) = 0;

 protected:
  const reader::FileSchema& schema_;
  std::shared_ptr<reader::EntrySchema> entry_schema_;
};

class ArrowExportWriter : public ExportWriter {
 public:
  ArrowExportWriter(const reader::FileSchema& schema,
                    std::shared_ptr<arrow::fs::FileSystem> fileSystem,
                    std::shared_ptr<reader::EntrySchema> entry_schema = nullptr)
      : ExportWriter(schema, std::move(entry_schema)),
        fileSystem_(fileSystem) {}
  ~ArrowExportWriter() {}

  virtual Status write(const execution::Context& context,
                       const StorageReadInterface& graph) override;

  virtual Status writeTable(const std::shared_ptr<arrow::Table>& table) = 0;

 protected:
  std::shared_ptr<arrow::fs::FileSystem> fileSystem_;
};

class ArrowCsvExportWriter : public ArrowExportWriter {
 public:
  ArrowCsvExportWriter(
      const reader::FileSchema& schema,
      std::shared_ptr<arrow::fs::FileSystem> fileSystem,
      std::shared_ptr<reader::EntrySchema> entry_schema = nullptr)
      : ArrowExportWriter(schema, fileSystem, std::move(entry_schema)) {}
  ~ArrowCsvExportWriter() {}

  virtual Status writeTable(
      const std::shared_ptr<arrow::Table>& table) override;
};

}  // namespace writer
}  // namespace neug
