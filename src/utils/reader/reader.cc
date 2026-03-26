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

#include <arrow/result.h>
#include <arrow/status.h>
#include <glog/logging.h>

#include "neug/utils/reader/reader.h"

#include <memory>
#include <string>
#include <vector>

#include <arrow/array/array_base.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/discovery.h>
#include <arrow/dataset/file_csv.h>
#include <arrow/dataset/scanner.h>
#include <arrow/filesystem/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/filesystem/type_fwd.h>
#include <arrow/table.h>
#include <arrow/type.h>

#include "neug/compiler/common/assert.h"
#include "neug/execution/common/columns/arrow_context_column.h"
#include "neug/execution/common/context.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/reader/options.h"

namespace neug {
namespace reader {

void ArrowReader::read(std::shared_ptr<ReadLocalState> localState,
                       execution::Context& ctx) {
  if (!sharedState) {
    THROW_INVALID_ARGUMENT_EXCEPTION("SharedState is null");
  }

  if (!fileSystem) {
    THROW_INVALID_ARGUMENT_EXCEPTION("FileSystem is null");
  }

  auto scanner = createScanner(fileSystem);
  NEUG_ASSERT(scanner != nullptr);

  // Choose read mode: batch_read streams data, full_read loads entire dataset
  const auto& fileSchema = sharedState->schema.file;
  ReadOptions options;
  if (options.batch_read.get(fileSchema.options)) {
    batch_read(scanner, ctx);
  } else {
    full_read(scanner, ctx);
  }
}

std::shared_ptr<arrow::dataset::Scanner> ArrowReader::createScanner(
    std::shared_ptr<arrow::fs::FileSystem> fs) {
  if (!fs) {
    THROW_INVALID_ARGUMENT_EXCEPTION("FileSystem is null");
  }

  if (!sharedState) {
    THROW_INVALID_ARGUMENT_EXCEPTION("SharedState is null");
  }

  const auto& fileSchema = sharedState->schema.file;
  const std::vector<std::string>& file_paths = fileSchema.paths;

  if (file_paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("No file paths provided");
  }

  if (!optionsBuilder) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Options builder is null");
  }

  auto arrowOptions = optionsBuilder->build();
  if (!arrowOptions.scanOptions) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Failed to build arrow options");
  }

  if (!optionsBuilder->skipColumns(arrowOptions)) {
    LOG(WARNING) << "Failed to set column projection, using all columns";
  }

  if (!optionsBuilder->skipRows(arrowOptions)) {
    LOG(WARNING) << "Failed to set row filter, using no filter";
  }

  auto scan_opts = arrowOptions.scanOptions;
  auto fileFormat = arrowOptions.fileFormat;
  if (!fileFormat) {
    LOG(ERROR) << "File format is null in arrow options";
    THROW_INVALID_ARGUMENT_EXCEPTION("File format is null in arrow options");
  }

  auto factory = datasetBuilder->buildFactory(sharedState, fs, fileFormat);

  arrow::Result<std::shared_ptr<arrow::dataset::Dataset>> dataset_result;
  if (scan_opts->dataset_schema) {
    dataset_result = factory->Finish(scan_opts->dataset_schema);
  } else {
    arrow::dataset::FinishOptions finish_options;
    finish_options.validate_fragments = false;
    dataset_result = factory->Finish(finish_options);
  }
  if (!dataset_result.ok()) {
    LOG(ERROR) << "Failed to create dataset from factory: "
               << dataset_result.status().message();
    THROW_IO_EXCEPTION("Failed to create dataset from factory: " +
                       dataset_result.status().message());
  }
  auto dataset = dataset_result.ValueOrDie();

  arrow::dataset::ScannerBuilder scanner_builder(dataset, scan_opts);
  auto scanner_result = scanner_builder.Finish();
  if (!scanner_result.ok()) {
    LOG(ERROR) << "Failed to create scanner: "
               << scanner_result.status().message();
    THROW_IO_EXCEPTION("Failed to create scanner: " +
                       scanner_result.status().message());
  }
  return scanner_result.ValueOrDie();
}

void ArrowReader::full_read(std::shared_ptr<arrow::dataset::Scanner> scanner,
                            execution::Context& output) {
  if (!sharedState) {
    THROW_INVALID_ARGUMENT_EXCEPTION("SharedState is null");
  }
  if (!scanner) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Scanner is null");
  }

  auto table_result = scanner->ToTable();
  if (!table_result.ok()) {
    LOG(ERROR) << "Failed to read table via scanner: "
               << table_result.status().message();
    THROW_IO_EXCEPTION("Failed to read table via scanner: " +
                       table_result.status().message());
  }
  auto table = table_result.ValueOrDie();

  int num_cols = sharedState->columnNum();
  if (num_cols != table->num_columns()) {
    THROW_IO_EXCEPTION(
        "Column number mismatch between schema and table, schema: " +
        std::to_string(num_cols) +
        ", table: " + std::to_string(table->num_columns()));
  }

  output.clear();
  for (int i = 0; i < num_cols; ++i) {
    auto chunk_arrays = table->column(i)->chunks();
    execution::ArrowArrayContextColumnBuilder builder;
    for (const auto& array : chunk_arrays) {
      builder.push_back(array);
    }
    output.set(i, builder.finish());
  }
}

void ArrowReader::batch_read(std::shared_ptr<arrow::dataset::Scanner> scanner,
                             execution::Context& output) {
  if (!sharedState) {
    THROW_INVALID_ARGUMENT_EXCEPTION("SharedState is null");
  }
  if (!scanner) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Scanner is null");
  }
  auto row_num_result = scanner->CountRows();
  int64_t row_num = 0;
  if (!row_num_result.ok()) {
    LOG(WARNING) << "Failed to count rows via scanner: "
                 << row_num_result.status().message();
    THROW_IO_EXCEPTION("Failed to count rows via scanner: " +
                       row_num_result.status().message());
  } else {
    VLOG(10) << "Row count from scanner: " << row_num_result.ValueOrDie();
    row_num = row_num_result.ValueOrDie();
  }

  auto batch_reader_result = scanner->ToRecordBatchReader();
  if (!batch_reader_result.ok()) {
    LOG(ERROR) << "Failed to create RecordBatchReader from scanner: "
               << batch_reader_result.status().message();
    THROW_IO_EXCEPTION("Failed to create RecordBatchReader from scanner: " +
                       batch_reader_result.status().message());
  }
  auto batch_reader = batch_reader_result.ValueOrDie();

  auto batch_supplier = std::make_shared<neug::ArrowRecordBatchStreamSupplier>(
      batch_reader, row_num);

  int num_cols = sharedState->columnNum();
  output.clear();
  for (int i = 0; i < num_cols; ++i) {
    // NOTE: Each column uses the same shared RecordBatch supplier, so columns
    // share access to entire batches rather than being split by column. This
    // design may need refactoring when storage no longer relies on Arrow.
    execution::ArrowStreamContextColumnBuilder builder({batch_supplier});
    output.set(i, builder.finish());
  }
}

arrow::Result<std::shared_ptr<arrow::Schema>> ArrowReader::inferSchema() {
  if (!sharedState) {
    return arrow::Status::Invalid(neug::StatusCode::ERR_INVALID_ARGUMENT,
                                  "SharedState is null");
  }

  if (!fileSystem) {
    return arrow::Status::Invalid(neug::StatusCode::ERR_INVALID_ARGUMENT,
                                  "FileSystem is null");
  }

  if (!optionsBuilder) {
    return arrow::Status::Invalid(neug::StatusCode::ERR_INVALID_ARGUMENT,
                                  "Options builder is null");
  }

  // Reuse optionsBuilder->build() to get fileFormat
  // For schema inference, we need fileFormat but don't need entry schema.
  // build() will create an empty dataset_schema if entry schema is empty,
  // but fileFormat will still be correctly built.
  auto arrowOptions = optionsBuilder->build();
  if (!arrowOptions.fileFormat) {
    return arrow::Status::IOError(
        "Failed to build file format from options builder");
  }
  auto fileFormat = arrowOptions.fileFormat;

  auto factory =
      datasetBuilder->buildFactory(sharedState, fileSystem, fileFormat);

  // Infer schema using Inspect()
  return factory->Inspect();
}

}  // namespace reader
}  // namespace neug
