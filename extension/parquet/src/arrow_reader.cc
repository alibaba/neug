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

#include <arrow/dataset/discovery.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <glog/logging.h>

#include "parquet/arrow_column.h"
#include "parquet/arrow_reader.h"
#include "parquet/arrow_type_converter.h"
#include "parquet/record_batch_supplier.h"

#include "neug/compiler/common/assert.h"
#include "neug/execution/common/context.h"
#include "neug/execution/io/chunk_supplier.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/result.h"

namespace neug {
namespace reader {

std::shared_ptr<IDataChunkSupplier> ArrowReader::read() {
  if (!sharedState_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("SharedState is null");
  }

  if (!fileSystem_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("FileSystem is null");
  }

  auto scanner = createScanner(fileSystem_);
  NEUG_ASSERT(scanner != nullptr);

  // Choose read mode: batch_read streams data, full_read loads entire dataset
  const auto& fileSchema = sharedState_->schema.file;
  ReadOptions options;
  if (options.batch_read.get(fileSchema.options)) {
    return batch_read(scanner);
  } else {
    return full_read(scanner);
  }
}

std::shared_ptr<arrow::dataset::Scanner> ArrowReader::createScanner(
    std::shared_ptr<arrow::fs::FileSystem> fs) {
  if (!fs) {
    THROW_INVALID_ARGUMENT_EXCEPTION("FileSystem is null");
  }

  if (!sharedState_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("SharedState is null");
  }

  const auto& fileSchema = sharedState_->schema.file;
  const std::vector<std::string>& file_paths = fileSchema.paths;

  if (file_paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("No file paths provided");
  }

  if (!optionsBuilder_) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Options builder is null");
  }

  auto arrowOptions = optionsBuilder_->build();
  if (!arrowOptions.scanOptions) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Failed to build arrow options");
  }

  if (!optionsBuilder_->projectColumns(arrowOptions)) {
    LOG(WARNING) << "Failed to set column projection, using all columns";
  }

  if (!optionsBuilder_->skipRows(arrowOptions)) {
    LOG(WARNING) << "Failed to set row filter, using no filter";
  }

  auto scan_opts = arrowOptions.scanOptions;
  auto fileFormat = arrowOptions.fileFormat;
  if (!fileFormat) {
    LOG(ERROR) << "File format is null in arrow options";
    THROW_INVALID_ARGUMENT_EXCEPTION("File format is null in arrow options");
  }

  auto factory = datasetBuilder_->buildFactory(sharedState_, fs, fileFormat);

  arrow::Result<std::shared_ptr<arrow::dataset::Dataset>> dataset_result;
  if (scan_opts->dataset_schema) {
    auto inspected = factory->Inspect();
    if (inspected.ok()) {
      auto fileSchema = inspected.ValueOrDie();
      for (const auto& field : scan_opts->dataset_schema->fields()) {
        if (!fileSchema->GetFieldByName(field->name())) {
          THROW_SCHEMA_MISMATCH("Column '" + field->name() +
                                "' not found in file. Available columns: " +
                                fileSchema->ToString());
        }
      }
    }
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

std::shared_ptr<IDataChunkSupplier> ArrowReader::full_read(
    std::shared_ptr<arrow::dataset::Scanner> scanner) {
  if (!sharedState_) {
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

  int num_cols = sharedState_->columnNum();
  if (num_cols != table->num_columns()) {
    THROW_IO_EXCEPTION(
        "Column number mismatch between schema and table, schema: " +
        std::to_string(num_cols) +
        ", table: " + std::to_string(table->num_columns()));
  }

  auto chunk = std::make_shared<columnar::DataChunk>();
  for (int i = 0; i < num_cols; ++i) {
    auto table_column = table->column(i);
    chunk->set(i,
               columnar::arrow_arrays_to_value_column(table_column->chunks()));
  }
  return std::make_shared<MultiDataChunkSupplier>(
      std::vector<std::shared_ptr<columnar::DataChunk>>{chunk});
}

std::shared_ptr<IDataChunkSupplier> ArrowReader::batch_read(
    std::shared_ptr<arrow::dataset::Scanner> scanner) {
  if (!sharedState_) {
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

  return std::make_shared<RecordBatchChunkSupplier>(batch_reader, row_num);
}

result<std::shared_ptr<EntrySchema>> ArrowReader::inferSchema() {
  return convertArrowSchemaToEntrySchema(nullptr);
}

result<std::shared_ptr<EntrySchema>>
ArrowReader::convertArrowSchemaToEntrySchema(
    const std::shared_ptr<arrow::Schema>& providedSchema) {
  if (!sharedState_) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "SharedState is null");
  }

  if (!fileSystem_) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "FileSystem is null");
  }

  if (!optionsBuilder_) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "Options builder is null");
  }

  std::shared_ptr<arrow::Schema> arrowSchema = providedSchema;
  if (!arrowSchema) {
    auto arrowOptions = optionsBuilder_->build();
    if (!arrowOptions.fileFormat) {
      RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                          "Failed to build file format from options builder");
    }
    auto fileFormat = arrowOptions.fileFormat;
    auto factory =
        datasetBuilder_->buildFactory(sharedState_, fileSystem_, fileFormat);
    auto inspectResult = factory->Inspect();
    if (!inspectResult.ok()) {
      RETURN_STATUS_ERROR(
          neug::StatusCode::ERR_IO_ERROR,
          "Failed to inspect schema: " + inspectResult.status().message());
    }
    arrowSchema = inspectResult.ValueOrDie();
  }

  ArrowTypeConverter converter;
  auto entrySchema = std::make_shared<TableEntrySchema>();
  for (const auto& field : arrowSchema->fields()) {
    auto dataType = converter.convert(*field->type());
    if (!dataType) {
      RETURN_STATUS_ERROR(
          neug::StatusCode::ERR_IO_ERROR,
          "Unsupported arrow type: " + field->type()->ToString());
    }
    entrySchema->columnNames.push_back(field->name());
    entrySchema->columnTypes.push_back(dataType);
  }
  return entrySchema;
}

}  // namespace reader
}  // namespace neug
