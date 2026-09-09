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

#include <arrow/compute/expression.h>
// NOTE: internal Arrow API (provides FragmentDataset). Recheck this include
// and its usage when bumping the bundled Arrow version.
#include <arrow/dataset/dataset_internal.h>
#include <arrow/dataset/discovery.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <glog/logging.h>

#include <unordered_set>

#include "parquet/arrow_column.h"
#include "parquet/arrow_reader.h"
#include "parquet/record_batch_supplier.h"

#include "neug/compiler/common/assert.h"
#include "neug/execution/common/context.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/read/common/row_expression_filter.h"

namespace neug {
namespace reader {
namespace {

std::vector<std::string> fallbackProjection(const ReadSharedState& state) {
  const auto& all_columns = state.schema.entry->columnNames;
  // An empty projection means that the caller requests every column.
  if (state.projectColumns.empty()) {
    return all_columns;
  }
  std::unordered_set<std::string> required(state.projectColumns.begin(),
                                           state.projectColumns.end());
  std::vector<const ::common::Expression*> pending{state.skipRows.get()};
  while (!pending.empty()) {
    const auto* expr = pending.back();
    pending.pop_back();
    if (!expr) {
      continue;
    }
    for (const auto& opr : expr->operators()) {
      switch (opr.item_case()) {
      case ::common::ExprOpr::kVar:
        if (!opr.var().tag().has_name() || opr.var().has_property()) {
          THROW_INVALID_ARGUMENT_EXCEPTION(
              "File filter requires a column name without a graph property");
        }
        required.insert(opr.var().tag().name());
        break;
      case ::common::ExprOpr::kCase:
        for (const auto& when : opr.case_().when_then_expressions()) {
          pending.push_back(&when.when_expression());
          pending.push_back(&when.then_result_expression());
        }
        pending.push_back(&opr.case_().else_result_expression());
        break;
      case ::common::ExprOpr::kScalarFunc:
        for (const auto& arg : opr.scalar_func().parameters()) {
          pending.push_back(&arg);
        }
        break;
      case ::common::ExprOpr::kUdfFunc:
        for (const auto& arg : opr.udf_func().parameters()) {
          pending.push_back(&arg);
        }
        break;
      case ::common::ExprOpr::kToTuple:
        for (const auto& field : opr.to_tuple().fields()) {
          pending.push_back(&field);
        }
        break;
      case ::common::ExprOpr::kToList:
        for (const auto& field : opr.to_list().fields()) {
          pending.push_back(&field);
        }
        break;
      case ::common::ExprOpr::kToArray:
        for (const auto& field : opr.to_array().fields()) {
          pending.push_back(&field);
        }
        break;
      default:
        break;
      }
    }
  }

  // Keep a stable scan order; finishChunk restores the requested output order.
  std::vector<std::string> columns;
  for (const auto& name : all_columns) {
    if (required.erase(name)) {
      columns.push_back(name);
    }
  }
  if (!required.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Column not found in entry schema: " +
                                     *required.begin());
  }
  return columns;
}

}  // namespace

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

  filter_after_read_ = !optionsBuilder->skipRows(arrowOptions);
  if (filter_after_read_) {
    fallback_columns_ = fallbackProjection(*sharedState);
    auto projection = arrow::dataset::ProjectionDescr::FromNames(
        fallback_columns_, *arrowOptions.scanOptions->dataset_schema);
    if (!projection.ok()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(projection.status().ToString());
    }
    arrowOptions.scanOptions->projection = projection->expression;
    arrowOptions.scanOptions->projected_schema = projection->schema;
  } else if (!optionsBuilder->projectColumns(arrowOptions)) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Failed to set column projection");
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

  // A single Parquet file corresponds to one fragment, i.e. one scan task,
  // so a threaded scanner would have nothing to parallelize over. Split
  // Parquet fragments by row group to create independent scan tasks.
  if (scan_opts->use_threads) {
    auto fragments_result = dataset->GetFragments();
    if (fragments_result.ok()) {
      auto fragments_vec_result = fragments_result.ValueOrDie().ToVector();
      if (fragments_vec_result.ok()) {
        arrow::dataset::FragmentVector split_fragments;
        bool split_any = false;
        for (auto& fragment : fragments_vec_result.ValueOrDie()) {
          auto pq_fragment =
              std::dynamic_pointer_cast<arrow::dataset::ParquetFileFragment>(
                  fragment);
          if (pq_fragment) {
            auto rg_fragments =
                pq_fragment->SplitByRowGroup(arrow::compute::literal(true));
            if (rg_fragments.ok() && rg_fragments->size() > 1) {
              split_any = true;
              split_fragments.insert(split_fragments.end(),
                                     rg_fragments->begin(),
                                     rg_fragments->end());
              continue;
            }
          }
          split_fragments.push_back(std::move(fragment));
        }
        if (split_any) {
          LOG(INFO) << "Split parquet fragments by row group into "
                    << split_fragments.size() << " scan tasks";
          dataset = std::make_shared<arrow::dataset::FragmentDataset>(
              dataset->schema(), std::move(split_fragments));
        }
      } else {
        LOG(WARNING) << "Failed to collect fragments for row group splitting: "
                     << fragments_vec_result.status().message();
      }
    } else {
      LOG(WARNING) << "Failed to get fragments for row group splitting: "
                   << fragments_result.status().message();
    }
  }

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

  int num_cols =
      filter_after_read_ ? fallback_columns_.size() : sharedState->columnNum();
  if (num_cols != table->num_columns()) {
    THROW_IO_EXCEPTION(
        "Column number mismatch between schema and table, schema: " +
        std::to_string(num_cols) +
        ", table: " + std::to_string(table->num_columns()));
  }

  output.clear();
  DataChunk chunk;
  for (int i = 0; i < num_cols; ++i) {
    auto table_column = table->column(i);
    chunk.set(i, arrow_arrays_to_value_column(table_column->chunks()));
  }
  output.append_chunk(finishChunk(std::move(chunk)));
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

  auto batch_supplier =
      std::make_shared<RecordBatchChunkSupplier>(batch_reader, row_num);

  output.clear();
  while (auto chunk = batch_supplier->GetNextChunk()) {
    output.append_chunk(finishChunk(std::move(*chunk)));
  }
}

DataChunk ArrowReader::finishChunk(DataChunk chunk) const {
  if (!filter_after_read_ || chunk.col_num() == 0) {
    return chunk;
  }
  auto filtered = filter_chunk(chunk, sharedState->skipRows, fallback_columns_,
                               sharedState->parameters);
  return project_chunk(filtered, fallback_columns_,
                       sharedState->projectColumns);
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
