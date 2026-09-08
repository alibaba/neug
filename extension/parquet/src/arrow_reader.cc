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

#include "parquet/arrow_column.h"
#include "parquet/arrow_reader.h"
#include "parquet/record_batch_supplier.h"

#include "neug/compiler/common/assert.h"
#include "neug/execution/common/context.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/options.h"

namespace neug {
namespace reader {

void ArrowReader::read(std::shared_ptr<ReadLocalState> localState,
                       execution::Context& ctx) {
  auto supplier = getDataChunkSupplier();
  ctx.clear();
  while (auto chunk = supplier->GetNextChunk()) {
    ctx.append_chunk(std::move(*chunk));
  }
  ReadOptions options;
  if (!options.batch_read.get(sharedState->schema.file.options)) {
    ctx.flatten();
  }
}

std::shared_ptr<IDataChunkSupplier> ArrowReader::getDataChunkSupplier() {
  auto scanner = createScanner(fileSystem);
  auto batches = scanner->ToRecordBatchReader();
  if (!batches.ok()) {
    THROW_IO_EXCEPTION("Failed to create RecordBatchReader: " +
                       batches.status().message());
  }
  // Row count is unknown until consumption. Never scan the dataset merely to
  // count rows before producing its first batch.
  return std::make_shared<RecordBatchChunkSupplier>(batches.ValueOrDie(), -1,
                                                    std::move(scanner));
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

  if (!optionsBuilder->projectColumns(arrowOptions)) {
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
