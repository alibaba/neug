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

#include <arrow/dataset/dataset.h>
#include <arrow/dataset/scanner.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/type.h>

#include "neug/utils/io/read/common/file_reader.h"
#include "parquet/arrow_options.h"

namespace neug {

class IDataChunkSupplier;

namespace reader {

class DatasetBuilder {
 public:
  DatasetBuilder() = default;
  virtual ~DatasetBuilder() = default;

  virtual std::shared_ptr<arrow::dataset::DatasetFactory> buildFactory(
      std::shared_ptr<ReadSharedState> sharedState,
      std::shared_ptr<arrow::fs::FileSystem> fs,
      std::shared_ptr<arrow::dataset::FileFormat> fileFormat);
};

class ArrowReader : public FileReader {
 public:
  ArrowReader(std::shared_ptr<ReadSharedState> sharedState,
              std::unique_ptr<ArrowOptionsBuilder> optionsBuilder,
              std::shared_ptr<arrow::fs::FileSystem> fileSystem)
      : sharedState_(std::move(sharedState)),
        fileSystem_(std::move(fileSystem)),
        optionsBuilder_(std::move(optionsBuilder)),
        datasetBuilder_(std::make_shared<DatasetBuilder>()) {}
  ArrowReader(std::shared_ptr<ReadSharedState> sharedState,
              std::unique_ptr<ArrowOptionsBuilder> optionsBuilder,
              std::shared_ptr<arrow::fs::FileSystem> fileSystem,
              std::shared_ptr<DatasetBuilder> datasetBuilder)
      : sharedState_(std::move(sharedState)),
        fileSystem_(std::move(fileSystem)),
        optionsBuilder_(std::move(optionsBuilder)),
        datasetBuilder_(std::move(datasetBuilder)) {}
  ~ArrowReader() override = default;

  std::shared_ptr<IDataChunkSupplier> read() override;

  result<std::shared_ptr<EntrySchema>> inferSchema() override;

 protected:
  std::shared_ptr<arrow::dataset::Scanner> createScanner(
      std::shared_ptr<arrow::fs::FileSystem> fs);
  std::shared_ptr<IDataChunkSupplier> full_read(
      std::shared_ptr<arrow::dataset::Scanner> scanner);
  std::shared_ptr<IDataChunkSupplier> batch_read(
      std::shared_ptr<arrow::dataset::Scanner> scanner);

  result<std::shared_ptr<EntrySchema>> convertArrowSchemaToEntrySchema(
      const std::shared_ptr<arrow::Schema>& arrowSchema);

  std::shared_ptr<ReadSharedState> sharedState_;
  std::shared_ptr<arrow::fs::FileSystem> fileSystem_;
  std::unique_ptr<ArrowOptionsBuilder> optionsBuilder_;
  std::shared_ptr<DatasetBuilder> datasetBuilder_;
};

}  // namespace reader
}  // namespace neug
