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

#include "neug/utils/io/reader.h"
#include "parquet/arrow_options.h"

namespace neug {
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

template <class FileSystem>
class Reader {
 public:
  Reader(std::shared_ptr<ReadSharedState> sharedState,
         std::shared_ptr<FileSystem> fileSystem)
      : sharedState(std::move(sharedState)),
        fileSystem(std::move(fileSystem)) {}
  virtual ~Reader() = default;

  virtual void read(std::shared_ptr<ReadLocalState> localState,
                    execution::Context& ctx) = 0;

 protected:
  std::shared_ptr<ReadSharedState> sharedState;
  std::shared_ptr<FileSystem> fileSystem;
};

class ArrowReader : public Reader<arrow::fs::FileSystem> {
 public:
  ArrowReader(std::shared_ptr<ReadSharedState> sharedState,
              std::unique_ptr<ArrowOptionsBuilder> optionsBuilder,
              std::shared_ptr<arrow::fs::FileSystem> fileSystem)
      : Reader(std::move(sharedState), std::move(fileSystem)),
        optionsBuilder(std::move(optionsBuilder)),
        datasetBuilder(std::make_shared<DatasetBuilder>()) {}
  ArrowReader(std::shared_ptr<ReadSharedState> sharedState,
              std::unique_ptr<ArrowOptionsBuilder> optionsBuilder,
              std::shared_ptr<arrow::fs::FileSystem> fileSystem,
              std::shared_ptr<DatasetBuilder> datasetBuilder)
      : Reader(std::move(sharedState), std::move(fileSystem)),
        optionsBuilder(std::move(optionsBuilder)),
        datasetBuilder(std::move(datasetBuilder)) {}
  ~ArrowReader() override = default;

  void read(std::shared_ptr<ReadLocalState> localState,
            execution::Context& ctx) override;

  arrow::Result<std::shared_ptr<arrow::Schema>> inferSchema();

 protected:
  std::shared_ptr<arrow::dataset::Scanner> createScanner(
      std::shared_ptr<arrow::fs::FileSystem> fs);
  void full_read(std::shared_ptr<arrow::dataset::Scanner> scanner,
                 execution::Context& output);
  void batch_read(std::shared_ptr<arrow::dataset::Scanner> scanner,
                  execution::Context& output);

  std::unique_ptr<ArrowOptionsBuilder> optionsBuilder;
  std::shared_ptr<DatasetBuilder> datasetBuilder;
};

}  // namespace reader
}  // namespace neug
