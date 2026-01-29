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

#include <arrow/dataset/dataset.h>
#include <arrow/dataset/discovery.h>
#include <arrow/io/api.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <glog/logging.h>
#include <memory>
#include <string>
#include <vector>
#include "neug/utils/exception/exception.h"
#include "neug/utils/reader/options.h"
#include "neug/utils/reader/reader.h"
#include "neug/utils/service_utils.h"

namespace neug {
namespace reader {

std::shared_ptr<arrow::dataset::DatasetFactory> DatasetBuilder::buildFactory(
    std::shared_ptr<ReadSharedState> sharedState,
    std::shared_ptr<arrow::fs::FileSystem> fs,
    std::shared_ptr<arrow::dataset::FileFormat> fileFormat) {
  if (!sharedState) {
    THROW_INVALID_ARGUMENT_EXCEPTION("SharedState is null");
  }
  if (!fs) {
    THROW_INVALID_ARGUMENT_EXCEPTION("FileSystem is null");
  }
  if (!fileFormat) {
    THROW_INVALID_ARGUMENT_EXCEPTION("File format is null");
  }

  const auto& fileSchema = sharedState->schema.file;
  const std::vector<std::string>& file_paths = fileSchema.paths;

  if (file_paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("No file paths provided");
  }

  arrow::dataset::FileSystemFactoryOptions factory_options;
  factory_options.exclude_invalid_files = false;
  auto factory_result = arrow::dataset::FileSystemDatasetFactory::Make(
      fs, file_paths, fileFormat, factory_options);
  if (!factory_result.ok()) {
    THROW_IO_EXCEPTION("Failed to create FileSystemDatasetFactory: " +
                       factory_result.status().message());
  }
  return factory_result.ValueOrDie();
}

}  // namespace reader
}  // namespace neug