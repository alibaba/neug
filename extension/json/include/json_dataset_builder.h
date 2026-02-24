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

#include <arrow/dataset/dataset.h>
#include <arrow/dataset/discovery.h>
#include <arrow/filesystem/filesystem.h>
#include <memory>
#include "neug/utils/reader/reader.h"

namespace neug {
namespace reader {

/**
 * @brief JSON-specific DatasetBuilder implementation
 *
 * This class provides JSON-specific functionality for creating
 * DatasetFactory instances. It extends the base DatasetBuilder to handle
 * JSON file formats.
 */
class JsonDatasetBuilder : public DatasetBuilder {
 public:
  explicit JsonDatasetBuilder() = default;
  virtual ~JsonDatasetBuilder() = default;

  /**
   * @brief Creates a FileSystemDatasetFactory for JSON files
   *
   * This method creates a FileSystemDatasetFactory using the provided file
   * paths and JSON file format. It can be used for both schema inference and
   * data scanning.
   *
   * @param sharedState The shared read state containing file paths
   * @param fs FileSystem instance
   * @param fileFormat JSON file format instance
   * @return FileSystemDatasetFactory for JSON files
   */
  std::shared_ptr<arrow::dataset::DatasetFactory> buildFactory(
      std::shared_ptr<ReadSharedState> sharedState,
      std::shared_ptr<arrow::fs::FileSystem> fs,
      std::shared_ptr<arrow::dataset::FileFormat> fileFormat) override;
};

}  // namespace reader
}  // namespace neug
