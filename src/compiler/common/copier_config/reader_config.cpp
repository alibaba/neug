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

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/copier_config/file_scan_info.h"
#include "neug/compiler/common/string_utils.h"

namespace neug {
namespace common {

FileType FileTypeUtils::getFileTypeFromExtension(std::string_view extension) {
  if (extension == ".csv") {
    return FileType::CSV;
  }
  if (extension == ".parquet") {
    return FileType::PARQUET;
  }
  if (extension == ".npy") {
    return FileType::NPY;
  }
  return FileType::UNKNOWN;
}

std::string FileTypeUtils::toString(FileType fileType) {
  switch (fileType) {
  case FileType::UNKNOWN: {
    return "UNKNOWN";
  }
  case FileType::CSV: {
    return "CSV";
  }
  case FileType::PARQUET: {
    return "PARQUET";
  }
  case FileType::NPY: {
    return "NPY";
  }
  default: {
    NEUG_UNREACHABLE;
  }
  }
}

FileType FileTypeUtils::fromString(std::string fileType) {
  fileType = common::StringUtils::getUpper(fileType);
  if (fileType == "CSV") {
    return FileType::CSV;
  } else if (fileType == "PARQUET") {
    return FileType::PARQUET;
  } else if (fileType == "NPY") {
    return FileType::NPY;
  } else {
    return FileType::UNKNOWN;
    // THROW_BINDER_EXCEPTION(stringFormat("Unsupported file type:
    // {}.", fileType));
  }
}

}  // namespace common
}  // namespace neug
