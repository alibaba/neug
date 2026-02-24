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

#pragma once

#include <string>
#include <vector>

#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/compiler/common/copy_constructors.h"
#include "neug/compiler/common/types/value/value.h"

namespace neug {
namespace common {

enum class FileType : uint8_t {
  UNKNOWN = 0,
  CSV = 1,
  PARQUET = 2,
  NPY = 3,
};

struct FileTypeInfo {
  FileType fileType = FileType::UNKNOWN;
  std::string fileTypeStr;
};

struct FileTypeUtils {
  static FileType getFileTypeFromExtension(std::string_view extension);
  static std::string toString(FileType fileType);
  static FileType fromString(std::string fileType);
};

struct FileScanInfo {
  static constexpr const char* FILE_FORMAT_OPTION_NAME = "FILE_FORMAT";

  FileTypeInfo fileTypeInfo;
  std::vector<std::string> filePaths;
  case_insensitive_map_t<Value> options;

  FileScanInfo() : fileTypeInfo{FileType::UNKNOWN, ""} {}
  FileScanInfo(FileTypeInfo fileTypeInfo, std::vector<std::string> filePaths)
      : fileTypeInfo{std::move(fileTypeInfo)},
        filePaths{std::move(filePaths)} {}
  EXPLICIT_COPY_DEFAULT_MOVE(FileScanInfo);

  uint32_t getNumFiles() const { return filePaths.size(); }
  std::string getFilePath(idx_t fileIdx) const {
    NEUG_ASSERT(fileIdx < getNumFiles());
    return filePaths[fileIdx];
  }

  template <typename T>
  T getOption(std::string optionName, T defaultValue) const {
    const auto optionIt = options.find(optionName);
    if (optionIt != options.end()) {
      return optionIt->second.getValue<T>();
    } else {
      return defaultValue;
    }
  }

 private:
  FileScanInfo(const FileScanInfo& other)
      : fileTypeInfo{other.fileTypeInfo},
        filePaths{other.filePaths},
        options{other.options} {}
};

}  // namespace common
}  // namespace neug
