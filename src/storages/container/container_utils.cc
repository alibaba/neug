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

#include "neug/storages/container/container_utils.h"

#include <cstdio>
#include <filesystem>
#include <functional>
#include <memory>
#include <utility>

#include "neug/storages/container/anon_mmap_container.h"
#include "neug/storages/container/file_header.h"
#include "neug/storages/container/file_mmap_container.h"
#include "neug/storages/container/i_container.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/file_utils.h"

namespace neug {

void prepare_container_file(const std::string& snapshot_file,
                            const std::string& tmp_file) {
  // Ensure the target directory exists
  auto parent_dir = std::filesystem::path(tmp_file).parent_path();
  if (!parent_dir.empty()) {
    std::filesystem::create_directories(parent_dir);
  }

  if (std::filesystem::exists(snapshot_file)) {
    file_utils::copy_file(snapshot_file, tmp_file, true);
  } else {
    file_utils::create_file(tmp_file, sizeof(FileHeader));
  }
}

std::unique_ptr<IDataContainer> OpenDataContainer(
    MemoryLevel strategy, const std::string& file_name) {
  if (strategy == MemoryLevel::kSyncToFile) {
    if (file_name.empty()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "File name must be provided for file-backed mmap strategy");
    }
  }
  switch (strategy) {
  case MemoryLevel::kInMemory: {
    if (file_name.empty()) {
      return std::make_unique<AnonMMap>();
    } else {
      auto ret = std::make_unique<FilePrivateMMap>();
      if (std::filesystem::exists(file_name)) {
        ret->Open(file_name);
      }
      return ret;
    }
  }
  case MemoryLevel::kHugePagePrefered: {
    auto ret = std::make_unique<AnonHugeMMap>();
    if (std::filesystem::exists(file_name)) {
      ret->Open(file_name);
    }
    return ret;
  }
  case MemoryLevel::kSyncToFile: {
    auto ret = std::make_unique<FileSharedMMap>();
    if (!std::filesystem::exists(file_name)) {
      file_utils::create_file(file_name, sizeof(FileHeader));
    }
    ret->Open(file_name);
    return ret;
  }
  default:
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Unsupported storage strategy: " +
        std::to_string(static_cast<int>(strategy)));
  }
}

std::unique_ptr<IDataContainer> OpenContainer(const std::string& snapshot_file,
                                              const std::string& tmp_file,
                                              MemoryLevel memory_level) {
  if (memory_level == MemoryLevel::kSyncToFile) {
    if (tmp_file.empty()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Temporary file path is required for disk-backed containers");
    }
    // For disk-backed containers, prepare the file first
    prepare_container_file(snapshot_file, tmp_file);
    return OpenDataContainer(memory_level, tmp_file);
  } else {
    // For in-memory or hugepage containers, use snapshot file directly
    return OpenDataContainer(memory_level, snapshot_file);
  }
}

}  // namespace neug
