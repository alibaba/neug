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

#include "neug/storages/container/i_container.h"
#include "neug/storages/container/anon_mmap_container.h"
#include "neug/storages/container/file_header.h"
#include "neug/storages/container/file_mmap_container.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/file_utils.h"

#include <glog/logging.h>

namespace neug {

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
      CreateEmptyContainerFile(file_name);
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

void CreateEmptyContainerFile(const std::string& path) {
  file_utils::create_file(path, sizeof(FileHeader));
  FileHeader header;
  memset(header.data_md5, 0, MD5_DIGEST_LENGTH);
  FILE* fp = fopen(path.c_str(), "wb");
  if (fp == nullptr) {
    THROW_IO_EXCEPTION("Failed to create file: " + path);
  }
  auto ret = fwrite(&header, sizeof(FileHeader), 1, fp);
  if (ret != 1) {
    fclose(fp);
    THROW_IO_EXCEPTION("Failed to write header to file: " + path);
  }
  fclose(fp);
}

}  // namespace neug
