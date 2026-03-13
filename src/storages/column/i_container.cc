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

#include "neug/storages/column/i_container.h"
#include "neug/storages/column/anon_mmap_container.h"
#include "neug/storages/column/file_mmap_container.h"
#include "neug/utils/exception/exception.h"

#include <glog/logging.h>

namespace neug {

std::unique_ptr<IDataContainer> CreateDataContainer(
    MemoryLevel strategy, const std::string& file_name, size_t size) {
  if (strategy == MemoryLevel::kSyncToFile) {
    if (file_name.empty()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "File name must be provided for file-backed mmap strategy");
    }
  }
  switch (strategy) {
  case MemoryLevel::kInMemory: {
    if (file_name.empty()) {
      auto ret = std::make_unique<AnonMMap>();
      ret->OpenAnonymous(size);
      return ret;
    } else {
      auto ret = std::make_unique<FilePrivateMMap>();
      ret->Open(file_name);
      ret->Resize(size);
      return ret;
    }
  }
  case MemoryLevel::kHugePagePrefered: {
    // TODO(zhanglei): Determine whether hugepage is actually available and
    // fallback to normal mmap if not
    auto ret = std::make_unique<AnonHugeMMap>();
    ret->Open(file_name);
    ret->Resize(size);
    return ret;
  }
  case MemoryLevel::kSyncToFile: {
    auto ret = std::make_unique<FileSharedMMap>();
    ret->Open(file_name);
    ret->Resize(size);
    return ret;
  }
  default:
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Unsupported storage strategy: " +
        std::to_string(static_cast<int>(strategy)));
  }
}

}  // namespace neug
