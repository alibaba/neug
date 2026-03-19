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

#include "neug/utils/file_sys/file_system.h"

#include <arrow/filesystem/localfs.h>
#include <mutex>
#include <string>
#include <vector>

#include "neug/execution/execute/ops/batch/batch_update_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/reader/schema.h"

namespace neug {
namespace fsys {

class LocalFileSystem : public FileSystem {
 public:
  LocalFileSystem() = default;

  std::vector<std::string> glob(const std::string& path) override {
    // Normalize file:// URIs by stripping the scheme before passing to
    // match_files_with_pattern, which uses POSIX glob() and std::filesystem
    // and expects local paths without URI scheme.
    constexpr const char* kFilePrefix = "file://";
    constexpr size_t kFilePrefixLen = 7;
    if (path.starts_with(kFilePrefix)) {
      std::string local_path = path.substr(kFilePrefixLen);
      if (local_path.empty() || local_path[0] != '/') {
        local_path = "/" + local_path;
      }
      return neug::execution::ops::match_files_with_pattern(local_path);
    }
    return neug::execution::ops::match_files_with_pattern(path);
  }

  std::unique_ptr<arrow::fs::FileSystem> toArrowFileSystem() override {
    return std::make_unique<arrow::fs::LocalFileSystem>();
  }
};

FileSystemRegistry::FileSystemRegistry() {
  Register("file", [](const reader::FileSchema&) {
    return std::make_unique<LocalFileSystem>();
  });
}

void FileSystemRegistry::Register(const std::string& protocol,
                                  FileSystemFactory factory) {
  std::unique_lock<std::shared_mutex> lck(mtx);
  factories_[protocol] = std::move(factory);
}

std::unique_ptr<FileSystem> FileSystemRegistry::Provide(
    const reader::FileSchema& schema) {
  std::string protocol = schema.protocol;
  if (protocol.empty()) {
    const auto& paths = schema.paths;
    if (paths.empty()) {
      THROW_INVALID_ARGUMENT_EXCEPTION("No file paths provided");
    }
    // we assume all paths share the same protocol
    const auto& path = paths[0];
    auto pos = path.find("://");
    if (pos != std::string::npos) {
      protocol = path.substr(0, pos);
    } else {
      protocol = "file";
    }
  }

  FileSystemFactory factory;
  {
    std::shared_lock<std::shared_mutex> lck(mtx);
    auto it = factories_.find(protocol);
    if (it == factories_.end()) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Unsupported file system protocol: " +
                                       protocol);
    }
    factory = it->second;
  }
  return factory(schema);
}

}  // namespace fsys
}  // namespace neug
