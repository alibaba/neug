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

#pragma once

#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "neug/utils/io/read/common/read_state.h"

namespace neug {
namespace fsys {

// Unified FileSystem interface for different protocols: local, http, s3, oss
class FileSystem {
 public:
  virtual ~FileSystem() = default;
  // to support path regex patterns, i.e. /path/to/*.csv
  virtual std::vector<std::string> glob(const std::string& path) = 0;
  /// Opaque Arrow filesystem handle for extension readers (parquet/httpfs).
  /// Returns nullptr when the protocol has no Arrow backend (local paths).
  virtual std::shared_ptr<void> getArrowFileSystem() const { return nullptr; }
};

using FileSystemFactory =
    std::function<std::unique_ptr<FileSystem>(const reader::FileSchema&)>;

class FileSystemRegistry {
 public:
  FileSystemRegistry();
  ~FileSystemRegistry() = default;

  void Register(const std::string& protocol, FileSystemFactory factory);

  std::unique_ptr<FileSystem> Provide(const reader::FileSchema& schema);

 private:
  std::shared_mutex mtx;
  std::unordered_map<std::string, FileSystemFactory> factories_;
};
}  // namespace fsys
}  // namespace neug
