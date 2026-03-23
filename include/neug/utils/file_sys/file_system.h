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

#include <arrow/filesystem/localfs.h>
#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "neug/utils/reader/reader.h"

namespace neug {
namespace fsys {

// Unified FileSystem interface for different protocols: local, http, s3, oss
class FileSystem {
 public:
  virtual ~FileSystem() = default;
  // to support path regex patterns, i.e. /path/to/*.csv
  virtual std::vector<std::string> glob(const std::string& path) = 0;
  // Currently, our read and write interfaces depend on arrow file system, so we
  // return arrow file system here
  virtual std::unique_ptr<arrow::fs::FileSystem> toArrowFileSystem() = 0;
  // todo: add other methods like OpenFile, GetFileInfo ...
};

// Create specific FileSystem instance according to schema.
using FileSystemFactory =
    std::function<std::unique_ptr<FileSystem>(const reader::FileSchema&)>;

// Unified management of registered file system factories for different
// protocols
class FileSystemRegistry {
 public:
  FileSystemRegistry();
  ~FileSystemRegistry() = default;

  // register factory function for specific protocol
  void Register(const std::string& protocol, FileSystemFactory factory);

  // create and return specific FileSystem instance according to schema
  std::unique_ptr<FileSystem> Provide(const reader::FileSchema& schema);

 private:
  std::shared_mutex mtx;
  std::unordered_map<std::string, FileSystemFactory> factories_;
};
}  // namespace fsys
}  // namespace neug