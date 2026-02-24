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

#include <cstdint>
#include <string>

#include "neug/compiler/common/cast.h"
#include "neug/utils/api.h"

namespace neug {
namespace common {

class FileSystem;

struct NEUG_API FileInfo {
  FileInfo(std::string path, FileSystem* fileSystem)
      : path{std::move(path)}, fileSystem{fileSystem} {}

  virtual ~FileInfo() = default;

  uint64_t getFileSize() const;

  void readFromFile(void* buffer, uint64_t numBytes, uint64_t position);

  int64_t readFile(void* buf, size_t nbyte);

  void writeFile(const uint8_t* buffer, uint64_t numBytes, uint64_t offset);

  void syncFile() const;

  int64_t seek(uint64_t offset, int whence);

  void reset();

  void truncate(uint64_t size);

  bool canPerformSeek() const;

  template <class TARGET>
  TARGET* ptrCast() {
    return common::neug_dynamic_cast<TARGET*>(this);
  }

  template <class TARGET>
  const TARGET* constPtrCast() const {
    return common::neug_dynamic_cast<const TARGET*>(this);
  }

  template <class TARGET>
  const TARGET& constCast() const {
    return common::neug_dynamic_cast<const TARGET&>(*this);
  }

  template <class TARGET>
  TARGET& cast() {
    return common::neug_dynamic_cast<TARGET&>(*this);
  }

  const std::string path;

  FileSystem* fileSystem;
};

}  // namespace common
}  // namespace neug
