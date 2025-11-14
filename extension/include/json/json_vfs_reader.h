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

#include <memory>
#include <string>

#include "neug/compiler/common/file_system/file_info.h"
#include "neug/compiler/common/file_system/virtual_file_system.h"

namespace gs {
namespace extension {

// JSON format
enum class JsonFormat {
  ARRAY,              // JSON array: [{"a":1}, {"a":2}]
  NEWLINE_DELIMITED   // JSONL: {"a":1}\n{"a":2}
};

struct JsonReaderConstants {
  static constexpr uint64_t BUFFER_SIZE = 32 * 1024 * 1024; // 32MB
  static constexpr uint64_t PADDING_SIZE = 4;     // rapidjson padding
  static constexpr uint64_t MAX_BUFFER_SIZE = 512ULL * 1024 * 1024;  // 512MB
};

class JsonVFSReader {
 public:
  JsonVFSReader(common::VirtualFileSystem* vfs, const std::string& filePath);
  ~JsonVFSReader();

  std::string readAll();

  uint64_t readNextBuffer(uint8_t* buffer, uint64_t bufferSize);

  bool isEOF() const { return currentPosition_ >= fileSize_; }

  uint64_t getFileSize() const;

  uint64_t getCurrentPosition() const { return currentPosition_; }

  void resetPosition() { currentPosition_ = 0; }

 private:
  std::unique_ptr<common::FileInfo> fileInfo_;
  uint64_t fileSize_;
  uint64_t currentPosition_;
};

}  // namespace extension
}  // namespace gs