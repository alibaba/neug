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

#include <memory>

#include "neug/compiler/common/serializer/reader.h"
#include "neug/compiler/common/serializer/writer.h"

namespace neug {
namespace common {

struct FileInfo;

class BufferedFileWriter final : public Writer {
 public:
  explicit BufferedFileWriter(FileInfo& fileInfo);
  ~BufferedFileWriter() override;

  void write(const uint8_t* data, uint64_t size) override;

  void flush();
  void sync();

  // Note: this function is reseting next file offset to be written. Make sure
  // buffer is empty.
  void setFileOffset(uint64_t fileOffset) { this->fileOffset = fileOffset; }
  uint64_t getFileOffset() const { return fileOffset; }
  void resetOffsets() {
    fileOffset = 0;
    bufferOffset = 0;
  }

  uint64_t getFileSize() const;
  FileInfo& getFileInfo() const { return fileInfo; }

 protected:
  std::unique_ptr<uint8_t[]> buffer;
  uint64_t fileOffset, bufferOffset;
  FileInfo& fileInfo;
};

class BufferedFileReader final : public Reader {
 public:
  explicit BufferedFileReader(std::unique_ptr<FileInfo> fileInfo);

  void read(uint8_t* data, uint64_t size) override;

  bool finished() override;

 private:
  std::unique_ptr<uint8_t[]> buffer;
  uint64_t fileOffset, bufferOffset;
  std::unique_ptr<FileInfo> fileInfo;
  uint64_t fileSize;
  uint64_t bufferSize;

 private:
  void readNextPage();
};

}  // namespace common
}  // namespace neug
