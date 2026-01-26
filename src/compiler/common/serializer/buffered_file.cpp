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

#include "neug/compiler/common/serializer/buffered_file.h"

#include <cstring>

#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/file_system/file_info.h"
#include "neug/compiler/common/system_config.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace common {

static constexpr uint64_t BUFFER_SIZE = NEUG_PAGE_SIZE;

BufferedFileWriter::~BufferedFileWriter() { flush(); }

BufferedFileWriter::BufferedFileWriter(FileInfo& fileInfo)
    : buffer(std::make_unique<uint8_t[]>(BUFFER_SIZE)),
      fileOffset(0),
      bufferOffset(0),
      fileInfo(fileInfo) {}

void BufferedFileWriter::write(const uint8_t* data, uint64_t size) {
  if (size > BUFFER_SIZE) {
    flush();
    fileInfo.writeFile(data, size, fileOffset);
    fileOffset += size;
    return;
  }
  NEUG_ASSERT(size <= BUFFER_SIZE);
  if (bufferOffset + size <= BUFFER_SIZE) {
    memcpy(&buffer[bufferOffset], data, size);
    bufferOffset += size;
  } else {
    auto toCopy = BUFFER_SIZE - bufferOffset;
    memcpy(&buffer[bufferOffset], data, toCopy);
    bufferOffset += toCopy;
    flush();
    auto remaining = size - toCopy;
    memcpy(buffer.get(), data + toCopy, remaining);
    bufferOffset += remaining;
  }
}

void BufferedFileWriter::flush() {
  if (bufferOffset == 0) {
    return;
  }
  fileInfo.writeFile(buffer.get(), bufferOffset, fileOffset);
  fileOffset += bufferOffset;
  bufferOffset = 0;
  memset(buffer.get(), 0, BUFFER_SIZE);
}

void BufferedFileWriter::sync() { fileInfo.syncFile(); }

uint64_t BufferedFileWriter::getFileSize() const {
  return fileInfo.getFileSize() + bufferOffset;
}

BufferedFileReader::BufferedFileReader(std::unique_ptr<FileInfo> fileInfo)
    : buffer(std::make_unique<uint8_t[]>(BUFFER_SIZE)),
      fileOffset(0),
      bufferOffset(0),
      fileInfo(std::move(fileInfo)),
      bufferSize{0} {
  fileSize = this->fileInfo->getFileSize();
  readNextPage();
}

void BufferedFileReader::read(uint8_t* data, uint64_t size) {
  if (size > BUFFER_SIZE) {
    // Clear read buffer.
    fileOffset -= bufferSize;
    fileOffset += bufferOffset;
    fileInfo->readFromFile(data, size, fileOffset);
    fileOffset += size;
    bufferOffset = bufferSize;
  } else if (bufferOffset + size <= bufferSize) {
    memcpy(data, &buffer[bufferOffset], size);
    bufferOffset += size;
  } else {
    auto toCopy = bufferSize - bufferOffset;
    memcpy(data, &buffer[bufferOffset], toCopy);
    bufferOffset += toCopy;
    readNextPage();
    auto remaining = size - toCopy;
    memcpy(data + toCopy, buffer.get(), remaining);
    bufferOffset += remaining;
  }
}

bool BufferedFileReader::finished() {
  return bufferOffset >= bufferSize && fileSize <= fileOffset;
}

void BufferedFileReader::readNextPage() {
  if (fileSize <= fileOffset) {
    THROW_RUNTIME_ERROR(stringFormat(
        "Reading past the end of the file {} with size {} at offset {}",
        fileInfo->path, fileSize, fileOffset));
  }
  bufferSize = std::min(fileSize - fileOffset, BUFFER_SIZE);
  fileInfo->readFromFile(buffer.get(), bufferSize, fileOffset);
  fileOffset += bufferSize;
  bufferOffset = 0;
}

}  // namespace common
}  // namespace neug
