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

#include "neug/compiler/common/file_system/file_info.h"

#include "neug/compiler/common/file_system/file_system.h"

#if defined(_WIN32)
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace neug {
namespace common {

uint64_t FileInfo::getFileSize() const {
  return fileSystem->getFileSize(*this);
}

void FileInfo::readFromFile(void* buffer, uint64_t numBytes,
                            uint64_t position) {
  fileSystem->readFromFile(*this, buffer, numBytes, position);
}

int64_t FileInfo::readFile(void* buf, size_t nbyte) {
  return fileSystem->readFile(*this, buf, nbyte);
}

void FileInfo::writeFile(const uint8_t* buffer, uint64_t numBytes,
                         uint64_t offset) {
  fileSystem->writeFile(*this, buffer, numBytes, offset);
}

void FileInfo::syncFile() const { fileSystem->syncFile(*this); }

int64_t FileInfo::seek(uint64_t offset, int whence) {
  return fileSystem->seek(*this, offset, whence);
}

void FileInfo::reset() { fileSystem->reset(*this); }

void FileInfo::truncate(uint64_t size) { fileSystem->truncate(*this, size); }

bool FileInfo::canPerformSeek() const { return fileSystem->canPerformSeek(); }

}  // namespace common
}  // namespace neug
