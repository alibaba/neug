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

#include "json_vfs_reader.h"
#include "neug/utils/exception/exception.h"
#include <algorithm>

namespace gs {
namespace extension {

JsonVFSReader::JsonVFSReader(common::VirtualFileSystem* vfs,
                             const std::string& filePath)
    : currentPosition_(0) {
  if (!vfs) {
    THROW_EXTENSION_EXCEPTION("VirtualFileSystem pointer is null");
  }
  
  common::FileOpenFlags flags(common::FileFlags::READ_ONLY);
  fileInfo_ = vfs->openFile(filePath, flags, nullptr);
  
  if (!fileInfo_) {
    THROW_EXTENSION_EXCEPTION("Failed to open file: " + filePath);
  }
  
  fileSize_ = fileInfo_->getFileSize();
}


JsonVFSReader::~JsonVFSReader() = default;


std::string JsonVFSReader::readAll() {
  if (fileSize_ == 0) {
    return "";
  }
  
  std::vector<char> buffer(fileSize_);
  fileInfo_->readFromFile(buffer.data(), fileSize_, 0);
  
  return std::string(buffer.begin(), buffer.end());
}


uint64_t JsonVFSReader::readNextBuffer(uint8_t* buffer, uint64_t bufferSize) {
  if (currentPosition_ >= fileSize_) {
    return 0;  // EOF
  }
  
  uint64_t remainingBytes = fileSize_ - currentPosition_;
  uint64_t bytesToRead = std::min(bufferSize, remainingBytes);
  
  fileInfo_->readFromFile(buffer, bytesToRead, currentPosition_);
  
  currentPosition_ += bytesToRead;
  return bytesToRead;
}


uint64_t JsonVFSReader::getFileSize() const {
  return fileSize_;
}

}  // namespace extension
}  // namespace gs