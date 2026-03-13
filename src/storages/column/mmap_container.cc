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

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <stdexcept>

#include "neug/storages/column/file_header.h"
#include "neug/storages/column/mmap_container.h"

namespace neug {

MMapContainer::MMapContainer()
    : mmap_data_(nullptr), mmap_size_(0), data_(nullptr), size_(0) {}

void* MMapContainer::GetData() { return data_; }

size_t MMapContainer::GetDataSize() { return size_; }

std::string MMapContainer::GetPath() { return path_; }

void MMapContainer::Open(const std::string& path) {
  if (!path_.empty() || mmap_data_ != nullptr) {
    Close();
  }

  path_ = path;
  mmap_size_ = std::filesystem::file_size(path);

  mmap_data_ = mmapImpl(path, mmap_size_);
  if (mmap_data_ == MAP_FAILED) {
    throw std::runtime_error("Failed to mmap file: " + path);
  }
  data_ = static_cast<char*>(mmap_data_) + sizeof(FileHeader);
  size_ = mmap_size_ - sizeof(FileHeader);
}

void MMapContainer::Close() {
  if (mmap_data_ && mmap_size_ > 0) {
    munmapImpl(mmap_data_, mmap_size_);
  }
  path_.clear();
  mmap_data_ = nullptr;
  mmap_size_ = 0;
  data_ = nullptr;
  size_ = 0;
}

void MMapContainer::Sync() {}

void MMapContainer::Dump(const std::string& path) {
  FileHeader header;
  MD5((unsigned char*) data_, size_, header.data_md5);
  FILE* fp = fopen(path.c_str(), "wb");
  if (fp == nullptr) {
    throw std::runtime_error("Failed to open file: " + path_);
  }
  auto ret = fwrite(&header, sizeof(FileHeader), 1, fp);
  if (ret != 1) {
    throw std::runtime_error("Failed to write to file: " + path);
  }
  ret = fwrite(data_, size_, 1, fp);
  if (ret != 1) {
    throw std::runtime_error("Failed to write to file: " + path);
  }
  fclose(fp);
}

bool MMapContainer::IsDirty() {
  if (path_ == "" && mmap_data_ != nullptr) {
    return true;
  }
  unsigned char md5[MD5_DIGEST_LENGTH];
  MD5((unsigned char*) data_, size_, md5);
  return memcmp(md5, reinterpret_cast<FileHeader*>(mmap_data_)->data_md5,
                MD5_DIGEST_LENGTH) != 0;
}

}  // namespace neug