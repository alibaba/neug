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

void* MMapContainer::GetData() const { return data_; }

size_t MMapContainer::GetDataSize() const { return size_; }

std::string MMapContainer::GetPath() const { return path_; }

void MMapContainer::Open(const std::string& path) {
  if (!path_.empty() || mmap_data_ != nullptr) {
    Close();
  }
  if (!std::filesystem::exists(path)) {
    LOG(WARNING) << "File does not exist: " << path;
    return;
  }

  path_ = path;
  mmap_size_ = std::filesystem::file_size(path);
  if (mmap_size_ == 0) {
    LOG(WARNING) << "File is empty: " << path;
    return;
  }

  mmap_data_ = mmapImpl(path, mmap_size_);
  if (mmap_data_ == MAP_FAILED) {
    throw std::runtime_error("Failed to mmap file: " + path);
  }
  data_ = static_cast<char*>(mmap_data_) + sizeof(FileHeader);
  size_ = mmap_size_ - sizeof(FileHeader);
  unsigned char data_md5[MD5_DIGEST_LENGTH];
  MD5((unsigned char*) data_, size_, data_md5);
  if (size_ > 0) {
    if (memcmp(data_md5, reinterpret_cast<FileHeader*>(mmap_data_)->data_md5,
               MD5_DIGEST_LENGTH) != 0) {
      THROW_INTERNAL_EXCEPTION("Data integrity check failed for file: " + path);
    }
  }
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

void MMapContainer::Resize(size_t size) {
  if (size == size_) {
    return;
  }
  path_.clear();
  if (size == 0) {
    if (mmap_data_ && mmap_size_ > 0) {
      munmapImpl(mmap_data_, mmap_size_);
    }
    mmap_data_ = nullptr;
    mmap_size_ = 0;
    data_ = nullptr;
    size_ = 0;
    return;
  }

  void* new_mmap_data = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                             MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (new_mmap_data == MAP_FAILED) {
    throw std::runtime_error("Failed to allocate memory for resizing");
  }
  if (mmap_data_ && size_ > 0) {
    memcpy(new_mmap_data, data_, size_ < size ? size_ : size);
    munmapImpl(mmap_data_, mmap_size_);
  }
  mmap_data_ = new_mmap_data;
  mmap_size_ = size;
  data_ = mmap_data_;
  size_ = size;
}

void MMapContainer::Dump(const std::string& path) {
  FileHeader header;
  MD5((unsigned char*) data_, size_, header.data_md5);
  FILE* fp = fopen(path.c_str(), "wb");
  if (fp == nullptr) {
    THROW_IO_EXCEPTION("Failed to open file for writing: " + path);
  }
  auto ret = fwrite(&header, sizeof(FileHeader), 1, fp);
  if (ret != 1) {
    throw std::runtime_error("Failed to write to file: " + path);
  }
  if (size_ > 0) {
    ret = fwrite(data_, size_, 1, fp);
    if (ret != 1) {
      THROW_RUNTIME_ERROR("Failed to write data to file: " + path);
    }
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