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

#include "neug/storages/container/file_header.h"
#include "neug/storages/container/mmap_container.h"
#include "neug/utils/file_utils.h"

#include <glog/logging.h>

namespace neug {

MMapContainer::MMapContainer()
    : IDataContainer(), mmap_data_(nullptr), mmap_size_(0) {}

std::string MMapContainer::GetPath() const { return path_; }

void MMapContainer::Open(const std::string& path) {
  if (!path_.empty() || mmap_data_ != nullptr) {
    Close();
  }
  if (!std::filesystem::exists(path)) {
    THROW_IO_EXCEPTION("File does not exist: " + path);
    return;
  }

  path_ = path;
  mmap_size_ = std::filesystem::file_size(path);
  if (mmap_size_ == 0) {
    path_.clear();
    LOG(WARNING) << "File is empty: " << path;
    return;
  }

  mmap_data_ = mmapImpl(path, mmap_size_);
  if (mmap_data_ == MAP_FAILED) {
    THROW_IO_EXCEPTION("Failed to mmap file: " + path);
  }
  if (mmap_size_ < sizeof(FileHeader)) {
    munmapImpl(mmap_data_, mmap_size_);
    mmap_data_ = nullptr;
    mmap_size_ = 0;
    path_.clear();
    THROW_IO_EXCEPTION("File too small to contain header: " + path);
  }
  data_ = static_cast<char*>(mmap_data_) + sizeof(FileHeader);
  size_ = mmap_size_ - sizeof(FileHeader);
  // Skip checksum verification, always assume it is correct.
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

  // Save old state before allocating, so we can always unmap on success
  // regardless of whether size_ was 0 (header-only file case).
  void* old_mmap_data = mmap_data_;
  size_t old_mmap_size = mmap_size_;
  void* old_data = data_;
  size_t old_size = size_;

  void* new_mmap_data = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                             MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (new_mmap_data == MAP_FAILED) {
    THROW_IO_EXCEPTION("Failed to create anonymous mmap for resizing");
  }

  // Copy existing payload only when there is payload to copy
  if (old_data && old_size > 0) {
    memcpy(new_mmap_data, old_data, std::min(old_size, size));
  }

  // Always unmap the old region if it was mapped, even when size_ was 0
  if (old_mmap_data && old_mmap_size > 0) {
    munmapImpl(old_mmap_data, old_mmap_size);
  }

  mmap_data_ = new_mmap_data;
  mmap_size_ = size;
  data_ = mmap_data_;
  size_ = size;
}

void MMapContainer::Dump(const std::string& path) {
  FileHeader header;
  MD5((unsigned char*) data_, size_, header.data_md5);
  std::unique_ptr<FILE, decltype(&fclose)> fp(fopen(path.c_str(), "wb"),
                                              &fclose);
  if (fp == nullptr) {
    THROW_IO_EXCEPTION("Failed to open file for writing: " + path);
  }
  auto ret = fwrite(&header, sizeof(FileHeader), 1, fp.get());
  if (ret != 1) {
    THROW_IO_EXCEPTION("Failed to write header to file: " + path);
  }
  if (size_ > 0) {
    ret = fwrite(data_, size_, 1, fp.get());
    if (ret != 1) {
      THROW_IO_EXCEPTION("Failed to write data to file: " + path);
    }
  }
  Close();
}

bool MMapContainer::IsDirty() {
  // Guard: no mapping or mapping too small to hold a header → never dirty.
  if (mmap_data_ == nullptr || mmap_size_ < sizeof(FileHeader)) {
    return false;
  }
  if (path_.empty()) {
    // Anonymous mmap with no backing file – always considered dirty.
    return true;
  }
  if (size_ == 0) {
    // Header-only file: no payload to compare, so not dirty.
    return false;
  }
  unsigned char md5[MD5_DIGEST_LENGTH];
  MD5((unsigned char*) data_, size_, md5);
  return memcmp(md5, reinterpret_cast<FileHeader*>(mmap_data_)->data_md5,
                MD5_DIGEST_LENGTH) != 0;
}

}  // namespace neug