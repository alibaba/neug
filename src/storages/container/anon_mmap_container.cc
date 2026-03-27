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
#include <cstdio>
#include <stdexcept>

#include <glog/logging.h>
#include "neug/storages/container/anon_mmap_container.h"
#include "neug/storages/container/file_header.h"
#include "neug/utils/file_utils.h"

namespace neug {

AnonMMap::AnonMMap() : MMapContainer() {}

AnonMMap::~AnonMMap() {
  if (mmap_data_ && mmap_size_ > 0) {
    munmapImpl(mmap_data_, mmap_size_);
    mmap_data_ = nullptr;
    mmap_size_ = 0;
  }
}

void AnonMMap::OpenAnonymous(size_t size) {
  if (!path_.empty() || mmap_data_ != nullptr) {
    Close();
  }
  path_.clear();
  mmap_size_ = size;
  mmap_data_ = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (mmap_data_ == MAP_FAILED) {
    THROW_RUNTIME_ERROR("Failed to allocate memory");
  }
  data_ = mmap_data_;
  size_ = mmap_size_;
}

void* AnonMMap::mmapImpl(const std::string& path, size_t mmap_size) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd == -1) {
    THROW_RUNTIME_ERROR("Failed to open file: " + path);
  }
  void* mmap_data =
      mmap(nullptr, mmap_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
  close(fd);
  return mmap_data;
}

void AnonMMap::munmapImpl(void* mmap_data, size_t mmap_size) {
  munmap(mmap_data, mmap_size);
}

// AnonHugeMMap implementation
AnonHugeMMap::AnonHugeMMap() : MMapContainer() {}

AnonHugeMMap::~AnonHugeMMap() {
  if (mmap_data_ && mmap_size_ > 0) {
    munmapImpl(mmap_data_, mmap_size_);
  }
}

void AnonHugeMMap::OpenAnonymous(size_t size) {
  if (!path_.empty() || mmap_data_ != nullptr) {
    Close();
  }
  path_.clear();
  mmap_size_ = size;
  size_t hugepage_size = file_utils::hugepage_round_up(size);
  mmap_data_ = file_utils::allocate_hugepages(hugepage_size);
  if (mmap_data_ == MAP_FAILED) {
    THROW_RUNTIME_ERROR("Failed to allocate memory");
  }
  data_ = mmap_data_;
  size_ = size;
}

void* try_allocate_hugepages(size_t size) {
  void* mmap_data = file_utils::allocate_hugepages(size);
  if (mmap_data == MAP_FAILED) {
    LOG(WARNING)
        << "Failed to allocate hugepages, falling back to regular mmap";
    mmap_data = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (mmap_data == MAP_FAILED) {
      THROW_RUNTIME_ERROR("Failed to allocate memory");
    }
  }
  return mmap_data;
}

void AnonHugeMMap::Resize(size_t size) {
  if (size == size_) {
    return;  // No need to resize if the new size is smaller or equal
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
  size_t hugepage_size = file_utils::hugepage_round_up(size);
  void* new_mmap_data = try_allocate_hugepages(hugepage_size);
  if (mmap_data_ && size_ > 0) {
    memcpy(new_mmap_data, data_, size_ < size ? size_ : size);
    munmapImpl(mmap_data_, mmap_size_);
  }
  mmap_data_ = new_mmap_data;
  mmap_size_ = hugepage_size;
  data_ = mmap_data_;
  size_ = size;
}

void* AnonHugeMMap::mmapImpl(const std::string& path, size_t mmap_size) {
  size_t hugepage_size = file_utils::hugepage_round_up(mmap_size);
  void* mmap_data = try_allocate_hugepages(hugepage_size);
  FILE* fp = fopen(path.c_str(), "rb");
  if (fp == nullptr) {
    THROW_RUNTIME_ERROR("Failed to open file: " + path);
  }
  auto ret = fread(mmap_data, 1, mmap_size, fp);
  if (ret != mmap_size) {
    THROW_RUNTIME_ERROR("Failed to read from file: " + path);
  }
  fclose(fp);
  return mmap_data;
}

void AnonHugeMMap::munmapImpl(void* mmap_data, size_t mmap_size) {
  size_t hugepage_size = file_utils::hugepage_round_up(mmap_size);
  munmap(mmap_data, hugepage_size);
}

}  // namespace neug