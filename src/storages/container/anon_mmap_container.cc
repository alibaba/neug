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
#include <functional>
#include <memory>
#include <stdexcept>
#ifdef __linux__
#include <linux/fs.h>
#include <sys/syscall.h>
#endif
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <memory>
#include <stdexcept>

#ifdef __ia64__
#define ADDR (void*) (0x8000000000000000UL)
#ifdef MAP_HUGETLB
#define FLAGS (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_FIXED)
#else
#define FLAGS (MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED)
#endif
#else
#define ADDR (void*) (0x0UL)
#ifdef MAP_HUGETLB
#define FLAGS (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB)
#else
#define FLAGS (MAP_PRIVATE | MAP_ANONYMOUS)
#endif
#endif

#define PROTECTION (PROT_READ | PROT_WRITE)

#define HUGEPAGE_SIZE (2UL * 1024 * 1024)
#define HUGEPAGE_MASK (2UL * 1024 * 1024 - 1UL)
#define ROUND_UP(size) (((size) + HUGEPAGE_MASK) & (~HUGEPAGE_MASK))

#include <glog/logging.h>
#include "neug/storages/container/anon_mmap_container.h"
#include "neug/storages/container/file_header.h"
#include "neug/utils/file_utils.h"

namespace neug {

void* allocate_hugepages(size_t size) {
  return mmap(ADDR, ROUND_UP(size), PROTECTION, FLAGS, -1, 0);
}

size_t hugepage_round_up(size_t size) { return ROUND_UP(size); }

#undef ADDR
#undef FLAGS
#undef HUGEPAGE_SIZE
#undef HUGEPAGE_MASK
#undef ROUND_UP
#undef PROTECTION

AnonMMap::AnonMMap() : MMapContainer() {}

AnonMMap::~AnonMMap() { Close(); }

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

AnonHugeMMap::~AnonHugeMMap() { Close(); }

void AnonHugeMMap::OpenAnonymous(size_t size) {
  if (!path_.empty() || mmap_data_ != nullptr) {
    Close();
  }
  path_.clear();
  mmap_size_ = size;
  size_t hugepage_size = hugepage_round_up(size);
  mmap_data_ = allocate_hugepages(hugepage_size);
  if (mmap_data_ == MAP_FAILED) {
    THROW_RUNTIME_ERROR("Failed to allocate memory");
  }
  data_ = mmap_data_;
  size_ = size;
}

void* try_allocate_hugepages(size_t size) {
  void* mmap_data = allocate_hugepages(size);
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
  size_t hugepage_size = hugepage_round_up(size);
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
  size_t hugepage_size = hugepage_round_up(mmap_size);
  void* mmap_data = try_allocate_hugepages(hugepage_size);
  // RAII guard: munmap hugepages on any error path.
  auto mmap_guard = std::unique_ptr<void, std::function<void(void*)>>(
      mmap_data, [hugepage_size](void* p) { munmap(p, hugepage_size); });

  std::unique_ptr<FILE, decltype(&fclose)> fp(fopen(path.c_str(), "rb"),
                                              &fclose);
  if (fp == nullptr) {
    THROW_RUNTIME_ERROR("Failed to open file: " + path);
  }
  auto ret = fread(mmap_data, 1, mmap_size, fp.get());
  if (ret != mmap_size) {
    THROW_RUNTIME_ERROR("Failed to read from file: " + path);
  }
  mmap_guard.release();  // Ownership transferred to caller.
  return mmap_data;
}

void AnonHugeMMap::munmapImpl(void* mmap_data, size_t mmap_size) {
  size_t hugepage_size = hugepage_round_up(mmap_size);
  munmap(mmap_data, hugepage_size);
}

}  // namespace neug