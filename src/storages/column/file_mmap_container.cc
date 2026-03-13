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
#include <cstring>
#include <stdexcept>

#include "neug/storages/column/file_header.h"
#include "neug/storages/column/file_mmap_container.h"

namespace neug {

// FilePrivateMMap implementation
FilePrivateMMap::FilePrivateMMap() : MMapContainer() {}

FilePrivateMMap::~FilePrivateMMap() {
  if (mmap_data_ && mmap_size_ > 0) {
    munmapImpl(mmap_data_, mmap_size_);
  }
}

void FilePrivateMMap::OpenAnonymous(size_t size) {
  if (!path_.empty() || mmap_data_ != nullptr) {
    Close();
  }
  path_.clear();
  mmap_size_ = size;
  mmap_data_ = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (mmap_data_ == MAP_FAILED) {
    throw std::runtime_error("Failed to allocate memory");
  }
  data_ = mmap_data_;
  size_ = mmap_size_;
}

void FilePrivateMMap::Resize(size_t size) {
  if (size <= size_) {
    return;  // No need to resize if the new size is smaller or equal
  }
  void* new_mmap_data = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                             MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (new_mmap_data == MAP_FAILED) {
    throw std::runtime_error("Failed to allocate memory for resizing");
  }
  // Copy existing data to the new mapping
  std::memcpy(new_mmap_data, mmap_data_, size_);
  // Unmap the old mapping
  munmapImpl(mmap_data_, mmap_size_);
  // Update pointers and sizes
  mmap_data_ = new_mmap_data;
  mmap_size_ = size;
  data_ = mmap_data_;
  size_ = size;
}

void* FilePrivateMMap::mmapImpl(const std::string& path, size_t mmap_size) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd == -1) {
    throw std::runtime_error("Failed to open file: " + path);
  }
  void* mmap_data =
      mmap(nullptr, mmap_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
  close(fd);
  return mmap_data;
}

void FilePrivateMMap::munmapImpl(void* mmap_data, size_t mmap_size) {
  munmap(mmap_data, mmap_size);
}

StorageStrategy FilePrivateMMap::GetStorageStrategy() const {
  return StorageStrategy::kFilePrivate;
}

// FileSharedMMap implementation
FileSharedMMap::FileSharedMMap() : MMapContainer() {}

FileSharedMMap::~FileSharedMMap() {
  if (mmap_data_ && mmap_size_ > 0) {
    munmapImpl(mmap_data_, mmap_size_);
  }
}

void FileSharedMMap::Resize(size_t size) {
  if (size <= size_) {
    return;  // No need to resize if the new size is smaller or equal
  }
  // Unmap the old mapping
  munmapImpl(mmap_data_, mmap_size_);
  // Update the file size
  int fd = open(path_.c_str(), O_RDWR);
  if (fd == -1) {
    throw std::runtime_error("Failed to open file for resizing: " + path_);
  }
  if (ftruncate(fd, size) == -1) {
    close(fd);
    throw std::runtime_error("Failed to resize file: " + path_);
  }
  close(fd);
  // Create a new mapping with the updated file size
  mmap_data_ = mmapImpl(path_, size);
  if (mmap_data_ == MAP_FAILED) {
    throw std::runtime_error("Failed to mmap file after resizing: " + path_);
  }
  data_ = static_cast<char*>(mmap_data_) + sizeof(FileHeader);
  size_ = size - sizeof(FileHeader);
}

void* FileSharedMMap::mmapImpl(const std::string& path, size_t mmap_size) {
  int fd = open(path.c_str(), O_RDWR);
  if (fd == -1) {
    throw std::runtime_error("Failed to open file: " + path);
  }
  void* mmap_data =
      mmap(nullptr, mmap_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  close(fd);
  return mmap_data;
}

void FileSharedMMap::munmapImpl(void* mmap_data, size_t mmap_size) {
  munmap(mmap_data, mmap_size);
}

void FileSharedMMap::Sync() {
  unsigned char md5[MD5_DIGEST_LENGTH];
  MD5((unsigned char*) this->data_, this->size_, md5);
  if (memcmp(md5, reinterpret_cast<FileHeader*>(mmap_data_)->data_md5,
             MD5_DIGEST_LENGTH) != 0) {
    memcpy(reinterpret_cast<FileHeader*>(mmap_data_)->data_md5, md5,
           MD5_DIGEST_LENGTH);
    msync(mmap_data_, mmap_size_, MS_SYNC);
  }
}

StorageStrategy FileSharedMMap::GetStorageStrategy() const {
  return StorageStrategy::kFileShared;
}

}  // namespace neug