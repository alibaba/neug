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

#include <filesystem>
#include "neug/storages/container/file_header.h"
#include "neug/storages/container/file_mmap_container.h"
#include "neug/utils/file_utils.h"

namespace neug {

FilePrivateMMap::FilePrivateMMap() : MMapContainer() {}

FilePrivateMMap::~FilePrivateMMap() { Close(); }

void FilePrivateMMap::OpenAnonymous(size_t size) {
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

void* FilePrivateMMap::mmapImpl(const std::string& path, size_t mmap_size) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd == -1) {
    THROW_RUNTIME_ERROR("Failed to open file: " + path);
  }
  void* mmap_data =
      mmap(nullptr, mmap_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
  close(fd);
  return mmap_data;
}

void FilePrivateMMap::munmapImpl(void* mmap_data, size_t mmap_size) {
  munmap(mmap_data, mmap_size);
}

FileSharedMMap::FileSharedMMap() : MMapContainer() {}

FileSharedMMap::~FileSharedMMap() { Close(); }

void FileSharedMMap::Resize(size_t size) {
  if (size == size_) {
    return;
  }
  size_t real_size = size + sizeof(FileHeader);
  if (mmap_data_ && mmap_size_ > 0) {
    munmapImpl(mmap_data_, mmap_size_);
  }
  mmap_data_ = nullptr;
  mmap_size_ = 0;
  data_ = nullptr;
  size_ = 0;

  int fd = open(path_.c_str(), O_RDWR);
  if (fd == -1) {
    THROW_RUNTIME_ERROR("Failed to open file for resizing: " + path_);
  }
  if (ftruncate(fd, real_size) == -1) {
    close(fd);
    THROW_RUNTIME_ERROR("Failed to resize file: " + path_);
  }
  close(fd);
  mmap_size_ = std::filesystem::file_size(path_);
  assert(mmap_size_ == real_size);
  // Create a new mapping with the updated file size
  mmap_data_ = mmapImpl(path_, mmap_size_);
  if (mmap_data_ == MAP_FAILED) {
    THROW_RUNTIME_ERROR("Failed to mmap file after resizing: " + path_);
  }
  data_ = static_cast<char*>(mmap_data_) + sizeof(FileHeader);
  size_ = mmap_size_ - sizeof(FileHeader);
}

void* FileSharedMMap::mmapImpl(const std::string& path, size_t mmap_size) {
  int fd = open(path.c_str(), O_RDWR);
  if (fd == -1) {
    THROW_RUNTIME_ERROR("Failed to open file: " + path);
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
  if (mmap_data_ == nullptr || data_ == nullptr || size_ == 0) {
    return;
  }
  unsigned char md5[MD5_DIGEST_LENGTH];
  MD5((unsigned char*) this->data_, this->size_, md5);
  if (memcmp(md5, reinterpret_cast<FileHeader*>(mmap_data_)->data_md5,
             MD5_DIGEST_LENGTH) != 0) {
    memcpy(reinterpret_cast<FileHeader*>(mmap_data_)->data_md5, md5,
           MD5_DIGEST_LENGTH);
    msync(mmap_data_, mmap_size_, MS_SYNC);
  }
}

void FileSharedMMap::Dump(const std::string& path) {
  // If there is no backing file, fall back to the fwrite-based base Dump().
  if (path_.empty()) {
    MMapContainer::Dump(path);
    return;
  }

  // Flush the MD5 header and all dirty pages to the backing file.
  Sync();

  if (path == path_) {
    // Target is the same file; Sync() already ensured it is up-to-date.
    return;
  }

  // Use file_utils::copy_file which tries FICLONE (reflink/COW) first,
  // then copy_file_range, then falls back to traditional read/write copy.
  // All three paths produce an independent inode, so a subsequent open()
  // that copies the snapshot back to a tmp file will never alias the source.
  try {
    file_utils::copy_file(path_, path, /*overwrite=*/true);
  } catch (const std::exception& e) {
    LOG(WARNING) << "copy_file failed (" << e.what()
                 << "), falling back to fwrite for " << path;
    MMapContainer::Dump(path);
  }
  Close();
}

}  // namespace neug