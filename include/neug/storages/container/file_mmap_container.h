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
#pragma once

#include <glog/logging.h>
#include <filesystem>

#include "neug/storages/container/mmap_container.h"

namespace neug {

/**
 * @brief File-backed private memory-mapped container.
 *
 * FilePrivateMMap uses MAP_PRIVATE for file mappings, providing
 * copy-on-write semantics where modifications are not written back to the file.
 */
class FilePrivateMMap : public MMapContainer {
 public:
  FilePrivateMMap();
  ~FilePrivateMMap() override;
  ContainerType GetContainerType() const override {
    return ContainerType::kFilePrivateMMap;
  }

  /**
   * @brief Create an anonymous memory mapping (not file-backed).
   */
  void OpenAnonymous(size_t size);

 protected:
  void* mmapImpl(const std::string& path, size_t mmap_size) override;
  void munmapImpl(void* mmap_data, size_t mmap_size) override;
};

/**
 * @brief File-backed shared memory-mapped container.
 *
 * FileSharedMMap uses MAP_SHARED for file mappings, providing
 * direct write-through to the underlying file.
 */
class FileSharedMMap : public MMapContainer {
 public:
  FileSharedMMap();
  ~FileSharedMMap() override;
  ContainerType GetContainerType() const override {
    return ContainerType::kFileSharedMMap;
  }

  void Resize(size_t size) override;
  void Sync() override;

  /**
   * @brief Dump data to a file preserving sparse structure.
   *
   * Since FileSharedMMap writes directly to the backing file via MAP_SHARED,
   * we flush via Sync() and then delegate to file_utils::copy_file() which
   * tries in order:
   *   1. FICLONE ioctl (reflink/COW clone) - preserves sparse holes, zero copy
   *   2. copy_file_range() - server-side copy, may use COW on supported FS
   *   3. Traditional read/write fallback
   *
   * All three paths produce an independent inode, so a subsequent open() that
   * copies the snapshot back to a tmp file will never alias the source.
   * Falls back to the base-class fwrite-based Dump() if copy_file() throws.
   */
  void Dump(const std::string& path) override;

 protected:
  void* mmapImpl(const std::string& path, size_t mmap_size) override;
  void munmapImpl(void* mmap_data, size_t mmap_size) override;
};

}  // namespace neug