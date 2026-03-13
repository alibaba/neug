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

#include "neug/storages/column/mmap_container.h"

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

  void Resize(size_t size) override;
  void* mmapImpl(const std::string& path, size_t mmap_size) override;
  void munmapImpl(void* mmap_data, size_t mmap_size) override;
  MemoryLevel GetStorageStrategy() const override;
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
  void* mmapImpl(const std::string& path, size_t mmap_size) override;
  void munmapImpl(void* mmap_data, size_t mmap_size) override;
  void Sync() override;
  MemoryLevel GetStorageStrategy() const override;
};

}  // namespace neug