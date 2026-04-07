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

#include "neug/storages/container/mmap_container.h"

namespace neug {

/**
 * @brief Anonymous memory-mapped container using regular pages.
 *
 * AnonMMap uses MAP_PRIVATE | MAP_ANONYMOUS for memory allocation,
 * providing copy-on-write semantics for file-backed opens.
 */
class AnonMMap : public MMapContainer {
 public:
  AnonMMap();
  ~AnonMMap() override;
  ContainerType GetContainerType() const override {
    return ContainerType::kAnonMMap;
  }

  /**
   * @brief Create an anonymous memory mapping of the specified size.
   */
  void OpenAnonymous(size_t size);

 protected:
  void* mmapImpl(const std::string& path, size_t mmap_size) override;
  void munmapImpl(void* mmap_data, size_t mmap_size) override;
};

/**
 * @brief Anonymous memory-mapped container using huge pages.
 *
 * AnonHugeMMap provides better TLB performance for large allocations
 * by using huge pages (typically 2MB or 1GB).
 */
class AnonHugeMMap : public MMapContainer {
 public:
  AnonHugeMMap();
  ~AnonHugeMMap() override;
  ContainerType GetContainerType() const override {
    return ContainerType::kAnonHugeMMap;
  }

  /**
   * @brief Create an anonymous huge page mapping of the specified size.
   */
  void OpenAnonymous(size_t size);

  void Resize(size_t size) override;

 protected:
  void* mmapImpl(const std::string& path, size_t mmap_size) override;
  void munmapImpl(void* mmap_data, size_t mmap_size) override;
};

}  // namespace neug