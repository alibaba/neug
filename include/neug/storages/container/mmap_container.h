
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

#include <cstddef>
#include <memory>
#include <string>

#include "neug/storages/container/i_container.h"

namespace neug {

/**
 * @brief Base class for memory-mapped data containers.
 *
 * MMapContainer provides common functionality for all mmap-based storage
 * implementations, including file I/O, dirty checking, and forking.
 */
class MMapContainer : public IDataContainer {
 public:
  MMapContainer();
  virtual ~MMapContainer() {}

  virtual void Resize(size_t size) override;
  std::string GetPath() const override;

  void Open(const std::string& path) override;
  void Close() override;
  void Dump(const std::string& path) override;
  virtual void Sync() override;
  bool IsDirty() override;
  std::unique_ptr<IDataContainer> Fork(Checkpoint& checkpoint,
                                       MemoryLevel level) override;

 protected:
  /**
   * @brief Implementation-specific mmap call.
   */
  virtual void* mmapImpl(const std::string& path, size_t mmap_size) = 0;

  /**
   * @brief Implementation-specific munmap call.
   */
  virtual void munmapImpl(void* mmap_data, size_t mmap_size) = 0;
  std::string path_;
  void* mmap_data_;
  size_t mmap_size_;
};

}  // namespace neug