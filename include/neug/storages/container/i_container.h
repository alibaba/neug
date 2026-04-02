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

#include "neug/config.h"
#include "neug/utils/property/types.h"

namespace neug {

enum class ContainerType {
  kAnonMMap = 0,
  kAnonHugeMMap = 1,
  kFilePrivateMMap = 2,
  kFileSharedMMap = 3,
};

/**
 * @brief Interface for data containers with mmap-based storage.
 *
 * IDataContainer defines the contract for all data container implementations,
 * supporting various storage strategies (anonymous mmap, file-backed mmap,
 * etc.)
 */
class IDataContainer {
 public:
  IDataContainer() : data_(nullptr), size_(0) {}
  virtual ~IDataContainer() {}

  virtual ContainerType GetContainerType() const = 0;

  /**
   * @brief Get pointer to the data region.
   */
  inline void* GetData() const { return data_; }

  /**
   * @brief Get the size of the data region.
   */
  inline size_t GetDataSize() const { return size_; }

  /**
   * @brief Resize the container to accommodate at least the specified number of
   * elements.
   * @param size The new size in bytes.
   */
  virtual void Resize(size_t size) = 0;

  /**
   * @brief Get the file path (empty for anonymous mappings).
   */
  virtual std::string GetPath() const = 0;

  /**
   * @brief Open a file-backed container.
   */
  virtual void Open(const std::string& path) = 0;

  /**
   * @brief Synchronize changes to persistent storage.
   */
  virtual void Sync() = 0;

  /**
   * @brief Dump the container contents to a file.
   * @note This will close the container after writing.
   */
  virtual void Dump(const std::string& path) = 0;

  /**
   * @brief Close the container and release resources.
   */
  virtual void Close() = 0;

  /**
   * @brief Check if the data has been modified.
   */
  virtual bool IsDirty() = 0;

 protected:
  void* data_;
  size_t size_;
};

}  // namespace neug
