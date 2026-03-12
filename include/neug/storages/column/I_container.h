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

/**
 * @brief Interface for data containers with mmap-based storage.
 *
 * IDataContainer defines the contract for all data container implementations,
 * supporting various storage strategies (anonymous mmap, file-backed mmap,
 * etc.)
 */
class IDataContainer {
 public:
  virtual ~IDataContainer() {}

  /**
   * @brief Get pointer to the data region.
   */
  virtual void* GetData() = 0;

  /**
   * @brief Get the size of the data region.
   */
  virtual size_t GetDataSize() = 0;

  /**
   * @brief Get the file path (empty for anonymous mappings).
   */
  virtual std::string GetPath() = 0;

  /**
   * @brief Get the storage strategy used by this container.
   */
  virtual StorageStrategy GetStorageStrategy() const = 0;

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

  /**
   * @brief Create a fork (copy) of this container.
   */
  virtual std::unique_ptr<IDataContainer> Fork(Checkpoint& checkpoint,
                                               StorageStrategy strategy) = 0;
};
