/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/index/index.h"
#include "neug/storages/module/module_broker.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace neug {

/**
 * @brief Manages all index instances in the storage layer.
 *
 * IndexManager owns a map of named indexes and provides lifecycle operations
 * (Open/Dump) that integrate with the Checkpoint framework.
 * Supports COW via Fork() for transaction isolation.
 */
class StorageIndexManager {
 public:
  StorageIndexManager() = default;
  ~StorageIndexManager() = default;

  /**
   * @brief Create a new index and register it.
   * @param name Unique index name.
   * @param meta Index metadata.
   * @return Pointer to the created index, or error.
   */
  neug::result<StorageIndex*> CreateIndex(const std::string& name,
                                          std::unique_ptr<IndexMeta> meta);

  /**
   * @brief Remove an index by name.
   */
  Status DropIndex(const std::string& name);

  /**
   * @brief Find indexes matching a label and one property name.
   */
  neug::result<std::vector<StorageIndex*>> GetIndex(
      label_t label_id, const std::string& property_name) const;

  StorageIndex* GetIndexByName(const std::string& name) const;

  /**
   * @brief Get all registered indexes.
   */
  neug::result<std::vector<StorageIndex*>> GetAllIndexes() const;

  /**
   * @brief Restore all indexes from a ModuleBroker + CheckpointManifest.
   *
   * Takes ownership of index modules (prefix "index_") from the broker.
   */
  void Open(std::shared_ptr<Checkpoint> ckp, ModuleBroker& store,
            MemoryLevel level);

  /**
   * @brief Persist all indexes to a checkpoint manifest.
   */
  void Dump(std::shared_ptr<Checkpoint> ckp, ModuleBroker& store,
            CheckpointManifest& meta);

  void Clear();

  /**
   * @brief COW: Fork each Index (shallow copy).
   */
  std::unique_ptr<StorageIndexManager> Clone() const;

  // 删除该接口
  // const auto& GetAllIndexEntries() const { return indexes_; }

  static bool IsIndexModule(const std::string& name);
  static std::string GetKey(const std::string& index_name);

 private:
  std::shared_ptr<Checkpoint> ckp_;
  MemoryLevel memory_level_{MemoryLevel::kInMemory};
  std::unordered_map<std::string, std::unique_ptr<StorageIndex>> indexes_;
};

}  // namespace neug
