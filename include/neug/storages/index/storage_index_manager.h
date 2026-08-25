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
#include <unordered_set>
#include <vector>

#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/index/storage_index.h"
#include "neug/storages/module/module_broker.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace neug {

class PropertyGraph;

struct PendingIndex {
  // A persisted index has checkpoint modules to reopen. A created index was
  // reconstructed from CREATE INDEX WAL and still needs BulkBuild.
  enum class State {
    kPersisted,
    kCreated,
  };

  std::string key;
  ModuleDescriptor descriptor;
  IndexMeta meta;
  State state{State::kPersisted};
};

/**
 * @brief Manages all index instances in the storage layer.
 *
 * IndexManager owns a map of named indexes and provides lifecycle operations
 * (Open/Dump) that integrate with the Checkpoint framework.
 * Supports COW via Fork() for transaction isolation.
 */
class StorageIndexManager {
 public:
  using PendingIndex = neug::PendingIndex;
  using IndexColumns =
      std::unordered_map<label_t,
                         std::unordered_map<std::string, const ColumnBase*>>;
  using IndexVertexSets = std::unordered_map<label_t, VertexSet>;

  struct PendingIndexMutation {
    enum class Type { kInsert, kUpdate, kDelete };

    Type type;
    label_t label_id;
    vid_t vid;
    std::vector<std::pair<std::string, Value>> properties;
  };

  StorageIndexManager() = default;
  ~StorageIndexManager() = default;

  /**
   * @brief Create a new index and register it.
   * @param meta Index metadata.
   * @param index_id_accessor Index ID mapping strategy.
   * @param column Property column bound to the index.
   * @param vertex_set Existing vertices used to populate the index.
   * @return The built index or its pending representation, or an error.
   */
  neug::result<CreatedIndex> CreateIndex(
      std::unique_ptr<IndexMeta> meta,
      std::unique_ptr<IndexIDAccessor> index_id_accessor,
      const ColumnBase* column, const VertexSet& vertex_set,
      bool required = true);

  /**
   * @brief Remove an index by name.
   */
  Status DropIndex(const std::string& name);

  /**
   * @brief Find indexes matching a label and one property name.
   */
  neug::result<std::vector<StorageIndex*>> GetIndex(
      label_t label_id, const std::string& property_name) const;

  /// Return indexes that are about to be mutated and mark them dirty for
  /// incremental checkpoint persistence.
  neug::result<std::vector<StorageIndex*>> GetIndexForUpdate(
      label_t label_id, const std::string& property_name);

  bool HasPendingIndex(label_t label_id) const;
  bool HasPendingIndex(label_t label_id,
                       const std::string& property_name) const;
  neug::result<std::vector<PendingIndex*>> GetPendingIndex(
      label_t label_id, const std::string& property_name);
  result<PendingIndex*> GetPendingIndexByName(const std::string& name);
  result<std::vector<const PendingIndex*>> GetAllPendingIndexes() const;

  void RecordPendingInsert(
      label_t label_id, vid_t vertex_id,
      std::vector<std::pair<std::string, Value>> properties);
  void RecordPendingUpdate(label_t label_id, vid_t vertex_id,
                           std::string property_name, Value value);
  void RecordPendingDelete(label_t label_id, vid_t vertex_id);

  neug::result<StorageIndex*> GetIndexByName(const std::string& name) const;

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

  result<size_t> ActivateIndexes(const IndexColumns& columns,
                                 const IndexVertexSets& vertex_sets);

  bool HasPendingIndexes() const { return !pending_indexes_.empty(); }
  bool HasPendingMutations() const { return !pending_mutations_.empty(); }

  /**
   * @brief Move all active indexes into the module broker for persistence.
   */
  void Dump(ModuleBroker& store, CheckpointManifest& meta);

  void Clear();

  /**
   * @brief COW: Fork each Index (shallow copy).
   */
  std::unique_ptr<StorageIndexManager> Clone() const;

  static bool IsIndexModule(const std::string& name);
  static std::string GetKey(const std::string& index_name);

 private:
  friend class PropertyGraph;

  void StageIncrementalModules(ModuleBroker& store, CheckpointManifest& meta);
  Status ValidateCheckpointPreconditions() const;
  void InstallIncrementalCheckpoint(std::shared_ptr<Checkpoint> ckp);
  bool HasCatalogChanges() const { return catalog_dirty_; }
  bool HasCheckpointChanges() const {
    return catalog_dirty_ || !dirty_index_names_.empty();
  }

  std::shared_ptr<Checkpoint> ckp_;
  MemoryLevel memory_level_{MemoryLevel::kInMemory};
  std::unordered_map<std::string, std::unique_ptr<StorageIndex>> indexes_;
  std::unordered_map<std::string, PendingIndex> pending_indexes_;
  std::vector<std::shared_ptr<const PendingIndexMutation>> pending_mutations_;
  std::unordered_set<std::string> dirty_index_names_;
  bool catalog_dirty_{false};
};

}  // namespace neug
