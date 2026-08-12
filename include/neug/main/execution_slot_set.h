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
#include <vector>

#include "neug/main/execution_slot.h"

namespace neug {

class CheckpointCoordinator;
class GraphSnapshotStore;
class IGraphPlanner;
class IVersionManager;
class WalWriterSet;
struct NeugDBConfig;

/** @brief Database-owned transactional execution slots. */
class ExecutionSlotSet {
  struct alignas(4096) Entry {
    Entry(GraphSnapshotStore& snapshot_store,
          std::shared_ptr<IGraphPlanner> planner,
          std::shared_ptr<execution::GlobalQueryCache> global_query_cache,
          IVersionManager& version_manager, Allocator& allocator,
          WalWriterSet& wal_writers,
          CheckpointCoordinator& checkpoint_coordinator,
          const NeugDBConfig& config, size_t slot_id);

    ExecutionSlot slot;
    char padding[(4096 - sizeof(ExecutionSlot) % 4096) % 4096];
  };

  static_assert(alignof(Entry) == 4096);
  static_assert(sizeof(Entry) % 4096 == 0);

 public:
  ExecutionSlotSet(
      GraphSnapshotStore& snapshot_store,
      std::shared_ptr<IGraphPlanner> planner,
      std::shared_ptr<execution::GlobalQueryCache> global_query_cache,
      IVersionManager& version_manager,
      const std::vector<std::shared_ptr<Allocator>>& allocators,
      WalWriterSet& wal_writers, CheckpointCoordinator& checkpoint_coordinator,
      const NeugDBConfig& config);
  ~ExecutionSlotSet() noexcept;

  ExecutionSlotSet(const ExecutionSlotSet&) = delete;
  ExecutionSlotSet& operator=(const ExecutionSlotSet&) = delete;

  ExecutionSlot& At(size_t slot_id);
  const ExecutionSlot& At(size_t slot_id) const;
  size_t Size() const noexcept { return slot_num_; }
  size_t ExecutedQueryNum() const noexcept;

 private:
  Entry* entries_{nullptr};
  size_t slot_num_{0};
};

}  // namespace neug
