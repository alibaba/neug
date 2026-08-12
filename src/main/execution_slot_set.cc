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

#include "neug/main/execution_slot_set.h"

#include <glog/logging.h>

#include <cstdlib>
#include <new>
#include <utility>

#include "neug/main/checkpoint_coordinator.h"
#include "neug/main/wal_writer_set.h"
#include "neug/storages/allocators.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/version_manager.h"

namespace neug {

ExecutionSlotSet::Entry::Entry(
    GraphSnapshotStore& snapshot_store, std::shared_ptr<IGraphPlanner> planner,
    std::shared_ptr<execution::GlobalQueryCache> global_query_cache,
    IVersionManager& version_manager, Allocator& allocator,
    WalWriterSet& wal_writers, CheckpointCoordinator& checkpoint_coordinator,
    const NeugDBConfig& config, size_t slot_id)
    : slot(snapshot_store, std::move(planner), std::move(global_query_cache),
           version_manager, allocator, QueryExecutionStrategy::kTransactional,
           wal_writers.WriterFor(slot_id), checkpoint_coordinator, config,
           static_cast<int>(slot_id)) {}

ExecutionSlotSet::ExecutionSlotSet(
    GraphSnapshotStore& snapshot_store, std::shared_ptr<IGraphPlanner> planner,
    std::shared_ptr<execution::GlobalQueryCache> global_query_cache,
    IVersionManager& version_manager,
    const std::vector<std::shared_ptr<Allocator>>& allocators,
    WalWriterSet& wal_writers, CheckpointCoordinator& checkpoint_coordinator,
    const NeugDBConfig& config)
    : slot_num_(allocators.size()) {
  CHECK_EQ(slot_num_, wal_writers.SlotNum());
  entries_ =
      static_cast<Entry*>(aligned_alloc(4096, sizeof(Entry) * slot_num_));
  if (entries_ == nullptr) {
    throw std::bad_alloc();
  }

  size_t constructed = 0;
  try {
    for (; constructed < slot_num_; ++constructed) {
      new (&entries_[constructed])
          Entry(snapshot_store, planner, global_query_cache, version_manager,
                *allocators.at(constructed), wal_writers,
                checkpoint_coordinator, config, constructed);
    }
  } catch (...) {
    while (constructed > 0) {
      entries_[--constructed].~Entry();
    }
    free(entries_);
    entries_ = nullptr;
    throw;
  }
}

ExecutionSlotSet::~ExecutionSlotSet() noexcept {
  if (entries_ != nullptr) {
    for (size_t slot_id = 0; slot_id < slot_num_; ++slot_id) {
      entries_[slot_id].~Entry();
    }
    free(entries_);
  }
}

ExecutionSlot& ExecutionSlotSet::At(size_t slot_id) {
  CHECK_LT(slot_id, slot_num_);
  return entries_[slot_id].slot;
}

const ExecutionSlot& ExecutionSlotSet::At(size_t slot_id) const {
  CHECK_LT(slot_id, slot_num_);
  return entries_[slot_id].slot;
}

size_t ExecutionSlotSet::ExecutedQueryNum() const noexcept {
  size_t total = 0;
  for (size_t slot_id = 0; slot_id < slot_num_; ++slot_id) {
    total += entries_[slot_id].slot.query_num();
  }
  return total;
}

}  // namespace neug
