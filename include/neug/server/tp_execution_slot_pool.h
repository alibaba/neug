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

#include <cstdlib>
#include <memory>
#include <new>
#include <utility>
#include <vector>

#include "neug/main/execution_slot.h"
#include "neug/main/wal_writer_set.h"

#include "bthread/bthread.h"

namespace neug {
class CheckpointCoordinator;
class NeugDBService;
class IWalWriter;

/**
 * @brief Pool of database slots for concurrent query execution.
 *
 * TpExecutionSlotPool owns and schedules a fixed set of ExecutionSlot
 * instances for TP query execution. Each aligned entry borrows a stable
 * database-owned per-slot WAL writer.
 *
 * TpExecutionSlotPool is used internally by NeugDBService. For most use cases,
 * access slots through NeugDBService::AcquireExecutionSlot() rather than
 * directly through the pool.
 *
 * **Key Features:**
 * - Owns service-local slots for query execution
 * - Thread-safe lease/release with bthread synchronization
 * - Stable WAL (Write-Ahead Log) writer per logical slot
 * - 4096-byte-aligned per-slot Entry storage
 *
 * **Pool Size:** `NeugDBConfig::max_thread_num` determines the pool size. Each
 * TP query leases one slot and one thread for its duration.
 *
 * @see NeugDBService for HTTP service wrapper
 * @see ExecutionSlotLease for RAII slot management
 * @since v0.1.0
 */
class TpExecutionSlotPool {
  static constexpr size_t kEntryAlignment = 4096;
  static constexpr size_t kSlotOffset = 128;

  // TP-only per-slot record. Implementation detail of the pool; external code
  // only ever sees ExecutionSlotLease.
  struct alignas(kEntryAlignment) Entry {
    Entry(GraphSnapshotStore& snapshot_store,
          std::shared_ptr<IGraphPlanner> planner,
          std::shared_ptr<execution::GlobalQueryCache> global_query_cache,
          std::shared_ptr<Allocator> alloc, IVersionManager& version_manager,
          CheckpointCoordinator& checkpoint_coordinator, int slot_id,
          ExtensionManager& extension_manager, IWalWriter& wal_writer,
          const NeugDBConfig& config)
        : allocator(std::move(alloc)),
          slot(snapshot_store, std::move(planner),
               std::move(global_query_cache), version_manager, *allocator,
               QueryExecutionStrategy::kTransactional, &wal_writer,
               checkpoint_coordinator, extension_manager, config, slot_id) {}

    std::shared_ptr<Allocator> allocator;
    char _padding0[kSlotOffset - sizeof(std::shared_ptr<Allocator>)];
    ExecutionSlot slot;
    char _padding2[(kEntryAlignment -
                    (kSlotOffset + sizeof(ExecutionSlot)) % kEntryAlignment) %
                   kEntryAlignment];
  };

  static_assert(alignof(Entry) == kEntryAlignment);
  static_assert(sizeof(Entry) == kEntryAlignment);

 public:
  explicit TpExecutionSlotPool(
      GraphSnapshotStore& snapshot_store,
      std::shared_ptr<IGraphPlanner> planner,
      std::shared_ptr<execution::GlobalQueryCache> global_query_cache,
      IVersionManager& version_manager,
      CheckpointCoordinator& checkpoint_coordinator,
      ExtensionManager& extension_manager,
      const std::vector<std::shared_ptr<Allocator>>& allocators,
      WalWriterSet& wal_writers, const NeugDBConfig& config)
      : entries_(nullptr), slot_num_(allocators.size()) {
    available_slot_ids_.reserve(slot_num_);
    entries_ = static_cast<Entry*>(
        aligned_alloc(kEntryAlignment, sizeof(Entry) * slot_num_));
    if (entries_ == nullptr) {
      throw std::bad_alloc();
    }

    size_t constructed_entries = 0;
    try {
      for (; constructed_entries < slot_num_; ++constructed_entries) {
        const auto slot_id = static_cast<int>(constructed_entries);
        new (&entries_[constructed_entries])
            Entry(snapshot_store, planner, global_query_cache,
                  allocators.at(constructed_entries), version_manager,
                  checkpoint_coordinator, slot_id, extension_manager,
                  wal_writers.WriterFor(constructed_entries), config);
      }
    } catch (...) {
      while (constructed_entries > 0) {
        auto& entry = entries_[--constructed_entries];
        entry.~Entry();
      }
      free(entries_);
      entries_ = nullptr;
      throw;
    }
    bthread_mutex_init(&mutex_, nullptr);
    bthread_cond_init(&cond_, nullptr);
    for (size_t i = 0; i < slot_num_; ++i) {
      available_slot_ids_.push_back(i);
    }
    LOG(INFO) << "Initializing TpExecutionSlotPool with " << slot_num_
              << " slots.";
  }

  ~TpExecutionSlotPool() {
    CHECK_EQ(available_slot_ids_.size(), slot_num_)
        << "All ExecutionSlotLease objects must be released before "
           "TpExecutionSlotPool destruction";
    if (entries_ != nullptr) {
      for (size_t slot_id = 0; slot_id < slot_num_; ++slot_id) {
        entries_[slot_id].~Entry();
      }
      free(entries_);
      entries_ = nullptr;
    }
    bthread_cond_destroy(&cond_);
    bthread_mutex_destroy(&mutex_);
  }

  /**
   * @brief Lease a slot from the pool. Blocks if no slot is available.
   * @return ExecutionSlotLease managing the leased slot. The slot is returned
   *         to the pool when the lease goes out of scope.
   */
  ExecutionSlotLease AcquireExecutionSlot();

  inline size_t ExecutionSlotNum() const { return slot_num_; }

  /**
   * @brief Get the total number of executed queries across all slots.
   *        Expect lock held by caller.
   * @return Total number of executed queries.
   */
  size_t getExecutedQueryNum() const {
    size_t ret = 0;
    for (size_t i = 0; i < slot_num_; ++i) {
      ret += entries_[i].slot.query_num();
    }
    return ret;
  }

 private:
  static void releaseExecutionSlot(void* owner, size_t slot_id) noexcept;

  friend class NeugDBService;

  Entry* entries_;
  size_t slot_num_;
  std::vector<size_t> available_slot_ids_;
  bthread_mutex_t mutex_;
  bthread_cond_t cond_;
};

}  // namespace neug
