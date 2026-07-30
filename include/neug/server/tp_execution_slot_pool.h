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
#include <string>
#include <utility>
#include <vector>

#include "neug/main/execution_slot.h"
#include "neug/transaction/wal/wal.h"

#include "bthread/bthread.h"

namespace neug {
class NeugDBService;

/**
 * @brief Pool of database slots for concurrent query execution.
 *
 * TpExecutionSlotPool owns and schedules a fixed set of ExecutionSlot
 * instances for TP query execution. Each aligned entry owns both its slot and
 * per-slot WAL writer.
 *
 * TpExecutionSlotPool is used internally by NeugDBService. For most use cases,
 * access slots through NeugDBService::AcquireExecutionSlot() rather than
 * directly through the pool.
 *
 * **Key Features:**
 * - Owns service-local slots for query execution
 * - Thread-safe lease/release with bthread synchronization
 * - Automatic WAL (Write-Ahead Log) management per slot
 * - 4096-byte-aligned per-slot Entry storage
 *
 * **Pool Size:** Determined by `NeugDBConfig::max_thread_num`, typically
 * matching the number of concurrent request handlers.
 *
 * @see NeugDBService for HTTP service wrapper
 * @see ExecutionSlotLease for RAII slot management
 * @since v0.1.0
 */
class TpExecutionSlotPool {
  // TP-only per-slot record. Implementation detail of the pool; external code
  // only ever sees ExecutionSlotLease.
  struct alignas(4096) Entry {
    Entry(GraphSnapshotStore& snapshot_store,
          std::shared_ptr<IGraphPlanner> planner,
          std::shared_ptr<execution::GlobalQueryCache> global_query_cache,
          std::shared_ptr<Allocator> alloc, IVersionManager& version_manager,
          int slot_id, std::unique_ptr<IWalWriter> in_logger,
          const NeugDBConfig& config)
        : allocator(std::move(alloc)),
          logger(std::move(in_logger)),
          slot(snapshot_store, std::move(planner),
               std::move(global_query_cache), version_manager, *allocator,
               QueryExecutionStrategy::kTransactional, logger.get(), config,
               slot_id) {
      CHECK(logger != nullptr);
      logger->open();
    }

    std::shared_ptr<Allocator> allocator;
    char _padding0[128 - sizeof(std::shared_ptr<Allocator>)];
    // Declaration order is intentional: slot is destroyed before the WAL
    // writer it references.
    std::unique_ptr<IWalWriter> logger;
    char _padding1[4096 - sizeof(std::unique_ptr<IWalWriter>) -
                   sizeof(std::shared_ptr<Allocator>) - sizeof(_padding0)];
    ExecutionSlot slot;
    char _padding2[(4096 - sizeof(ExecutionSlot) % 4096) % 4096];
  };

  static_assert(alignof(Entry) == 4096);
  static_assert(sizeof(Entry) % 4096 == 0);

 public:
  explicit TpExecutionSlotPool(
      GraphSnapshotStore& snapshot_store,
      std::shared_ptr<IGraphPlanner> planner,
      std::shared_ptr<execution::GlobalQueryCache> global_query_cache,
      IVersionManager& version_manager,
      const std::vector<std::shared_ptr<Allocator>>& allocators,
      const std::string& wal_uri, const NeugDBConfig& config)
      : entries_(nullptr), slot_num_(allocators.size()) {
    WalWriterFactory::Init();
    available_slot_ids_.reserve(slot_num_);
    entries_ =
        static_cast<Entry*>(aligned_alloc(4096, sizeof(Entry) * slot_num_));
    if (entries_ == nullptr) {
      throw std::bad_alloc();
    }

    size_t constructed_entries = 0;
    try {
      for (; constructed_entries < slot_num_; ++constructed_entries) {
        const auto slot_id = static_cast<int>(constructed_entries);
        auto logger = WalWriterFactory::CreateWalWriter(wal_uri, slot_id);
        new (&entries_[constructed_entries])
            Entry(snapshot_store, planner, global_query_cache,
                  allocators.at(constructed_entries), version_manager, slot_id,
                  std::move(logger), config);
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
