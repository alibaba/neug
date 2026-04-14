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

#include <stddef.h>
#include <stdint.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/utils/result.h"

namespace neug {

/**
 * @brief Fixed-size slot pool for MVCC PropertyGraph snapshots.
 *
 * Maintains `slot_num` slots. `cur_slot_index_` marks the active slot.
 * Readers pin via acquireSnapshot/releaseSnapshot (refcounted). Stale
 * slots are recycled when the last reader unpins.
 *
 * Transaction usage:
 * - Read/Insert: acquireSnapshot() -> slot.view() -> releaseSnapshot().
 *   InsertTransaction mutates the live slot in-place (timestamp-filtered).
 * - Update: currentSnapshot().Fork() -> mutate COW copy -> installSnapshot().
 *
 * Concurrency:
 * - Lock-free acquireSnapshot via optimistic pin + verify loop.
 * - Concurrent installs are NOT safe — SLVersionManager serializes
 *   updates via start_commit_update_timestamp (CAS 0→1), ensuring only one
 *   update/compact can be in progress at a time.
 * - installSnapshot publishes the new slot BEFORE VersionManager advances
 *   read_ts_, so readers never see "new ts + old slot".
 */
class StorageStore {
 public:
  /// A slot holding a PropertyGraph, its GraphView, and a pin count.
  class StorageSlot {
   public:
    StorageSlot() = default;
    ~StorageSlot() = default;

    // Non-copyable, non-movable: slots live in a fixed-size vector and are
    // accessed exclusively by pointer/reference. The atomic reader_count_
    // also prevents implicit copy/move, but we state it explicitly for
    // clarity.
    StorageSlot(const StorageSlot&) = delete;
    StorageSlot& operator=(const StorageSlot&) = delete;
    StorageSlot(StorageSlot&&) = delete;
    StorageSlot& operator=(StorageSlot&&) = delete;

    /// Read-only view accessor.
    const GraphView& view() const { return view_; }
    /// Mutable view accessor (for InsertTransaction / AP write path).
    GraphView& mutable_view() { return view_; }
    /// Mutable PropertyGraph pointer (storage_.get() yields T* regardless
    /// of shared_ptr constness, so this works through const StorageSlot& too).
    PropertyGraph* pg() const { return storage_.get(); }

   private:
    friend class StorageStore;
    std::shared_ptr<PropertyGraph> storage_;
    GraphView view_;
    std::atomic<int> reader_count_{0};
  };

  /// @param slot_num  Pool capacity (default 128).
  /// @param initial_pg Installed into slot 0.
  explicit StorageStore(int slot_num,
                        std::shared_ptr<PropertyGraph> initial_pg);

  ~StorageStore();

  /// Pin the current slot via lock-free optimistic loop: load cur_slot_index_,
  /// fetch_add reader_count, verify index unchanged. Retries on concurrent
  /// installSnapshot or cleanup-in-progress. Caller must releaseSnapshot().
  StorageSlot& acquireSnapshot();

  /// Unpin a slot. Cleans up and recycles if last reader on a stale slot.
  void releaseSnapshot(const StorageSlot& slot);

  /// Current PropertyGraph (for UpdateTransaction to Fork).
  /// No lock — SLVersionManager guarantees exclusive update access
  /// (update_state_==1, all inserters drained).
  const PropertyGraph& currentSnapshot() const;

  /// Publish a COW PropertyGraph into a free slot and switch cur_slot_index_.
  /// Steps: reserve free slot -> prep-pin new slot -> write PG + build view
  /// -> phantom-pin old slot -> switch (release store) -> release phantom pin
  /// -> release prep pin. Old slots are recycled lazily by releaseSnapshot.
  /// Returns ERR_POOL_EXHAUSTED without touching @p new_pg on failure.
  Status installSnapshot(const std::shared_ptr<PropertyGraph>& new_pg);

  /// Pool capacity.
  int slotNum() const { return slot_num_; }

  /// Best-effort check for a free slot. Used by Commit() to fail-fast
  /// before writing WAL. Stable under serialized Updates; `false` may
  /// become `true` asynchronously as readers unpin stale slots.
  bool hasFreeSlot() const {
    std::lock_guard<std::mutex> lock(free_list_mutex_);
    return !free_list_.empty();
  }

 private:
  int slot_num_;
  std::vector<StorageSlot> slots_;
  std::atomic<int> cur_slot_index_{0};
  std::vector<int> free_list_;
  mutable std::mutex free_list_mutex_;

  void initFreeList();
  int getFreeSlot();
  void returnFreeSlot(int slot_index);
  void releaseSnapshotByIndex(int slot_index);
  void cleanupSlot(int slot_index);
};

/**
 * @brief RAII guard for StorageStore::acquireSnapshot / releaseSnapshot.
 *
 * Ensures the pinned slot is always released, even on exception paths.
 * Call release() to explicitly unpin early; the destructor is a no-op
 * after release().
 */
class SlotGuard {
 public:
  explicit SlotGuard(StorageStore& store)
      : store_(&store), slot_(&store.acquireSnapshot()) {}

  SlotGuard(StorageStore& store, StorageStore::StorageSlot& slot)
      : store_(&store), slot_(&slot) {}

  ~SlotGuard() {
    if (slot_) {
      store_->releaseSnapshot(*slot_);
    }
  }

  SlotGuard(const SlotGuard&) = delete;
  SlotGuard& operator=(const SlotGuard&) = delete;

  SlotGuard(SlotGuard&& other) noexcept
      : store_(other.store_), slot_(other.slot_) {
    other.slot_ = nullptr;
  }

  SlotGuard& operator=(SlotGuard&& other) noexcept {
    if (this != &other) {
      if (slot_) {
        store_->releaseSnapshot(*slot_);
      }
      store_ = other.store_;
      slot_ = other.slot_;
      other.slot_ = nullptr;
    }
    return *this;
  }

  StorageStore::StorageSlot& get() { return *slot_; }
  const StorageStore::StorageSlot& get() const { return *slot_; }

  bool valid() const { return slot_ != nullptr; }

  void release() {
    if (slot_) {
      store_->releaseSnapshot(*slot_);
      slot_ = nullptr;
    }
  }

 private:
  StorageStore* store_;
  StorageStore::StorageSlot* slot_;
};

}  // namespace neug
