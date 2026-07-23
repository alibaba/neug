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
 * Readers pin via PinCurrentSnapshot/UnpinSnapshot (refcounted). Stale
 * slots are recycled when the last reader unpins.
 *
 * Transaction usage:
 * - Read/Insert: PinCurrentSnapshot() -> slot.view() -> UnpinSnapshot().
 *   InsertTransaction mutates the live slot in-place (timestamp-filtered).
 * - Update: CurrentSnapshot().Clone() -> mutate COW copy ->
 * PublishSnapshot().
 *
 * Concurrency:
 * - Lock-free PinCurrentSnapshot via optimistic pin + verify loop.
 * - Concurrent installs are NOT safe — VersionManager serializes
 *   updates via begin_update_commit (CAS 0→1), ensuring only one
 *   update/compact can be in progress at a time.
 * - PublishSnapshot publishes the new slot BEFORE VersionManager advances
 *   read_ts_, so readers never see "new ts + old slot".
 */
class GraphSnapshotStore {
 public:
  /// A slot holding a PropertyGraph, its GraphView, and a pin count.
  class SnapshotSlot {
   public:
    SnapshotSlot() = default;
    ~SnapshotSlot() = default;

    // Non-copyable, non-movable: slots live in a fixed-size vector and are
    // accessed exclusively by pointer/reference. The atomic reader_count_
    // also prevents implicit copy/move, but we state it explicitly for
    // clarity.
    SnapshotSlot(const SnapshotSlot&) = delete;
    SnapshotSlot& operator=(const SnapshotSlot&) = delete;
    SnapshotSlot(SnapshotSlot&&) = delete;
    SnapshotSlot& operator=(SnapshotSlot&&) = delete;

    /// Read-only view accessor.
    const GraphView& view() const { return view_; }
    /// Mutable view accessor (for InsertTransaction / AP write path).
    GraphView& mutable_view() { return view_; }
    /// Mutable PropertyGraph pointer (storage_.get() yields T* regardless
    /// of shared_ptr constness, so this works through const SnapshotSlot& too).
    PropertyGraph* mutable_graph() const { return storage_.get(); }

   private:
    friend class GraphSnapshotStore;
    std::shared_ptr<PropertyGraph> storage_;
    GraphView view_;
    std::atomic<int> reader_count_{0};
  };

  /// @param slot_num  Pool capacity (default 128).
  /// @param initial_pg Published into slot 0.
  explicit GraphSnapshotStore(int slot_num,
                              std::shared_ptr<PropertyGraph> initial_pg);

  ~GraphSnapshotStore();

  /// Pin the current slot via lock-free optimistic loop: load cur_slot_index_,
  /// fetch_add reader_count, verify index unchanged. Retries on concurrent
  /// PublishSnapshot or cleanup-in-progress. Caller must UnpinSnapshot().
  SnapshotSlot& PinCurrentSnapshot() noexcept;

  /// Unpin a slot. Cleans up and recycles if last reader on a stale slot.
  void UnpinSnapshot(const SnapshotSlot& slot) noexcept;

  /// Current PropertyGraph (for UpdateTransaction to Clone).
  /// No lock — VersionManager guarantees exclusive update access
  /// (update_state_==1, all inserters drained).
  const PropertyGraph& CurrentSnapshot() const;

  /// Publish a COW PropertyGraph into a free slot and switch cur_slot_index_.
  /// Steps: reserve free slot -> prep-pin new slot -> write PG + build view
  /// -> phantom-pin old slot -> switch (release store) -> release phantom pin
  /// -> release prep pin. Old slots are recycled lazily by UnpinSnapshot.
  /// Returns ERR_POOL_EXHAUSTED without touching @p new_pg on failure.
  Status PublishSnapshot(const std::shared_ptr<PropertyGraph>& new_pg);

  /// Pool capacity.
  int SlotCount() const { return slot_num_; }

  /// Best-effort check for a free slot. Used by Commit() to fail-fast
  /// before writing WAL. Stable under serialized Updates; `false` may
  /// become `true` asynchronously as readers unpin stale slots.
  bool HasFreeSlot() const {
    std::lock_guard<std::mutex> lock(free_list_mutex_);
    return !free_list_.empty();
  }

 private:
  int slot_num_;
  std::vector<SnapshotSlot> slots_;
  std::atomic<int> cur_slot_index_{0};
  std::vector<int> free_list_;
  mutable std::mutex free_list_mutex_;

  void initFreeList();
  int getFreeSlot();
  void returnFreeSlot(int slot_index);
  void UnpinSnapshotByIndex(int slot_index) noexcept;
  void cleanupSlot(int slot_index);
};

/**
 * @brief RAII guard for GraphSnapshotStore::PinCurrentSnapshot / UnpinSnapshot.
 *
 * Ensures the pinned slot is always released, even on exception paths.
 * Call release() to explicitly unpin early; the destructor is a no-op
 * after release().
 */
class SnapshotGuard {
 public:
  explicit SnapshotGuard(GraphSnapshotStore& store) noexcept
      : store_(&store), slot_(&store.PinCurrentSnapshot()) {}

  SnapshotGuard(GraphSnapshotStore& store,
                GraphSnapshotStore::SnapshotSlot& slot) noexcept
      : store_(&store), slot_(&slot) {}

  ~SnapshotGuard() noexcept {
    if (slot_) {
      store_->UnpinSnapshot(*slot_);
    }
  }

  SnapshotGuard(const SnapshotGuard&) = delete;
  SnapshotGuard& operator=(const SnapshotGuard&) = delete;

  SnapshotGuard(SnapshotGuard&& other) noexcept
      : store_(other.store_), slot_(other.slot_) {
    other.slot_ = nullptr;
  }

  SnapshotGuard& operator=(SnapshotGuard&& other) noexcept {
    if (this != &other) {
      if (slot_) {
        store_->UnpinSnapshot(*slot_);
      }
      store_ = other.store_;
      slot_ = other.slot_;
      other.slot_ = nullptr;
    }
    return *this;
  }

  GraphSnapshotStore::SnapshotSlot& get() { return *slot_; }
  const GraphSnapshotStore::SnapshotSlot& get() const { return *slot_; }

  bool valid() const { return slot_ != nullptr; }

  void release() noexcept {
    if (slot_) {
      store_->UnpinSnapshot(*slot_);
      slot_ = nullptr;
    }
  }

 private:
  GraphSnapshotStore* store_;
  GraphSnapshotStore::SnapshotSlot* slot_;
};

}  // namespace neug
