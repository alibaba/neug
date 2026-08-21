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
#include <functional>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/utils/api.h"
#include "neug/utils/result.h"

namespace neug {

class ExecutionSlot;
class InPlaceWriteScope;
class Checkpoint;

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
 * - Update: CloneCurrentForUpdate() -> mutate COW copy -> PrepareSnapshot() ->
 *   PreparedSnapshot::Publish().
 *
 * Concurrency:
 * - Lock-free PinCurrentSnapshot via optimistic pin + verify loop.
 * - Concurrent installs are NOT safe — VersionManager serializes
 *   updates via its update-execution state, ensuring only one
 *   update/compact can be in progress at a time.
 * - PublishSnapshot publishes the new slot BEFORE VersionManager advances
 *   read_ts_, so readers never see "new ts + old slot".
 */
class NEUG_API GraphSnapshotStore {
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
    /// Mutable PropertyGraph accessor (for InsertTransaction / AP write path).
    PropertyGraph* mutable_graph() { return storage_.get(); }
    /// Snapshot publication generation carried by this slot incarnation.
    uint32_t snapshot_generation() const { return snapshot_generation_; }
    /// Plan-cache invalidation generation carried by this snapshot.
    uint64_t planning_generation() const {
      return planning_generation_.load(std::memory_order_acquire);
    }

   private:
    friend class GraphSnapshotStore;
    std::shared_ptr<PropertyGraph> storage_;
    GraphView view_;
    uint32_t snapshot_generation_{0};
    std::atomic<uint64_t> planning_generation_{0};
    std::atomic<int> reader_count_{0};
  };

  /**
   * @brief RAII owner of a prepared snapshot slot.
   *
   * Preparation reserves a pool slot and snapshot generation and builds the
   * GraphView. Publish() only switches the current slot and therefore cannot
   * fail. Destruction without Publish() discards the prepared slot and returns
   * it to the pool.
   */
  class PreparedSnapshot {
   public:
    ~PreparedSnapshot() noexcept;

    PreparedSnapshot(const PreparedSnapshot&) = delete;
    PreparedSnapshot& operator=(const PreparedSnapshot&) = delete;

    PreparedSnapshot(PreparedSnapshot&& other) noexcept;
    PreparedSnapshot& operator=(PreparedSnapshot&&) = delete;

    /// Consumes this preparation, installs its slot, and returns its
    /// generation.
    uint32_t Publish() && noexcept;

   private:
    friend class GraphSnapshotStore;

    PreparedSnapshot(GraphSnapshotStore& store, int slot_index) noexcept;

    GraphSnapshotStore* store_;
    int slot_index_;
  };

  /**
   * Callback-scoped capability for destructive in-place maintenance.
   *
   * GraphSnapshotStore owns the precondition checks and creates this context
   * only for the duration of WithCheckpointMaintenance(). The context keeps the
   * low-level mutable/reopen operations out of the public GraphSnapshotStore
   * API while preventing callers from taking ownership of the maintenance
   * window.
   */
  class CheckpointMaintenanceContext {
   public:
    CheckpointMaintenanceContext(const CheckpointMaintenanceContext&) = delete;
    CheckpointMaintenanceContext& operator=(
        const CheckpointMaintenanceContext&) = delete;
    CheckpointMaintenanceContext(CheckpointMaintenanceContext&&) = delete;
    CheckpointMaintenanceContext& operator=(CheckpointMaintenanceContext&&) =
        delete;

    PropertyGraph& MutableCurrentSnapshot();
    void ReopenCurrentGraphFromCheckpoint(
        std::shared_ptr<Checkpoint> checkpoint, MemoryLevel memory_level);

    /// Rebuild pointer-based views after selective in-place module replacement.
    /// Returns the unchanged snapshot generation used by transaction publish.
    uint32_t RefreshCurrentView(bool planning_changed);

   private:
    friend class GraphSnapshotStore;

    explicit CheckpointMaintenanceContext(SnapshotSlot& slot) noexcept;

    SnapshotSlot& slot_;
  };

  using CheckpointMaintenanceFn =
      std::function<Status(CheckpointMaintenanceContext&)>;

  /// @param slot_num  Pool capacity (default 128).
  /// @param initial_pg Published into slot 0.
  explicit GraphSnapshotStore(int slot_num,
                              std::shared_ptr<PropertyGraph> initial_pg,
                              uint32_t initial_snapshot_generation = 0);

  ~GraphSnapshotStore();

  /// Pin the current slot via lock-free optimistic loop: load cur_slot_index_,
  /// increment a positive reader_count with CAS, then verify the index is
  /// unchanged. Retries on concurrent publication or cleanup-in-progress.
  /// Caller must UnpinSnapshot().
  SnapshotSlot& PinCurrentSnapshot() noexcept;

  /// Unpin a slot. Cleans up and recycles if last reader on a stale slot.
  void UnpinSnapshot(const SnapshotSlot& slot) noexcept;

  /// Current PropertyGraph. The caller must keep the current slot stable
  /// through writer admission while using the returned reference.
  const PropertyGraph& CurrentSnapshot() const noexcept;

  /// Reserve and build a pending snapshot tagged with @p planning_generation.
  /// Returns ERR_POOL_EXHAUSTED without touching @p new_pg on failure.
  /// Readers retain this tag for the lifetime of their pinned slot.
  result<PreparedSnapshot> PrepareSnapshot(
      const std::shared_ptr<PropertyGraph>& new_pg,
      uint64_t planning_generation);

  /// Pool capacity.
  int SlotCount() const { return slot_num_; }

  /// Best-effort diagnostic used by lifecycle and pool-reclamation tests.
  /// `false` may become `true` asynchronously as readers unpin stale slots.
  bool HasFreeSlot() const {
    std::lock_guard<std::mutex> lock(free_list_mutex_);
    return !free_list_.empty();
  }

  /// Clone the pinned current graph and capture its planning generation for an
  /// update transaction. The caller must hold update admission to exclude
  /// concurrent in-place writers.
  std::pair<std::shared_ptr<PropertyGraph>, uint64_t> CloneCurrentForUpdate();

  /**
   * Run checkpoint maintenance after external quiescence has been acquired.
   *
   * The caller must already guarantee transaction quiescence through the
   * database lifecycle or an exclusive VersionManager state. This method
   * verifies that the current slot has no ordinary pins and that every
   * non-current slot has already been reclaimed, then invokes @p fn with a
   * callback-scoped context for the maintenance-only operations.
   */
  Status WithCheckpointMaintenance(CheckpointMaintenanceFn fn);

 private:
  friend class InPlaceWriteScope;

  int slot_num_;
  std::vector<SnapshotSlot> slots_;
  std::atomic<int> cur_slot_index_{0};
  std::atomic<uint32_t> last_reserved_snapshot_generation_{0};
  std::vector<int> free_list_;
  mutable std::mutex free_list_mutex_;

  void initFreeList();
  int getFreeSlot();
  void returnFreeSlot(int slot_index);
  uint32_t reserveSnapshotGeneration();
  uint32_t publishInPlaceMutation(SnapshotSlot& mutated_slot,
                                  bool planning_changed) noexcept;
  void publishPreparedSnapshot(int slot_index) noexcept;
  void unpinSnapshotByIndex(int slot_index) noexcept;
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
