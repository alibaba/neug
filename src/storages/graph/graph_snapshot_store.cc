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

#include "neug/storages/graph_snapshot_store.h"

#include <glog/logging.h>
#include <limits>
#include <utility>

#include "neug/generated/proto/plan/error.pb.h"
#include "neug/utils/exception/exception.h"

namespace neug {

static constexpr int kCleanupSentinel = -(1 << 20);

GraphSnapshotStore::PreparedSnapshot::PreparedSnapshot(
    GraphSnapshotStore& store, int slot_index) noexcept
    : store_(&store), slot_index_(slot_index) {}

GraphSnapshotStore::PreparedSnapshot::~PreparedSnapshot() noexcept {
  if (store_ != nullptr) {
    store_->UnpinSnapshotByIndex(slot_index_);
  }
}

GraphSnapshotStore::PreparedSnapshot::PreparedSnapshot(
    PreparedSnapshot&& other) noexcept
    : store_(std::exchange(other.store_, nullptr)),
      slot_index_(other.slot_index_) {}

uint32_t GraphSnapshotStore::PreparedSnapshot::Publish() && noexcept {
  CHECK(store_ != nullptr);
  auto* store = std::exchange(store_, nullptr);
  const uint32_t generation = store->slots_[slot_index_].snapshot_generation_;
  store->publishPreparedSnapshot(slot_index_);
  return generation;
}

GraphSnapshotStore::GraphSnapshotStore(
    int slot_num, std::shared_ptr<PropertyGraph> initial_pg,
    uint32_t initial_snapshot_generation)
    : slot_num_(slot_num),
      slots_(slot_num),
      last_reserved_snapshot_generation_(initial_snapshot_generation) {
  // Publish initial PG into slot 0.
  //
  // Invariant: while a slot is current, reader_count_ >= 1 (held by the
  // "cur-pin"). A prepared publication transfers the cur-pin from the old slot
  // to the new slot atomically around the cur_slot_index_ switch. This lets
  // PinCurrentSnapshot safely roll back when old_count == 0 (slot is free or
  // being cleaned up) without mistaking a live cur slot for a dead one,
  // because a live cur slot always has count >= 1.
  slots_[0].storage_ = std::move(initial_pg);
  slots_[0].view_ = GraphView(*slots_[0].storage_);
  slots_[0].snapshot_generation_ = initial_snapshot_generation;
  slots_[0].reader_count_.store(1, std::memory_order_relaxed);  // cur-pin
  cur_slot_index_.store(0, std::memory_order_release);

  initFreeList();
}

GraphSnapshotStore::~GraphSnapshotStore() {
  for (auto& slot : slots_) {
    slot.storage_.reset();
    slot.view_ = GraphView();
  }
}

void GraphSnapshotStore::initFreeList() {
  // Slots 1 to slot_num_-1 are initially free
  for (int i = 1; i < slot_num_; ++i) {
    free_list_.push_back(i);
  }
}

int GraphSnapshotStore::getFreeSlot() {
  std::lock_guard<std::mutex> lock(free_list_mutex_);
  if (free_list_.empty()) {
    return -1;  // No free slot
  }
  int slot_index = free_list_.back();
  free_list_.pop_back();
  return slot_index;
}

void GraphSnapshotStore::returnFreeSlot(int slot_index) {
  std::lock_guard<std::mutex> lock(free_list_mutex_);
  free_list_.push_back(slot_index);
}

void GraphSnapshotStore::cleanupSlot(int slot_index) {
  if (slot_index < 0 || slot_index >= slot_num_) {
    return;
  }
  slots_[slot_index].storage_.reset();
  slots_[slot_index].view_ = GraphView();
  slots_[slot_index].snapshot_generation_ = 0;
  slots_[slot_index].reader_count_.fetch_add(-kCleanupSentinel,
                                             std::memory_order_release);
  returnFreeSlot(slot_index);
}

GraphSnapshotStore::SnapshotSlot&
GraphSnapshotStore::PinCurrentSnapshot() noexcept {
  while (true) {
    int slot_index = cur_slot_index_.load(std::memory_order_acquire);

    // Invariant: while a slot is current, reader_count_ >= 1 (cur-pin).
    // Spin until the slot looks ready: count <= 0 means either the
    // write-guard is active (count < 0, snapshot preparation still writing) or
    // the slot is transitioning / being cleaned up (count == 0, not yet
    // pinned by a new cur-pin).  In both cases reloading cur and retrying
    // is correct.  We do NOT modify reader_count_ in this branch to avoid
    // racing with the writer's release-bump.
    int observed =
        slots_[slot_index].reader_count_.load(std::memory_order_acquire);
    if (observed <= 0) {
      continue;
    }

    // Optimistically pin with acq_rel so we synchronise with the release
    // fence in PrepareSnapshot and see fully-written storage_/view_.
    int old_count = slots_[slot_index].reader_count_.fetch_add(
        1, std::memory_order_acq_rel);

    if (old_count <= 0) {
      // Raced between the load and fetch_add — count dropped back to <= 0
      // (write-guard re-entered or slot freed).  Roll back with a plain
      // relaxed sub: count stays negative/zero throughout so no cleanup
      // CAS (which requires count == 0 after transition from cur-pin) fires.
      slots_[slot_index].reader_count_.fetch_sub(1, std::memory_order_relaxed);
      continue;
    }

    if (cur_slot_index_.load(std::memory_order_acquire) == slot_index) {
      return slots_[slot_index];
    }

    UnpinSnapshotByIndex(slot_index);
  }
}

void GraphSnapshotStore::UnpinSnapshot(const SnapshotSlot& slot) noexcept {
  int slot_index = static_cast<int>(&slot - slots_.data());
  UnpinSnapshotByIndex(slot_index);
}

void GraphSnapshotStore::UnpinSnapshotByIndex(int slot_index) noexcept {
  if (slot_index < 0 || slot_index >= slot_num_) {
    LOG(ERROR) << "Invalid slot index in UnpinSnapshot: " << slot_index;
    return;
  }

  int prev_count =
      slots_[slot_index].reader_count_.fetch_sub(1, std::memory_order_acq_rel);
  if (prev_count <= 0) {
    LOG(ERROR) << "UnpinSnapshot called on slot with reader_count <= 0";
    return;
  }

  // If this was the last reader and slot is no longer current, clean it up.
  // Use CAS on reader_count (0 → -1) as a cleanup lock to prevent a
  // concurrent PinCurrentSnapshot (which does fetch_add(1)) from racing with
  // cleanup. If CAS fails, another thread either pinned the slot (count > 0)
  // or is already cleaning it up (count < 0); either way we skip cleanup.
  if (prev_count == 1) {
    int current = cur_slot_index_.load(std::memory_order_acquire);
    if (slot_index != current && slots_[slot_index].storage_) {
      int expected = 0;
      if (slots_[slot_index].reader_count_.compare_exchange_strong(
              expected, kCleanupSentinel, std::memory_order_acq_rel)) {
        cleanupSlot(slot_index);
      }
    }
  }
}

const PropertyGraph& GraphSnapshotStore::CurrentSnapshot() const {
  int slot_index = cur_slot_index_.load(std::memory_order_acquire);
  CHECK(slots_[slot_index].storage_ != nullptr);
  return *slots_[slot_index].storage_;
}

uint32_t GraphSnapshotStore::reserveSnapshotGeneration() {
  uint32_t current =
      last_reserved_snapshot_generation_.load(std::memory_order_relaxed);
  while (true) {
    if (current == std::numeric_limits<uint32_t>::max()) {
      THROW_RUNTIME_ERROR(
          "Snapshot generation space exhausted; restart the database before "
          "preparing another snapshot");
    }
    if (last_reserved_snapshot_generation_.compare_exchange_weak(
            current, current + 1, std::memory_order_acq_rel,
            std::memory_order_relaxed)) {
      return current + 1;
    }
  }
}

result<GraphSnapshotStore::PreparedSnapshot>
GraphSnapshotStore::PrepareSnapshot(
    const std::shared_ptr<PropertyGraph>& new_pg) {
  if (!new_pg) {
    return tl::unexpected(
        Status(StatusCode::ERR_INVALID_ARGUMENT,
               "Cannot prepare a null PropertyGraph snapshot"));
  }

  int slot_index = getFreeSlot();
  if (slot_index < 0) {
    return tl::unexpected(Status(StatusCode::ERR_POOL_EXHAUSTED,
                                 "GraphSnapshotStore slot exhausted"));
  }

  uint32_t snapshot_generation = 0;
  try {
    snapshot_generation = reserveSnapshotGeneration();

    // Keep the reserved slot invisible while its storage and view are built.
    slots_[slot_index].reader_count_.store(kCleanupSentinel,
                                           std::memory_order_relaxed);
    slots_[slot_index].storage_ = new_pg;
    slots_[slot_index].view_ = GraphView(*new_pg);
    slots_[slot_index].snapshot_generation_ = snapshot_generation;

    // Convert the write guard into the token's prep-pin. The prep-pin becomes
    // the cur-pin if Publish() succeeds, or drives cleanup if the token aborts.
    slots_[slot_index].reader_count_.fetch_add(-kCleanupSentinel + 1,
                                               std::memory_order_release);
  } catch (...) {
    slots_[slot_index].storage_.reset();
    slots_[slot_index].view_ = GraphView();
    slots_[slot_index].snapshot_generation_ = 0;
    slots_[slot_index].reader_count_.store(0, std::memory_order_release);
    returnFreeSlot(slot_index);
    throw;
  }

  return PreparedSnapshot(*this, slot_index);
}

void GraphSnapshotStore::publishPreparedSnapshot(int slot_index) noexcept {
  CHECK_GE(slot_index, 0);
  CHECK_LT(slot_index, slot_num_);
  CHECK_EQ(slots_[slot_index].reader_count_.load(std::memory_order_acquire), 1);

  // The prepared slot's prep-pin becomes its cur-pin at this exchange. Atomic
  // exchange also makes simultaneous slot transfers form a safe hand-off chain.
  const int old_slot_index =
      cur_slot_index_.exchange(slot_index, std::memory_order_acq_rel);

  // Release the old cur-pin; cleanup is immediate only when no reader holds it.
  UnpinSnapshotByIndex(old_slot_index);
}

}  // namespace neug
