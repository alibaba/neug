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

#include "neug/generated/proto/plan/error.pb.h"

namespace neug {

static constexpr int kCleanupSentinel = -(1 << 20);

GraphSnapshotStore::GraphSnapshotStore(
    int slot_num, std::shared_ptr<PropertyGraph> initial_pg)
    : slot_num_(slot_num), slots_(slot_num) {
  // Publish initial PG into slot 0.
  //
  // Invariant: while a slot is current, reader_count_ >= 1 (held by the
  // "cur-pin").  PublishSnapshot transfers the cur-pin from the old slot to
  // the new slot atomically around the cur_slot_index_ switch.  This lets
  // PinCurrentSnapshot safely roll back when old_count == 0 (slot is free or
  // being cleaned up) without mistaking a live cur slot for a dead one,
  // because a live cur slot always has count >= 1.
  slots_[0].storage_ = std::move(initial_pg);
  slots_[0].view_ = GraphView(*slots_[0].storage_);
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
  slots_[slot_index].reader_count_.fetch_add(-kCleanupSentinel,
                                             std::memory_order_release);
  returnFreeSlot(slot_index);
}

GraphSnapshotStore::SnapshotSlot& GraphSnapshotStore::PinCurrentSnapshot() {
  while (true) {
    int slot_index = cur_slot_index_.load(std::memory_order_acquire);

    // Invariant: while a slot is current, reader_count_ >= 1 (cur-pin).
    // Spin until the slot looks ready: count <= 0 means either the
    // write-guard is active (count < 0, PublishSnapshot still writing) or
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
    // fence in PublishSnapshot and see fully-written storage_/view_.
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

void GraphSnapshotStore::UnpinSnapshot(const SnapshotSlot& slot) {
  int slot_index = static_cast<int>(&slot - slots_.data());
  UnpinSnapshotByIndex(slot_index);
}

void GraphSnapshotStore::UnpinSnapshotByIndex(int slot_index) {
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

Status GraphSnapshotStore::PublishSnapshot(
    const std::shared_ptr<PropertyGraph>& new_pg) {
  int slot_index = getFreeSlot();
  if (slot_index < 0) {
    return Status(StatusCode::ERR_POOL_EXHAUSTED,
                  "GraphSnapshotStore slot exhausted");
  }

  // Write-guard: set reader_count_ to a large negative sentinel so that any
  // concurrent PinCurrentSnapshot that races onto this slot (via ABA reuse)
  // observes old_count < 0 and spins away.  We must finish writing
  // storage_ and view_ before making the slot visible to readers, so the
  // sentinel acts as a write-in-progress flag.
  //
  // cleanupSlot() leaves reader_count_ at 0 after returning the slot to the
  // free_list (fetch_add(-kCleanupSentinel) brings it from kCleanupSentinel
  // back to 0). We use store(kCleanupSentinel) here rather than fetch_add to
  // set an unambiguous negative value regardless of any residual count.
  slots_[slot_index].reader_count_.store(kCleanupSentinel,
                                         std::memory_order_relaxed);

  slots_[slot_index].storage_ = new_pg;
  slots_[slot_index].view_ = GraphView(*new_pg);

  // Release the write-guard: bump reader_count_ from kCleanupSentinel to 1
  // (the prep-pin) atomically with a release fence so that all prior writes
  // to storage_ and view_ are visible to any reader that subsequently observes
  // old_count >= 0 via fetch_add(1) with acquire semantics.
  slots_[slot_index].reader_count_.fetch_add(-kCleanupSentinel + 1,
                                             std::memory_order_release);

  // Load the old cur slot index while holding the invariant that the old
  // cur slot's count >= 1 (its cur-pin).  No need for a phantom-pin: the
  // cur-pin itself protects the old slot from premature cleanup across the
  // switch, because UnpinSnapshotByIndex only triggers cleanup when
  // prev_count == 1, meaning count drops to 0, which cannot happen while
  // the cur-pin is still held.
  int old_slot_index = cur_slot_index_.load(std::memory_order_acquire);

  // Switch cur to the new slot.  The new slot already has its cur-pin (= 1)
  // set by the fetch_add above, so readers that observe the new index will
  // see old_count >= 1 and pin successfully.
  cur_slot_index_.store(slot_index, std::memory_order_release);

  // Release the old slot's cur-pin now that the new slot is current.
  // If no readers are holding the old slot, prev_count == 1, count drops to
  // 0, and cleanup fires via the CAS inside UnpinSnapshotByIndex.
  // If readers still hold it, cleanup is deferred to the last reader release.
  UnpinSnapshotByIndex(old_slot_index);

  // The new slot's prep-pin becomes its cur-pin — do NOT release it here.

  return Status::OK();
}

}  // namespace neug
