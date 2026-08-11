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

#include <chrono>

#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/version_manager.h"

namespace neug {

/**
 * @brief AP read admission paired with a pinned current snapshot.
 *
 * The shared admission is acquired before the snapshot pin. AP writers use
 * the same gate exclusively, so no AP mutation can publish or modify the
 * current snapshot while this guard is active. PR-02A temporarily takes the
 * visibility timestamp from the specialized read operation solely for the
 * legacy direct storage reader; it does not own a TP coherent read view.
 */
class APSharedGuard {
 public:
  static APSharedGuard Acquire(IVersionManager& version_manager,
                               GraphSnapshotStore& snapshot_store);
  static APSharedGuard Acquire(IVersionManager& version_manager,
                               GraphSnapshotStore& snapshot_store,
                               std::chrono::steady_clock::time_point deadline);

  APSharedGuard(APSharedGuard&& other) noexcept;
  APSharedGuard& operator=(APSharedGuard&& other) noexcept;

  APSharedGuard(const APSharedGuard&) = delete;
  APSharedGuard& operator=(const APSharedGuard&) = delete;

  ~APSharedGuard() noexcept;

  void release() noexcept;
  bool active() const noexcept { return admission_.active(); }

  const GraphView& view() const { return snapshot_.get().view(); }
  uint64_t planning_generation() const {
    return snapshot_.get().planning_generation();
  }
  timestamp_t timestamp() const noexcept { return timestamp_; }

 private:
  APSharedGuard(SharedOperationLease admission, SnapshotGuard snapshot,
                timestamp_t timestamp) noexcept;

  timestamp_t timestamp_;
  SharedOperationLease admission_;
  // Declared last so fallback destruction unpins before reader admission
  // releases.
  SnapshotGuard snapshot_;
};

/**
 * @brief AP write admission paired with a pinned mutable current snapshot.
 *
 * This owns only gate exclusion and the snapshot pin. It never reserves a
 * write timestamp or changes the VersionManager timeline; AP mutation and
 * durability semantics are composed by the caller.
 */
class APExclusiveGuard {
 public:
  static APExclusiveGuard Acquire(IVersionManager& version_manager,
                                  GraphSnapshotStore& snapshot_store);
  static APExclusiveGuard Acquire(
      IVersionManager& version_manager, GraphSnapshotStore& snapshot_store,
      std::chrono::steady_clock::time_point deadline);

  APExclusiveGuard(APExclusiveGuard&& other) noexcept;
  APExclusiveGuard& operator=(APExclusiveGuard&& other) noexcept;

  APExclusiveGuard(const APExclusiveGuard&) = delete;
  APExclusiveGuard& operator=(const APExclusiveGuard&) = delete;

  ~APExclusiveGuard() noexcept;

  void release() noexcept;
  bool active() const noexcept { return admission_.active(); }

  GraphSnapshotStore::SnapshotSlot& Snapshot() { return snapshot_.get(); }
  const GraphSnapshotStore::SnapshotSlot& Snapshot() const {
    return snapshot_.get();
  }

 private:
  APExclusiveGuard(ExclusiveOperationLease admission,
                   SnapshotGuard snapshot) noexcept;

  ExclusiveOperationLease admission_;
  // Declared last so fallback destruction unpins before reopening the gate.
  SnapshotGuard snapshot_;
};

}  // namespace neug
