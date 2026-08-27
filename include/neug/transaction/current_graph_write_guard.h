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

#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/timestamp_lease.h"
#include "neug/transaction/version_manager.h"

namespace neug {

/**
 * @brief Exclusive operation admission paired with a pinned current snapshot.
 *
 * The guard reserves one database write timestamp, immediately upgrades the
 * corresponding write lease to fully exclusive admission, drains existing
 * readers, and then pins the current snapshot. AP remains single-version: the
 * timestamp orders WAL/recovery and later TP visibility, while the exclusive
 * admission prevents AP read/write concurrency.
 */
class CurrentGraphWriteGuard {
 public:
  static CurrentGraphWriteGuard Acquire(IVersionManager& version_manager,
                                        GraphSnapshotStore& snapshot_store);

  CurrentGraphWriteGuard(CurrentGraphWriteGuard&& other) noexcept;
  CurrentGraphWriteGuard& operator=(CurrentGraphWriteGuard&& other) = delete;

  CurrentGraphWriteGuard(const CurrentGraphWriteGuard&) = delete;
  CurrentGraphWriteGuard& operator=(const CurrentGraphWriteGuard&) = delete;

  ~CurrentGraphWriteGuard() noexcept;

  timestamp_t Timestamp() const noexcept {
    return timestamp_lease_.Timestamp();
  }

  void release(std::optional<uint32_t> installed_snapshot_generation =
                   std::nullopt) noexcept;
  bool active() const noexcept { return timestamp_lease_.active(); }

  GraphSnapshotStore::SnapshotSlot& Snapshot() { return snapshot_.get(); }
  const GraphSnapshotStore::SnapshotSlot& Snapshot() const {
    return snapshot_.get();
  }

 private:
  CurrentGraphWriteGuard(UpdateTimestampLease timestamp_lease,
                         SnapshotGuard snapshot) noexcept;

  UpdateTimestampLease timestamp_lease_;
  // Declared last so release explicitly unpins before publishing the completed
  // timestamp and reopening admission.
  SnapshotGuard snapshot_;
};

}  // namespace neug
