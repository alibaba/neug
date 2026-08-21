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

#include "neug/transaction/current_graph_write_guard.h"

#include <utility>

namespace neug {

CurrentGraphWriteGuard::CurrentGraphWriteGuard(
    UpdateTimestampLease timestamp_lease, SnapshotGuard snapshot) noexcept
    : timestamp_lease_(std::move(timestamp_lease)),
      snapshot_(std::move(snapshot)) {}

CurrentGraphWriteGuard CurrentGraphWriteGuard::Acquire(
    IVersionManager& version_manager, GraphSnapshotStore& snapshot_store) {
  UpdateTimestampLease timestamp_lease(version_manager);
  timestamp_lease.MakeUpdateExclusive();
  SnapshotGuard snapshot(snapshot_store);
  return CurrentGraphWriteGuard(std::move(timestamp_lease),
                                std::move(snapshot));
}

CurrentGraphWriteGuard::CurrentGraphWriteGuard(
    CurrentGraphWriteGuard&& other) noexcept
    : timestamp_lease_(std::move(other.timestamp_lease_)),
      snapshot_(std::move(other.snapshot_)) {}

CurrentGraphWriteGuard::~CurrentGraphWriteGuard() noexcept { release(); }

void CurrentGraphWriteGuard::release(
    std::optional<uint32_t> installed_snapshot_generation) noexcept {
  if (!timestamp_lease_.active()) {
    return;
  }
  snapshot_.release();
  timestamp_lease_.Finish(installed_snapshot_generation);
}

}  // namespace neug
