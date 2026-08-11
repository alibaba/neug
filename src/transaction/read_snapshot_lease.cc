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

#include "neug/transaction/read_snapshot_lease.h"

#include <optional>
#include <utility>

namespace neug {

ReadSnapshotLease::ReadSnapshotLease(SharedOperationLease admission,
                                     SnapshotGuard snapshot,
                                     timestamp_t timestamp) noexcept
    : timestamp_(timestamp),
      admission_(std::move(admission)),
      snapshot_(std::move(snapshot)) {}

ReadSnapshotLease::ReadSnapshotLease(ReadSnapshotLease&& other) noexcept
    : timestamp_(other.timestamp_),
      admission_(std::move(other.admission_)),
      snapshot_(std::move(other.snapshot_)) {}

ReadSnapshotLease& ReadSnapshotLease::operator=(
    ReadSnapshotLease&& other) noexcept {
  if (this != &other) {
    release();
    timestamp_ = other.timestamp_;
    admission_ = std::move(other.admission_);
    snapshot_ = std::move(other.snapshot_);
  }
  return *this;
}

ReadSnapshotLease::~ReadSnapshotLease() noexcept { release(); }

ReadSnapshotLease ReadSnapshotLease::Acquire(
    IVersionManager& version_manager, GraphSnapshotStore& snapshot_store) {
  std::optional<RuntimeBackoff> wait;
  for (;;) {
    auto operation = version_manager.acquire_read_operation();
    SnapshotGuard snapshot(snapshot_store);
    if (snapshot.get().snapshot_generation() ==
        operation.published_view.snapshot_generation) {
      return ReadSnapshotLease(std::move(operation.admission),
                               std::move(snapshot),
                               operation.published_view.visibility_ts);
    }

    // A snapshot was published between the read-view capture and the pin.
    // Release in protocol order, then reacquire a complete view.
    snapshot.release();
    operation.admission.release();
    if (!wait) {
      wait.emplace(version_manager.make_runtime_backoff());
    }
    (*wait)();
  }
}

void ReadSnapshotLease::release() noexcept {
  if (!admission_.active()) {
    return;
  }
  snapshot_.release();
  admission_.release();
}

}  // namespace neug
