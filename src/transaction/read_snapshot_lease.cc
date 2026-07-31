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

ReadSnapshotLease::ReadSnapshotLease(IVersionManager& version_manager,
                                     SnapshotGuard snapshot,
                                     timestamp_t timestamp) noexcept
    : version_manager_(&version_manager),
      timestamp_(timestamp),
      snapshot_(std::move(snapshot)) {}

ReadSnapshotLease::ReadSnapshotLease(ReadSnapshotLease&& other) noexcept
    : version_manager_(std::exchange(other.version_manager_, nullptr)),
      timestamp_(other.timestamp_),
      snapshot_(std::move(other.snapshot_)) {}

ReadSnapshotLease& ReadSnapshotLease::operator=(
    ReadSnapshotLease&& other) noexcept {
  if (this != &other) {
    release();
    version_manager_ = std::exchange(other.version_manager_, nullptr);
    timestamp_ = other.timestamp_;
    snapshot_ = std::move(other.snapshot_);
  }
  return *this;
}

ReadSnapshotLease::~ReadSnapshotLease() noexcept { release(); }

ReadSnapshotLease ReadSnapshotLease::Acquire(
    IVersionManager& version_manager, GraphSnapshotStore& snapshot_store) {
  std::optional<RuntimeBackoff> wait;
  for (;;) {
    const PublishedReadView published = version_manager.acquire_read_view();
    SnapshotGuard snapshot(snapshot_store);
    if (snapshot.get().snapshot_generation() == published.snapshot_generation) {
      return ReadSnapshotLease(version_manager, std::move(snapshot),
                               published.visibility_ts);
    }

    // A snapshot was published between the read-view capture and the pin.
    // Release in protocol order, then reacquire a complete view.
    snapshot.release();
    version_manager.release_read_view();
    if (!wait) {
      wait.emplace(version_manager.make_runtime_backoff());
    }
    (*wait)();
  }
}

void ReadSnapshotLease::release() noexcept {
  auto* version_manager = std::exchange(version_manager_, nullptr);
  if (!version_manager) {
    return;
  }
  snapshot_.release();
  version_manager->release_read_view();
}

}  // namespace neug
