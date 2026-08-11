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

#include "neug/transaction/ap_operation_guard.h"

#include <utility>

namespace neug {

APSharedGuard::APSharedGuard(SharedOperationLease admission,
                             SnapshotGuard snapshot,
                             timestamp_t timestamp) noexcept
    : timestamp_(timestamp),
      admission_(std::move(admission)),
      snapshot_(std::move(snapshot)) {}

APSharedGuard APSharedGuard::Acquire(IVersionManager& version_manager,
                                     GraphSnapshotStore& snapshot_store) {
  auto operation = version_manager.acquire_read_operation();
  SnapshotGuard snapshot(snapshot_store);
  return APSharedGuard(std::move(operation.admission), std::move(snapshot),
                       operation.published_view.visibility_ts);
}

APSharedGuard APSharedGuard::Acquire(
    IVersionManager& version_manager, GraphSnapshotStore& snapshot_store,
    std::chrono::steady_clock::time_point deadline) {
  auto operation = version_manager.acquire_read_operation_until(deadline);
  SnapshotGuard snapshot(snapshot_store);
  return APSharedGuard(std::move(operation.admission), std::move(snapshot),
                       operation.published_view.visibility_ts);
}

APSharedGuard::APSharedGuard(APSharedGuard&& other) noexcept
    : timestamp_(other.timestamp_),
      admission_(std::move(other.admission_)),
      snapshot_(std::move(other.snapshot_)) {}

APSharedGuard& APSharedGuard::operator=(APSharedGuard&& other) noexcept {
  if (this != &other) {
    release();
    timestamp_ = other.timestamp_;
    admission_ = std::move(other.admission_);
    snapshot_ = std::move(other.snapshot_);
  }
  return *this;
}

APSharedGuard::~APSharedGuard() noexcept { release(); }

void APSharedGuard::release() noexcept {
  if (!admission_.active()) {
    return;
  }
  snapshot_.release();
  admission_.release();
}

APExclusiveGuard::APExclusiveGuard(ExclusiveOperationLease admission,
                                   SnapshotGuard snapshot) noexcept
    : admission_(std::move(admission)), snapshot_(std::move(snapshot)) {}

APExclusiveGuard APExclusiveGuard::Acquire(IVersionManager& version_manager,
                                           GraphSnapshotStore& snapshot_store) {
  auto admission = version_manager.acquire_exclusive_operation();
  SnapshotGuard snapshot(snapshot_store);
  return APExclusiveGuard(std::move(admission), std::move(snapshot));
}

APExclusiveGuard APExclusiveGuard::Acquire(
    IVersionManager& version_manager, GraphSnapshotStore& snapshot_store,
    std::chrono::steady_clock::time_point deadline) {
  auto admission = version_manager.acquire_exclusive_operation_until(deadline);
  SnapshotGuard snapshot(snapshot_store);
  return APExclusiveGuard(std::move(admission), std::move(snapshot));
}

APExclusiveGuard::APExclusiveGuard(APExclusiveGuard&& other) noexcept
    : admission_(std::move(other.admission_)),
      snapshot_(std::move(other.snapshot_)) {}

APExclusiveGuard& APExclusiveGuard::operator=(
    APExclusiveGuard&& other) noexcept {
  if (this != &other) {
    release();
    admission_ = std::move(other.admission_);
    snapshot_ = std::move(other.snapshot_);
  }
  return *this;
}

APExclusiveGuard::~APExclusiveGuard() noexcept { release(); }

void APExclusiveGuard::release() noexcept {
  if (!admission_.active()) {
    return;
  }
  snapshot_.release();
  admission_.release();
}

}  // namespace neug
