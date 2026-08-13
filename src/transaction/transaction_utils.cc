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

#include "neug/transaction/transaction_utils.h"

#include <glog/logging.h>

#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/timestamp_lease.h"

namespace neug {

InPlaceWriteScope::InPlaceWriteScope(IVersionManager& version_manager,
                                     GraphSnapshotStore& snapshot_store)
    : InPlaceWriteScope(
          CurrentGraphWriteGuard::Acquire(version_manager, snapshot_store),
          snapshot_store) {}

InPlaceWriteScope::InPlaceWriteScope(
    CurrentGraphWriteGuard guard, GraphSnapshotStore& snapshot_store) noexcept
    : guard_(std::move(guard)), snapshot_store_(&snapshot_store) {
  CHECK(guard_.active());
}

InPlaceWriteScope::~InPlaceWriteScope() noexcept { publish(); }

void InPlaceWriteScope::publish() noexcept {
  if (snapshot_store_ == nullptr) {
    return;
  }
  const uint32_t snapshot_generation =
      snapshot_store_->publishInPlaceMutation(Snapshot(), planning_changed_);
  guard_.release(snapshot_generation);
  snapshot_store_ = nullptr;
}

UpdateTimestampLease InPlaceWriteScope::ReleaseForCheckpoint() noexcept {
  CHECK(snapshot_store_ != nullptr);
  // Publish before releasing the pin because checkpoint preparation can fail
  // while the partially mutated current graph remains live.
  snapshot_store_->publishInPlaceMutation(Snapshot(), planning_changed_);
  snapshot_store_ = nullptr;
  return guard_.ReleaseForCheckpoint();
}

}  // namespace neug
