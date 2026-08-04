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
    : timestamp_lease_(version_manager), snapshot_store_(snapshot_store) {
  timestamp_lease_.MakeUpdateExclusive();
  snapshot_guard_.emplace(snapshot_store_);
}

InPlaceWriteScope::~InPlaceWriteScope() noexcept { publish(); }

void InPlaceWriteScope::publish() noexcept {
  CHECK(snapshot_guard_ && snapshot_guard_->valid())
      << "In-place write scope requires a pinned mutable snapshot";
  const uint32_t snapshot_generation = snapshot_store_.publishInPlaceMutation(
      snapshot_guard_->get(), planning_changed_);
  timestamp_lease_.Finish(snapshot_generation);
}

}  // namespace neug
