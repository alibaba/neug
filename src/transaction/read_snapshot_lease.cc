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

namespace neug {

ReadSnapshotLease ReadSnapshotLease::Acquire(
    IVersionManager& version_manager, GraphSnapshotStore& snapshot_store) {
  for (;;) {
    const PublishedReadView published = version_manager.acquire_read_view();
    SnapshotGuard snapshot(snapshot_store);
    if (snapshot.get().view_generation() == published.view_generation) {
      return ReadSnapshotLease(version_manager, std::move(snapshot), published);
    }

    // A snapshot was published between the read-view capture and the pin.
    // Release in protocol order, then reacquire a complete view.
    snapshot.release();
    version_manager.release_read_timestamp();
  }
}

}  // namespace neug
