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

#include <optional>

#include "glog/logging.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/timestamp_lease.h"
#include "neug/utils/likely.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

/**
 * RAII owner of a writer-exclusive in-place write.
 *
 * Construction acquires an update timestamp, blocks and drains readers, then
 * pins the current snapshot for mutation. Destruction publishes its planning
 * generation first, publishes the matching timestamp/snapshot read view,
 * reopens admission, and finally releases the snapshot pin. Publication also
 * happens during stack unwinding because in-place mutations cannot be rolled
 * back.
 */
class InPlaceWriteScope {
 public:
  InPlaceWriteScope(IVersionManager& version_manager,
                    GraphSnapshotStore& snapshot_store);
  ~InPlaceWriteScope() noexcept;

  InPlaceWriteScope(const InPlaceWriteScope&) = delete;
  InPlaceWriteScope& operator=(const InPlaceWriteScope&) = delete;

  uint32_t Timestamp() const noexcept { return timestamp_lease_.Timestamp(); }
  GraphSnapshotStore::SnapshotSlot& Snapshot() noexcept {
    return snapshot_guard_->get();
  }
  void MarkPlanningChanged() noexcept { planning_changed_ = true; }

 private:
  void publish() noexcept;

  UpdateTimestampLease timestamp_lease_;
  GraphSnapshotStore& snapshot_store_;
  std::optional<SnapshotGuard> snapshot_guard_;
  bool planning_changed_{false};
};

enum class OpType : uint8_t {
  kCreateVertexType = 0,
  kCreateEdgeType = 1,
  kInsertVertex = 2,
  kInsertEdge = 3,
  kUpdateVertexProp = 4,
  kUpdateEdgeProp = 5,  // Update edge property by oe/ie offset
  kRemoveVertex = 6,
  kRemoveEdge = 7,  // Remove edge by oe/ie offset
  kAddVertexProp = 8,
  kAddEdgeProp = 9,
  kRenameVertexProp = 10,
  kRenameEdgeProp = 11,
  kDeleteVertexProp = 12,
  kDeleteEdgeProp = 13,
  kDeleteVertexType = 14,
  kDeleteEdgeType = 15,
  kAddGraphEntry = 16,
  kDropGraphEntry = 17
};

inline InArchive& operator<<(InArchive& in_archive, OpType& value) {
  in_archive << static_cast<uint8_t>(value);
  return in_archive;
}
inline OutArchive& operator>>(OutArchive& out_archive, OpType& value) {
  uint8_t op_type;
  out_archive >> op_type;
  value = static_cast<OpType>(op_type);
  return out_archive;
}

}  // namespace neug
