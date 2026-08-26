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

#include "neug/transaction/snapshot_cow_write_transaction.h"

#include <glog/logging.h>

#include <exception>
#include <limits>
#include <utility>

#include "neug/transaction/wal/wal.h"
#include "neug/utils/exception/exception.h"

namespace neug {

SnapshotCowWriteTransaction::SnapshotCowWriteTransaction(
    std::shared_ptr<PropertyGraph> cow_graph, uint64_t planning_generation,
    Allocator& alloc, IWalWriter& wal_writer,
    GraphSnapshotStore& snapshot_store, UpdateTimestampLease timestamp_lease)
    : workspace_(std::move(cow_graph), planning_generation),
      alloc_(alloc),
      wal_writer_(wal_writer),
      snapshot_store_(snapshot_store),
      timestamp_lease_(std::move(timestamp_lease)) {}

SnapshotCowWriteTransaction::SnapshotCowWriteTransaction(
    SnapshotCowWriteTransaction&& other) noexcept
    : workspace_(std::move(other.workspace_)),
      alloc_(other.alloc_),
      wal_writer_(other.wal_writer_),
      snapshot_store_(other.snapshot_store_),
      timestamp_lease_(std::move(other.timestamp_lease_)) {}

SnapshotCowWriteTransaction::~SnapshotCowWriteTransaction() noexcept {
  Abort();
}

bool SnapshotCowWriteTransaction::Commit() {
  if (!active()) {
    return true;
  }

  auto& logical_redo = workspace_.logical_redo();
  if (logical_redo.op_num() == 0 && logical_redo.content_size() == 0) {
    release(std::nullopt);
    return true;
  }

  const bool planning_changed = workspace_.PlanningChanged();
  if (planning_changed && workspace_.base_planning_generation() ==
                              std::numeric_limits<uint64_t>::max()) {
    LOG(ERROR) << "Planning generation space exhausted";
    Abort();
    return false;
  }
  const uint64_t committed_planning_generation =
      workspace_.base_planning_generation() + (planning_changed ? 1 : 0);

  auto prepared_result = snapshot_store_.PrepareSnapshot(
      workspace_.graph(), committed_planning_generation);
  if (!prepared_result) {
    LOG(ERROR) << "Failed to prepare graph snapshot: "
               << prepared_result.error().ToString();
    Abort();
    return false;
  }
  auto prepared = std::move(prepared_result).value();

  logical_redo.finalize(timestamp());
  // append() does not distinguish a pre-write failure from a partial append.
  // Until W1 framing makes recovery able to discard incomplete records, do not
  // report a normal rollback after starting the durability boundary.
  try {
    if (!wal_writer_.append(logical_redo.data(), logical_redo.size())) {
      LOG(FATAL) << "TP WAL append failed after commit append began; "
                    "terminating before snapshot publication";
    }
  } catch (const std::exception& e) {
    LOG(FATAL) << "TP WAL append failed after commit append began: " << e.what()
               << "; terminating before snapshot publication";
  } catch (...) {
    LOG(FATAL) << "TP WAL append failed after commit append began; "
                  "terminating before snapshot publication";
  }

  timestamp_lease_.BeginCommit();
  const uint32_t snapshot_generation = std::move(prepared).Publish();
  release(snapshot_generation);
  return true;
}

void SnapshotCowWriteTransaction::Abort() noexcept {
  if (active()) {
    release(std::nullopt);
  }
}

Value SnapshotCowWriteTransaction::GetVertexProperty(label_t label, vid_t lid,
                                                     int col_id) const {
  auto& graph = *workspace_.graph();
  auto col = graph.GetVertexPropertyColumn(label, col_id);
  if (!graph.IsValidLid(label, lid, timestamp())) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Vertex lid is not valid in this transaction");
  }
  if (col == nullptr) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Fail to find property column");
  }
  return col->get_any(lid);
}

Value SnapshotCowWriteTransaction::GetVertexId(label_t label, vid_t lid) const {
  return workspace_.graph()->GetOid(label, lid, timestamp());
}

bool SnapshotCowWriteTransaction::GetVertexIndex(label_t label, const Value& id,
                                                 vid_t& index) const {
  return workspace_.graph()->get_lid(label, id, index, timestamp());
}

void SnapshotCowWriteTransaction::release(
    std::optional<uint32_t> installed_snapshot_generation) noexcept {
  workspace_.Reset();
  timestamp_lease_.Finish(installed_snapshot_generation);
}

}  // namespace neug
