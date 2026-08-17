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

#include "neug/transaction/current_cow_write_transaction.h"

#include <glog/logging.h>

#include <limits>
#include <string>
#include <utility>

#include "neug/transaction/wal/wal.h"

namespace neug {

CurrentCowWriteTransaction CurrentCowWriteTransaction::Begin(
    CurrentGraphWriteGuard guard, Allocator& alloc,
    GraphSnapshotStore& snapshot_store, IWalWriter& wal_writer) {
  auto& base = guard.Snapshot();
  auto cow_graph = base.graph().Clone();
  return CurrentCowWriteTransaction(
      std::move(guard),
      CowGraphWriteSet(std::move(cow_graph), base.planning_generation()), alloc,
      snapshot_store, wal_writer);
}

CurrentCowWriteTransaction::CurrentCowWriteTransaction(
    CurrentGraphWriteGuard guard, CowGraphWriteSet write_set, Allocator& alloc,
    GraphSnapshotStore& snapshot_store, IWalWriter& wal_writer) noexcept
    : guard_(std::move(guard)),
      write_set_(std::move(write_set)),
      alloc_(alloc),
      snapshot_store_(snapshot_store),
      wal_writer_(wal_writer) {}

CurrentCowWriteTransaction::CurrentCowWriteTransaction(
    CurrentCowWriteTransaction&& other) noexcept
    : guard_(std::move(other.guard_)),
      write_set_(std::move(other.write_set_)),
      alloc_(other.alloc_),
      snapshot_store_(other.snapshot_store_),
      wal_writer_(other.wal_writer_) {}

CurrentCowWriteTransaction::~CurrentCowWriteTransaction() noexcept { Abort(); }

Status CurrentCowWriteTransaction::Commit() {
  if (!active()) {
    return Status::OK();
  }
  auto& logical_redo = write_set_.logical_redo();
  if (logical_redo.op_num() == 0) {
    release(false);
    return Status::OK();
  }

  uint64_t committed_planning_generation = 0;
  auto prepare_status = PrepareCommit(committed_planning_generation);
  if (!prepare_status.ok()) {
    Abort();
    return prepare_status;
  }

  logical_redo.finalize(timestamp());

  // The current WAL API cannot distinguish a pre-write failure from an
  // uncertain partial append. Until W1 framing supplies that decision, any
  // append failure must fail-stop instead of reopening the AP gate and
  // reporting an ordinary rollback.
  try {
    if (!wal_writer_.append(logical_redo.data(), logical_redo.size())) {
      LOG(FATAL) << "AP WAL append failed after commit append began; "
                    "terminating with the current slot unchanged";
    }
  } catch (const std::exception& e) {
    LOG(FATAL) << "AP WAL append failed after commit append began: " << e.what()
               << "; terminating with the current slot unchanged";
  } catch (...) {
    LOG(FATAL) << "AP WAL append failed after commit append began; "
                  "terminating with the current slot unchanged";
  }

  snapshot_store_.publishCurrentReplacement(
      guard_.Snapshot(), write_set_.graph(), write_set_.view(),
      committed_planning_generation);
  release(true);
  return Status::OK();
}

Status CurrentCowWriteTransaction::PrepareCommit(
    uint64_t& committed_planning_generation) {
  const bool planning_changed = write_set_.PlanningChanged();
  if (planning_changed && write_set_.base_planning_generation() ==
                              std::numeric_limits<uint64_t>::max()) {
    return Status::InternalError("Planning generation space exhausted");
  }
  committed_planning_generation =
      write_set_.base_planning_generation() + (planning_changed ? 1 : 0);

  // Rebuild the write set's own view before WAL append so every operation that
  // can allocate or fail stays on the rollback side of the durability boundary.
  try {
    write_set_.view().Rebuild(*write_set_.graph());
  } catch (const std::exception& e) {
    return Status::InternalError(
        std::string("Failed to prepare AP graph view: ") + e.what());
  } catch (...) {
    return Status::InternalError("Failed to prepare AP graph view");
  }
  return Status::OK();
}

void CurrentCowWriteTransaction::Abort() noexcept {
  if (active()) {
    release(false);
  }
}

void CurrentCowWriteTransaction::release(bool committed) noexcept {
  std::optional<uint32_t> snapshot_generation;
  if (committed) {
    snapshot_generation = guard_.Snapshot().snapshot_generation();
  }
  write_set_.Reset();
  guard_.release(snapshot_generation);
}

}  // namespace neug
