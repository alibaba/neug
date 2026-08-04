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

#include <stdint.h>

#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/version_manager.h"
#include "neug/utils/property/types.h"

namespace neug {

/**
 * @brief Move-only owner of one coherent timestamp/snapshot pair.
 *
 * Acquisition registers a reader, captures the atomically published
 * timestamp/generation pair, pins the current snapshot, and validates that
 * the pinned slot belongs to that generation. A concurrent publication
 * produces a detectable mismatch and is retried internally.
 */
class ReadSnapshotLease {
 public:
  static ReadSnapshotLease Acquire(IVersionManager& version_manager,
                                   GraphSnapshotStore& snapshot_store);

  ReadSnapshotLease(ReadSnapshotLease&& other) noexcept;
  ReadSnapshotLease& operator=(ReadSnapshotLease&& other) noexcept;

  ReadSnapshotLease(const ReadSnapshotLease&) = delete;
  ReadSnapshotLease& operator=(const ReadSnapshotLease&) = delete;

  ~ReadSnapshotLease() noexcept;

  void release() noexcept;

  timestamp_t timestamp() const { return timestamp_; }
  /// The returned view remains valid until this lease is released or destroyed.
  const GraphView& view() const { return snapshot_.get().view(); }
  /// Planning generation carried by the same pinned snapshot as view().
  uint64_t planning_generation() const {
    return snapshot_.get().planning_generation();
  }

 private:
  ReadSnapshotLease(IVersionManager& version_manager, SnapshotGuard snapshot,
                    timestamp_t timestamp) noexcept;

  IVersionManager* version_manager_;
  timestamp_t timestamp_;
  // Declared last so fallback destruction also unpins before reader release.
  SnapshotGuard snapshot_;
};

}  // namespace neug
