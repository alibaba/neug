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
#include <utility>

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

  ReadSnapshotLease(ReadSnapshotLease&& other) noexcept
      : version_manager_(other.version_manager_),
        published_view_(other.published_view_),
        active_(other.active_),
        snapshot_(std::move(other.snapshot_)) {
    other.version_manager_ = nullptr;
    other.active_ = false;
  }

  ReadSnapshotLease& operator=(ReadSnapshotLease&& other) noexcept {
    if (this != &other) {
      release();
      version_manager_ = other.version_manager_;
      published_view_ = other.published_view_;
      active_ = other.active_;
      snapshot_ = std::move(other.snapshot_);
      other.version_manager_ = nullptr;
      other.active_ = false;
    }
    return *this;
  }

  ReadSnapshotLease(const ReadSnapshotLease&) = delete;
  ReadSnapshotLease& operator=(const ReadSnapshotLease&) = delete;

  ~ReadSnapshotLease() noexcept { release(); }

  void release() noexcept {
    if (!active_) {
      return;
    }
    active_ = false;
    snapshot_.release();
    version_manager_->release_read_view();
  }

  timestamp_t timestamp() const { return published_view_.visibility_ts; }
  uint32_t view_generation() const { return published_view_.view_generation; }
  const GraphView& view() const { return snapshot_.get().view(); }
  const PropertyGraph* graph() const { return snapshot_.get().mutable_graph(); }
  bool valid() const { return active_; }

 private:
  ReadSnapshotLease(IVersionManager& version_manager, SnapshotGuard snapshot,
                    PublishedReadView published_view) noexcept
      : version_manager_(&version_manager),
        published_view_(published_view),
        active_(true),
        snapshot_(std::move(snapshot)) {}

  IVersionManager* version_manager_;
  PublishedReadView published_view_;
  bool active_;
  // Declared last so fallback destruction also unpins before reader release.
  SnapshotGuard snapshot_;
};

}  // namespace neug
