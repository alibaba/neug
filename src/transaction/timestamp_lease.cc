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

#include "neug/transaction/timestamp_lease.h"

#include <utility>

#include <glog/logging.h>

#include "neug/transaction/version_manager.h"
#include "neug/utils/exception/exception.h"

namespace neug {

std::optional<UpdateTimestampLease> UpdateTimestampLease::TryAcquire(
    IVersionManager& version_manager) {
  auto timestamp = version_manager.try_acquire_update_timestamp();
  if (!timestamp) {
    return std::nullopt;
  }
  return std::optional<UpdateTimestampLease>(
      UpdateTimestampLease(version_manager, *timestamp));
}

UpdateTimestampLease::UpdateTimestampLease(IVersionManager& version_manager,
                                           uint32_t timestamp) noexcept
    : version_manager_(&version_manager), timestamp_(timestamp) {
  CHECK_NE(timestamp_, kInactiveTimestamp);
}

UpdateTimestampLease::UpdateTimestampLease(IVersionManager& version_manager)
    : version_manager_(&version_manager),
      timestamp_(version_manager.acquire_update_timestamp()) {
  CHECK_NE(timestamp_, kInactiveTimestamp);
}

UpdateTimestampLease::UpdateTimestampLease(
    IVersionManager& version_manager,
    std::chrono::steady_clock::time_point deadline)
    : version_manager_(&version_manager),
      timestamp_(version_manager.acquire_update_timestamp_until(deadline)) {
  CHECK_NE(timestamp_, kInactiveTimestamp);
}

UpdateTimestampLease::UpdateTimestampLease(
    UpdateTimestampLease&& other) noexcept
    : version_manager_(std::exchange(other.version_manager_, nullptr)),
      timestamp_(std::exchange(other.timestamp_, kInactiveTimestamp)),
      commit_started_(std::exchange(other.commit_started_, false)) {}

UpdateTimestampLease::~UpdateTimestampLease() noexcept { reset(); }

void UpdateTimestampLease::BeginCommit() {
  CHECK_NE(timestamp_, kInactiveTimestamp);
  CHECK(!commit_started_);
  version_manager_->begin_update_commit(timestamp_);
  commit_started_ = true;
}

void UpdateTimestampLease::MakeUpdateExclusive() {
  BeginCommit();
  version_manager_->drain_readers();
}

void UpdateTimestampLease::MakeUpdateExclusiveUntil(
    std::chrono::steady_clock::time_point deadline) {
  BeginCommit();
  if (!version_manager_->drain_readers_until(deadline)) {
    THROW_TRANSACTION_TIMEOUT("waiting for active readers to finish");
  }
}

void UpdateTimestampLease::Finish(
    std::optional<uint32_t> installed_snapshot_generation) noexcept {
  CHECK_NE(timestamp_, kInactiveTimestamp);
  CHECK(!installed_snapshot_generation || commit_started_);
  version_manager_->finish_update_timestamp(timestamp_,
                                            installed_snapshot_generation);
  timestamp_ = kInactiveTimestamp;
  commit_started_ = false;
}

void UpdateTimestampLease::FinishAndResetTimeline() noexcept {
  CHECK_NE(timestamp_, kInactiveTimestamp);
  CHECK(commit_started_);
  version_manager_->finish_update_and_reset_timeline(timestamp_);
  timestamp_ = kInactiveTimestamp;
  commit_started_ = false;
}

void UpdateTimestampLease::reset() noexcept {
  if (timestamp_ == kInactiveTimestamp) {
    return;
  }
  version_manager_->finish_update_timestamp(timestamp_, std::nullopt);
  timestamp_ = kInactiveTimestamp;
  commit_started_ = false;
}

}  // namespace neug
