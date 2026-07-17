/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/transaction/update_timestamp_guard.h"

#include <cassert>
#include <utility>

#include "neug/transaction/version_manager.h"
#include "neug/utils/exception/exception.h"

namespace neug {

UpdateTimestampGuard UpdateTimestampGuard::Acquire(
    IVersionManager& version_manager) {
  return UpdateTimestampGuard(version_manager,
                              version_manager.acquire_update_timestamp());
}

UpdateTimestampGuard::UpdateTimestampGuard(IVersionManager& version_manager,
                                           timestamp_t timestamp) noexcept
    : version_manager_(&version_manager), timestamp_(timestamp) {}

UpdateTimestampGuard::~UpdateTimestampGuard() noexcept { Release(); }

UpdateTimestampGuard::UpdateTimestampGuard(
    UpdateTimestampGuard&& other) noexcept
    : version_manager_(std::exchange(other.version_manager_, nullptr)),
      timestamp_(std::exchange(other.timestamp_, INVALID_TIMESTAMP)),
      commit_started_(std::exchange(other.commit_started_, false)) {}

UpdateTimestampGuard& UpdateTimestampGuard::operator=(
    UpdateTimestampGuard&& other) noexcept {
  if (this != &other) {
    Release();
    version_manager_ = std::exchange(other.version_manager_, nullptr);
    timestamp_ = std::exchange(other.timestamp_, INVALID_TIMESTAMP);
    commit_started_ = std::exchange(other.commit_started_, false);
  }
  return *this;
}

bool UpdateTimestampGuard::active() const noexcept {
  return version_manager_ != nullptr && timestamp_ != INVALID_TIMESTAMP;
}

void UpdateTimestampGuard::BeginCommit() {
  if (!active() || commit_started_) {
    THROW_INTERNAL_EXCEPTION(
        "BeginCommit requires an active update execution guard");
  }
  version_manager_->begin_update_commit(timestamp_);
  commit_started_ = true;
}

void UpdateTimestampGuard::DrainReaders() {
  if (!active() || !commit_started_) {
    THROW_INTERNAL_EXCEPTION(
        "DrainReaders requires an active update commit guard");
  }
  version_manager_->drain_readers();
}

void UpdateTimestampGuard::Release() noexcept {
  if (!active()) {
    return;
  }
  version_manager_->release_update_timestamp(timestamp_);
  disarm();
}

void UpdateTimestampGuard::CompleteCheckpoint() noexcept {
  assert(active() && commit_started_ &&
         "CompleteCheckpoint requires an active update commit guard");
  version_manager_->reset_timeline_after_checkpoint(timestamp_);
  disarm();
}

void UpdateTimestampGuard::disarm() noexcept {
  version_manager_ = nullptr;
  timestamp_ = INVALID_TIMESTAMP;
  commit_started_ = false;
}

}  // namespace neug
