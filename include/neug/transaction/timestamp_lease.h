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
#include <chrono>
#include <optional>

namespace neug {

class ExecutionSlot;
class IVersionManager;

/**
 * @brief RAII owner of an update timestamp and its admission-state lifecycle.
 *
 * An unfinished lease closes its timestamp gap without changing the installed
 * snapshot generation. Snapshot commits finish with the generation installed
 * by the prepared snapshot publication.
 */
class UpdateTimestampLease {
 public:
  explicit UpdateTimestampLease(IVersionManager& version_manager);
  UpdateTimestampLease(IVersionManager& version_manager,
                       std::chrono::steady_clock::time_point deadline);
  UpdateTimestampLease(UpdateTimestampLease&& other) noexcept;
  ~UpdateTimestampLease() noexcept;

  UpdateTimestampLease(const UpdateTimestampLease&) = delete;
  UpdateTimestampLease& operator=(const UpdateTimestampLease&) = delete;
  UpdateTimestampLease& operator=(UpdateTimestampLease&&) = delete;

  uint32_t Timestamp() const noexcept { return timestamp_; }
  bool active() const noexcept { return timestamp_ != kInactiveTimestamp; }

  void BeginCommit();
  void MakeUpdateExclusive();
  void Finish(std::optional<uint32_t> installed_snapshot_generation) noexcept;
  /// Finish after storage and WAL have moved to a new timeline. The installed
  /// snapshot generation is preserved while transaction visibility timestamps
  /// restart from zero.
  void FinishAndResetTimeline() noexcept;

 private:
  friend class ExecutionSlot;

  static std::optional<UpdateTimestampLease> TryAcquire(
      IVersionManager& version_manager);
  UpdateTimestampLease(IVersionManager& version_manager, uint32_t timestamp)
      : version_manager_(&version_manager), timestamp_(timestamp) {}
  void reset() noexcept;

  static constexpr uint32_t kInactiveTimestamp = UINT32_MAX;

  IVersionManager* version_manager_{nullptr};
  uint32_t timestamp_{kInactiveTimestamp};
  bool commit_started_{false};
};

}  // namespace neug
