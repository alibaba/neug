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
#include <optional>

namespace neug {

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
  UpdateTimestampLease(UpdateTimestampLease&& other) noexcept;
  ~UpdateTimestampLease() noexcept;

  UpdateTimestampLease(const UpdateTimestampLease&) = delete;
  UpdateTimestampLease& operator=(const UpdateTimestampLease&) = delete;
  UpdateTimestampLease& operator=(UpdateTimestampLease&&) = delete;

  uint32_t Timestamp() const noexcept { return timestamp_; }

  void BeginCommit();
  void MakeUpdateExclusive();
  void Finish(std::optional<uint32_t> installed_snapshot_generation) noexcept;

 private:
  void reset() noexcept;

  static constexpr uint32_t kInactiveTimestamp = UINT32_MAX;

  IVersionManager* version_manager_{nullptr};
  uint32_t timestamp_{kInactiveTimestamp};
  bool commit_started_{false};
};

}  // namespace neug
