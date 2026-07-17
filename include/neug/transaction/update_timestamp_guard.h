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
#pragma once

#include "neug/utils/property/types.h"

namespace neug {

class IVersionManager;

/**
 * Move-only RAII owner of an update timestamp.
 *
 * The guard contains only transaction-layer concurrency state. Storage, WAL,
 * checkpoint, and query-cache policies remain with their respective callers.
 */
class UpdateTimestampGuard {
 public:
  static UpdateTimestampGuard Acquire(IVersionManager& version_manager);

  ~UpdateTimestampGuard() noexcept;

  UpdateTimestampGuard(const UpdateTimestampGuard&) = delete;
  UpdateTimestampGuard& operator=(const UpdateTimestampGuard&) = delete;
  UpdateTimestampGuard(UpdateTimestampGuard&& other) noexcept;
  UpdateTimestampGuard& operator=(UpdateTimestampGuard&& other) noexcept;

  timestamp_t timestamp() const noexcept { return timestamp_; }
  bool active() const noexcept;

  void BeginCommit();
  void DrainReaders();
  void Release() noexcept;
  /// Finish a successful checkpoint by resetting the timestamp timeline and
  /// restoring update_state_ to 0.
  void CompleteCheckpoint() noexcept;

 private:
  UpdateTimestampGuard(IVersionManager& version_manager,
                       timestamp_t timestamp) noexcept;

  void disarm() noexcept;

  IVersionManager* version_manager_;
  timestamp_t timestamp_;
  bool commit_started_{false};
};

}  // namespace neug
