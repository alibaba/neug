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

#include <atomic>
#include <cstdint>
#include <memory>

namespace neug {

// Timestamp completion tracking window
// Tracks completed timestamps in a sliding window to enable ordered read_ts
// advancement
class TimestampWindow {
 public:
  TimestampWindow();
  ~TimestampWindow();

  // Initialize/reset the window
  void init();

  // Mark a timestamp as completed
  void mark_completed(uint32_t ts);

  // Check if a timestamp is completed
  bool is_completed(uint32_t ts) const;

  // Clear a timestamp (called after read_ts advances past it)
  void clear(uint32_t ts);

  // Advance the window base position (sliding window maintenance)
  void slide_window(uint32_t current_ts);

 private:
  static constexpr size_t kWindowSize =
      65536;  // Window size for timestamp tracking

  // Convert timestamp to array index
  inline size_t ts_index(uint32_t ts) const { return ts % kWindowSize; }

  // Completed timestamp bitmap
  std::unique_ptr<std::atomic<bool>[]> completed_ts_;

  // Base position of sliding window
  uint32_t window_base_{0};
};

}  // namespace neug