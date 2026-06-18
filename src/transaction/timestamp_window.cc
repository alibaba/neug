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

#include "neug/transaction/timestamp_window.h"

#include "glog/logging.h"

namespace neug {

TimestampWindow::TimestampWindow() {
  // Initialize completed timestamp bitmap
  completed_ts_ = std::make_unique<std::atomic<bool>[]>(kWindowSize);
  for (size_t i = 0; i < kWindowSize; ++i) {
    completed_ts_[i].store(false, std::memory_order_relaxed);
  }
}

TimestampWindow::~TimestampWindow() = default;

void TimestampWindow::init() {
  window_base_ = 0;
  for (size_t i = 0; i < kWindowSize; ++i) {
    completed_ts_[i].store(false, std::memory_order_relaxed);
  }
}

void TimestampWindow::mark_completed(uint32_t ts) {
  size_t idx = ts_index(ts);
  // Correctness still holds even with the buffer overflow
  completed_ts_[idx].store(true, std::memory_order_release);
}

bool TimestampWindow::is_completed(uint32_t ts) const {
  size_t idx = ts_index(ts);
  return completed_ts_[idx].load(std::memory_order_acquire);
}

void TimestampWindow::clear(uint32_t ts) {
  size_t idx = ts_index(ts);
  completed_ts_[idx].store(false, std::memory_order_relaxed);
}

void TimestampWindow::slide_window(uint32_t current_ts) {
  // Sliding window (if advanced significantly)
  if (current_ts > window_base_ + kWindowSize / 2) {
    // Clean up old window
    uint32_t new_base = current_ts - kWindowSize / 4;
    for (uint32_t ts = window_base_; ts < new_base; ++ts) {
      clear(ts);
    }
    window_base_ = new_base;
  }
}

}  // namespace neug