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
  completed_ts_ = std::make_unique<std::atomic<uint32_t>[]>(kWindowSize);
  for (size_t i = 0; i < kWindowSize; ++i) {
    completed_ts_[i].store(0, std::memory_order_relaxed);
  }
}

TimestampWindow::~TimestampWindow() = default;

void TimestampWindow::init() {
  for (size_t i = 0; i < kWindowSize; ++i) {
    completed_ts_[i].store(0, std::memory_order_relaxed);
  }
}

void TimestampWindow::mark_completed(uint32_t ts) {
  DCHECK_NE(ts, 0U);
  completed_ts_[ts_index(ts)].store(ts, std::memory_order_release);
}

bool TimestampWindow::is_completed(uint32_t ts) const {
  return completed_ts_[ts_index(ts)].load(std::memory_order_acquire) == ts;
}

void TimestampWindow::clear(uint32_t ts) {
  uint32_t expected = ts;
  (void) completed_ts_[ts_index(ts)].compare_exchange_strong(
      expected, 0, std::memory_order_relaxed, std::memory_order_relaxed);
}

}  // namespace neug
