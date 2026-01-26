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

namespace neug {

/**
 * @brief A simple implementation of spinlock based on std::atomic.
 */
class SpinLock {
  std::atomic_flag locked = ATOMIC_FLAG_INIT;

 public:
  void lock() {
    while (locked.test_and_set(std::memory_order_acquire)) {
      {
      }
    }
  }

  void unlock() { locked.clear(std::memory_order_relaxed); }
};

}  // namespace neug
