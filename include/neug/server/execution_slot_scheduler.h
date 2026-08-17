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

#include <cstddef>
#include <vector>

#include "bthread/bthread.h"
#include "neug/main/execution_slot.h"

namespace neug {

class ExecutionSlotSet;

/** @brief Service-runtime scheduler for DB-owned execution slots. */
class ExecutionSlotScheduler {
 public:
  explicit ExecutionSlotScheduler(ExecutionSlotSet& slots);
  ~ExecutionSlotScheduler() noexcept;

  ExecutionSlotScheduler(const ExecutionSlotScheduler&) = delete;
  ExecutionSlotScheduler& operator=(const ExecutionSlotScheduler&) = delete;

  ExecutionSlotLease AcquireExecutionSlot();
  ExecutionSlotLease TryAcquireExecutionSlot();
  void CloseAndDrain() noexcept;
  size_t ExecutionSlotNum() const noexcept;
  size_t ExecutedQueryNum() const noexcept;

 private:
  static void ReleaseExecutionSlot(void* owner, size_t slot_id) noexcept;

  ExecutionSlotSet& slots_;
  std::vector<size_t> available_slot_ids_;
  bthread_mutex_t mutex_;
  bthread_cond_t cond_;
  size_t active_acquirers_{0};
  bool closing_{false};
};

}  // namespace neug
