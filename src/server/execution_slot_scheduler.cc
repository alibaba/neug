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

#include "neug/server/execution_slot_scheduler.h"

#include <glog/logging.h>

#include "neug/main/execution_slot_set.h"

namespace neug {

ExecutionSlotScheduler::ExecutionSlotScheduler(ExecutionSlotSet& slots)
    : slots_(slots) {
  available_slot_ids_.reserve(slots_.Size());
  bthread_mutex_init(&mutex_, nullptr);
  bthread_cond_init(&cond_, nullptr);
  for (size_t slot_id = 0; slot_id < slots_.Size(); ++slot_id) {
    available_slot_ids_.push_back(slot_id);
  }
  LOG(INFO) << "Initializing ExecutionSlotScheduler with " << slots_.Size()
            << " slots.";
}

ExecutionSlotScheduler::~ExecutionSlotScheduler() noexcept {
  CHECK_EQ(available_slot_ids_.size(), slots_.Size())
      << "All ExecutionSlotLease objects must be released before scheduler "
         "destruction";
  bthread_cond_destroy(&cond_);
  bthread_mutex_destroy(&mutex_);
}

ExecutionSlotLease ExecutionSlotScheduler::AcquireExecutionSlot() {
  bthread_mutex_lock(&mutex_);
  while (available_slot_ids_.empty()) {
    bthread_cond_wait(&cond_, &mutex_);
  }

  const auto slot_id = available_slot_ids_.back();
  available_slot_ids_.pop_back();
  bthread_mutex_unlock(&mutex_);
  return ExecutionSlotLease(&slots_.At(slot_id), this, slot_id,
                            &ExecutionSlotScheduler::ReleaseExecutionSlot);
}

size_t ExecutionSlotScheduler::ExecutionSlotNum() const noexcept {
  return slots_.Size();
}

size_t ExecutionSlotScheduler::ExecutedQueryNum() const noexcept {
  return slots_.ExecutedQueryNum();
}

void ExecutionSlotScheduler::ReleaseExecutionSlot(void* owner,
                                                  size_t slot_id) noexcept {
  auto* scheduler = static_cast<ExecutionSlotScheduler*>(owner);
  bthread_mutex_lock(&scheduler->mutex_);
  CHECK_LT(slot_id, scheduler->slots_.Size());
  CHECK_LT(scheduler->available_slot_ids_.size(), scheduler->slots_.Size());
  CHECK_GE(scheduler->available_slot_ids_.capacity(), scheduler->slots_.Size());
  scheduler->available_slot_ids_.push_back(slot_id);
  VLOG(10) << "Released slot_id=" << slot_id;
  bthread_cond_signal(&scheduler->cond_);
  bthread_mutex_unlock(&scheduler->mutex_);
}

}  // namespace neug
