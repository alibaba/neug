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

#include "neug/server/tp_execution_slot_pool.h"

namespace neug {

ExecutionSlotLease TpExecutionSlotPool::AcquireExecutionSlot() {
#ifndef _WIN32
  bthread_mutex_lock(&mutex_);
  while (available_slot_ids_.empty()) {
    bthread_cond_wait(&cond_, &mutex_);
  }

  const auto slot_id = available_slot_ids_.back();
  available_slot_ids_.pop_back();
  CHECK_LT(slot_id, slot_num_);
  bthread_mutex_unlock(&mutex_);
#else
  std::unique_lock<std::mutex> lock(mutex_);
  cond_.wait(lock, [this] { return !available_slot_ids_.empty(); });

  const auto slot_id = available_slot_ids_.back();
  available_slot_ids_.pop_back();
  CHECK_LT(slot_id, slot_num_);
  lock.unlock();
#endif
  return ExecutionSlotLease(&entries_[slot_id].slot, this, slot_id,
                            &TpExecutionSlotPool::releaseExecutionSlot);
}

ExecutionSlotLease TpExecutionSlotPool::TryAcquireExecutionSlot() {
#ifndef _WIN32
  bthread_mutex_lock(&mutex_);
  if (available_slot_ids_.empty()) {
    bthread_mutex_unlock(&mutex_);
    return {};
  }
  const auto slot_id = available_slot_ids_.back();
  available_slot_ids_.pop_back();
  CHECK_LT(slot_id, slot_num_);
  bthread_mutex_unlock(&mutex_);
#else
  std::unique_lock<std::mutex> lock(mutex_);
  if (available_slot_ids_.empty()) {
    lock.unlock();
    return {};
  }
  const auto slot_id = available_slot_ids_.back();
  available_slot_ids_.pop_back();
  CHECK_LT(slot_id, slot_num_);
  lock.unlock();
#endif
  return ExecutionSlotLease(&entries_[slot_id].slot, this, slot_id,
                            &TpExecutionSlotPool::releaseExecutionSlot);
}

void TpExecutionSlotPool::releaseExecutionSlot(void* owner,
                                               size_t slot_id) noexcept {
  auto* pool = static_cast<TpExecutionSlotPool*>(owner);
#ifndef _WIN32
  bthread_mutex_lock(&pool->mutex_);
  CHECK_LT(slot_id, pool->slot_num_);
  CHECK_LT(pool->available_slot_ids_.size(), pool->slot_num_);
  CHECK_GE(pool->available_slot_ids_.capacity(), pool->slot_num_);
  pool->available_slot_ids_.push_back(slot_id);
  VLOG(10) << "Released slot_id=" << slot_id;
  bthread_cond_signal(&pool->cond_);
  bthread_mutex_unlock(&pool->mutex_);
#else
  {
    std::lock_guard<std::mutex> lock(pool->mutex_);
    CHECK_LT(slot_id, pool->slot_num_);
    CHECK_LT(pool->available_slot_ids_.size(), pool->slot_num_);
    CHECK_GE(pool->available_slot_ids_.capacity(), pool->slot_num_);
    pool->available_slot_ids_.push_back(slot_id);
    VLOG(10) << "Released slot_id=" << slot_id;
  }
  pool->cond_.notify_one();
#endif
}

}  // namespace neug
