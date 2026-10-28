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
#include "neug/server/service_transport.h"

#include <condition_variable>
#include <mutex>

namespace neug {

namespace {
class NativeSlotSynchronizer final : public IExecutionSlotSynchronizer {
 public:
  void lock() noexcept override { mutex_.lock(); }
  void unlock() noexcept override { mutex_.unlock(); }
  void Wait() noexcept override {
    std::unique_lock lock(mutex_, std::adopt_lock);
    cond_.wait(lock);
    lock.release();
  }
  void NotifyOne() noexcept override { cond_.notify_one(); }

 private:
  std::mutex mutex_;
  std::condition_variable cond_;
};
}  // namespace

std::unique_ptr<IExecutionSlotSynchronizer> CreateNativeSlotSynchronizer() {
  return std::make_unique<NativeSlotSynchronizer>();
}

std::unique_ptr<IExecutionSlotSynchronizer>
IServiceTransport::CreateSlotSynchronizer() const {
  return CreateNativeSlotSynchronizer();
}

ExecutionSlotLease TpExecutionSlotPool::AcquireExecutionSlot() {
  std::lock_guard lock(*synchronizer_);
  while (available_slot_ids_.empty()) {
    synchronizer_->Wait();
  }
  const auto slot_id = available_slot_ids_.back();
  available_slot_ids_.pop_back();
  CHECK_LT(slot_id, slot_num_);
  return ExecutionSlotLease(&entries_[slot_id].slot, this, slot_id,
                            &TpExecutionSlotPool::releaseExecutionSlot);
}

ExecutionSlotLease TpExecutionSlotPool::TryAcquireExecutionSlot() {
  std::lock_guard lock(*synchronizer_);
  if (available_slot_ids_.empty()) {
    return {};
  }
  const auto slot_id = available_slot_ids_.back();
  available_slot_ids_.pop_back();
  CHECK_LT(slot_id, slot_num_);
  return ExecutionSlotLease(&entries_[slot_id].slot, this, slot_id,
                            &TpExecutionSlotPool::releaseExecutionSlot);
}

void TpExecutionSlotPool::releaseExecutionSlot(void* owner,
                                               size_t slot_id) noexcept {
  auto* pool = static_cast<TpExecutionSlotPool*>(owner);
  {
    std::lock_guard lock(*pool->synchronizer_);
    CHECK_LT(slot_id, pool->slot_num_);
    CHECK_LT(pool->available_slot_ids_.size(), pool->slot_num_);
    CHECK_GE(pool->available_slot_ids_.capacity(), pool->slot_num_);
    pool->available_slot_ids_.push_back(slot_id);
    pool->synchronizer_->NotifyOne();
  }
  VLOG(10) << "Released slot_id=" << slot_id;
}

}  // namespace neug
