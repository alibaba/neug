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

#include "neug/transaction/version_manager.h"

#include <limits>
#include <mutex>

#include "neug/utils/exception/exception.h"
#include "neug/utils/likely.h"

namespace neug {

// VersionManager implementation

VersionManager::VersionManager() : runtime_wait_(&NativeRuntimeWait) {}

void VersionManager::init_ts(PublishedReadView initial_read_view,
                             int thread_num) {
  const uint32_t ts = initial_read_view.visibility_ts;
  if (ts == std::numeric_limits<uint32_t>::max()) {
    THROW_RUNTIME_ERROR(
        "Transaction timestamp space exhausted; checkpoint/reset the timeline "
        "before reopening the database");
  }
  write_ts_.store(ts + 1, std::memory_order_relaxed);
  read_ts_.store(ts, std::memory_order_relaxed);
  installed_snapshot_generation_.store(initial_read_view.snapshot_generation,
                                       std::memory_order_relaxed);
  published_read_view_.store(PackPublishedReadView(initial_read_view),
                             std::memory_order_relaxed);
  active_readers_.store(0, std::memory_order_relaxed);
  active_inserters_.store(0, std::memory_order_relaxed);
  admission_state_.store(AdmissionState::kOpen, std::memory_order_relaxed);

  ts_window_.init();
  thread_num_ = thread_num;
}

bool VersionManager::try_set_runtime_wait_if_quiescent(
    RuntimeWaitFn runtime_wait) noexcept {
  if (runtime_wait == nullptr) {
    return false;
  }
  std::unique_lock lock(lock_, std::try_to_lock);
  if (!lock.owns_lock()) {
    return false;
  }

  AdmissionState expected = AdmissionState::kOpen;
  if (!admission_state_.compare_exchange_strong(
          expected, AdmissionState::kAllBlocked, std::memory_order_seq_cst,
          std::memory_order_acquire)) {
    return false;
  }

  // The seq_cst admission transition participates in the same total order as
  // transaction counter increments and their post-admission checks. Therefore
  // either an entrant observes kAllBlocked and rolls back, or this check
  // observes its increment.
  const bool quiescent = active_readers_.load(std::memory_order_seq_cst) == 0 &&
                         active_inserters_.load(std::memory_order_seq_cst) == 0;
  if (quiescent) {
    runtime_wait_.store(runtime_wait, std::memory_order_release);
  }
  admission_state_.store(AdmissionState::kOpen, std::memory_order_release);
  return quiescent;
}

PublishedReadView VersionManager::acquire_read_view() {
  // Pre-check: avoid incrementing if in commit phase
  auto state = admission_state_.load(std::memory_order_acquire);
  if (NEUG_UNLIKELY(state == AdmissionState::kAllBlocked)) {
    return acquire_read_view_slow();
  }

  // Optimistically increment counter
  active_readers_.fetch_add(1, std::memory_order_seq_cst);

  // Double-check: ensure commit didn't start between pre-check and increment.
  // This eliminates the ABA race where a reader increments active_readers_
  // but misses a concurrent transition to all-blocked.
  state = admission_state_.load(std::memory_order_seq_cst);
  if (NEUG_LIKELY(state != AdmissionState::kAllBlocked)) {
    return UnpackPublishedReadView(
        published_read_view_.load(std::memory_order_acquire));
  }

  // Rollback: commit started while we were incrementing
  active_readers_.fetch_sub(1, std::memory_order_acq_rel);

  // Slow path
  return acquire_read_view_slow();
}

PublishedReadView VersionManager::acquire_read_view_slow() {
  RuntimeBackoff wait = make_runtime_backoff();
  while (admission_state_.load(std::memory_order_acquire) ==
         AdmissionState::kAllBlocked) {
    wait();
  }

  // Retry
  return acquire_read_view();
}

void VersionManager::release_read_view() {
  active_readers_.fetch_sub(1, std::memory_order_acq_rel);
}

uint32_t VersionManager::acquire_insert_timestamp() {
  // Check state first (fast path)
  auto state = admission_state_.load(std::memory_order_acquire);
  if (NEUG_UNLIKELY(state != AdmissionState::kOpen)) {
    return acquire_insert_timestamp_slow();
  }

  // Increment counter
  active_inserters_.fetch_add(1, std::memory_order_seq_cst);

  // Double check: ensure update didn't start between checks
  state = admission_state_.load(std::memory_order_seq_cst);
  if (NEUG_LIKELY(state == AdmissionState::kOpen)) {
    return write_ts_.fetch_add(1, std::memory_order_acq_rel);
  }

  // Slow path: update just started
  active_inserters_.fetch_sub(1, std::memory_order_acq_rel);
  return acquire_insert_timestamp_slow();
}

uint32_t VersionManager::acquire_insert_timestamp_slow() {
  RuntimeBackoff wait = make_runtime_backoff();
  while (admission_state_.load(std::memory_order_acquire) !=
         AdmissionState::kOpen) {
    wait();
  }

  // Retry
  return acquire_insert_timestamp();
}

void VersionManager::release_insert_timestamp(uint32_t ts) {
  complete_write_timestamp(ts);

  // Decrement active inserter count
  active_inserters_.fetch_sub(1, std::memory_order_acq_rel);
}

void VersionManager::complete_write_timestamp(uint32_t ts) {
  // Mark completion (lock-free atomic operation)
  ts_window_.mark_completed(ts);

  // Check under lock: only advance if ts == read_ts + 1
  std::unique_lock lock(lock_, std::defer_lock);
  if (!lock.try_lock()) {
    RuntimeBackoff wait = make_runtime_backoff();
    do {
      wait();
    } while (!lock.try_lock());
  }
  uint32_t current_read_ts = read_ts_.load(std::memory_order_relaxed);
  if (ts == current_read_ts + 1) {
    // May need to advance, safe under lock protection
    advance_read_ts_locked();
  }
}

void VersionManager::advance_read_ts_locked() {
  uint32_t current = read_ts_.load(std::memory_order_relaxed);

  // Advance read_ts
  while (true) {
    uint32_t next_ts = current + 1;

    if (!ts_window_.is_completed(next_ts)) {
      break;  // Next timestamp not completed
    }

    // Clear the advanced bit
    ts_window_.clear(next_ts);
    current = next_ts;
    read_ts_.store(current, std::memory_order_release);
  }

  // Sliding window maintenance
  ts_window_.slide_window(current);
  published_read_view_.store(
      PackPublishedReadView({current, installed_snapshot_generation_.load(
                                          std::memory_order_relaxed)}),
      std::memory_order_release);
}

void VersionManager::enter_exclusive_state(AdmissionState desired_state) {
  AdmissionState expected = AdmissionState::kOpen;
  if (admission_state_.compare_exchange_strong(expected, desired_state,
                                               std::memory_order_seq_cst,
                                               std::memory_order_acquire)) {
    return;
  }

  RuntimeBackoff wait = make_runtime_backoff();
  do {
    wait();
    expected = AdmissionState::kOpen;
  } while (!admission_state_.compare_exchange_strong(
      expected, desired_state, std::memory_order_seq_cst,
      std::memory_order_acquire));
}

void VersionManager::wait_until_zero(const std::atomic<int>& counter) {
  if (counter.load(std::memory_order_seq_cst) <= 0) {
    return;
  }

  RuntimeBackoff wait = make_runtime_backoff();
  do {
    wait();
  } while (counter.load(std::memory_order_seq_cst) > 0);
}

RuntimeWaitFn VersionManager::runtime_wait_impl() const noexcept {
  return runtime_wait_.load(std::memory_order_acquire);
}

uint32_t VersionManager::acquire_update_timestamp() {
  enter_exclusive_state(AdmissionState::kInsertsBlocked);
  wait_until_zero(active_inserters_);

  return write_ts_.fetch_add(1, std::memory_order_acq_rel);
}

void VersionManager::begin_update_commit(uint32_t ts) {
  (void) ts;

  // Block new readers before publishing the new snapshot. Existing readers
  // keep using their pinned snapshots; readers in the acquisition window
  // either roll back after observing this state or complete as existing
  // readers.
  admission_state_.store(AdmissionState::kAllBlocked,
                         std::memory_order_seq_cst);
}

void VersionManager::drain_readers() {
  if (admission_state_.load(std::memory_order_acquire) !=
      AdmissionState::kAllBlocked) {
    THROW_INTERNAL_EXCEPTION(
        "drain_readers called while readers are not blocked");
  }
  wait_until_zero(active_readers_);
}

void VersionManager::finish_update_timestamp(
    uint32_t ts,
    std::optional<uint32_t> installed_snapshot_generation) noexcept {
  if (installed_snapshot_generation) {
    installed_snapshot_generation_.store(*installed_snapshot_generation,
                                         std::memory_order_relaxed);
  }
  complete_write_timestamp(ts);

  // Timestamp completion and any matching snapshot generation are visible
  // before new readers and inserters are admitted.
  admission_state_.store(AdmissionState::kOpen, std::memory_order_release);
}

uint32_t VersionManager::acquire_compact_timestamp() {
  enter_exclusive_state(AdmissionState::kAllBlocked);
  wait_until_zero(active_inserters_);
  // Compact rewrites storage timestamps, so existing readers must also drain.
  wait_until_zero(active_readers_);

  return write_ts_.fetch_add(1, std::memory_order_acq_rel);
}

void VersionManager::release_compact_timestamp(uint32_t ts) {
  // Compact must have blocked new readers.
  if (admission_state_.load(std::memory_order_acquire) !=
      AdmissionState::kAllBlocked) {
    THROW_INTERNAL_EXCEPTION(
        "release_compact_timestamp called while not in compact state");
  }

  complete_write_timestamp(ts);

  // Restore normal operation.
  admission_state_.store(AdmissionState::kOpen, std::memory_order_release);
}

void VersionManager::revert_compact_timestamp(uint32_t ts) {
  // Compact must have blocked new readers.
  if (admission_state_.load(std::memory_order_acquire) !=
      AdmissionState::kAllBlocked) {
    THROW_INTERNAL_EXCEPTION(
        "revert_compact_timestamp called while not in compact state");
  }

  // Close the timestamp gap so later commits can advance read_ts_.
  complete_write_timestamp(ts);

  // Restore normal operation.
  admission_state_.store(AdmissionState::kOpen, std::memory_order_release);
}

}  // namespace neug
