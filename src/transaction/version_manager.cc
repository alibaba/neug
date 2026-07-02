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

#include <glog/logging.h>
#include <ostream>
#include <thread>

#include "neug/utils/bitset.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/likely.h"

namespace neug {

// VersionManager implementation

VersionManager::VersionManager() {}

VersionManager::~VersionManager() {}

void VersionManager::init_ts(uint32_t ts, int thread_num) {
  write_ts_.store(ts + 1, std::memory_order_relaxed);
  read_ts_.store(ts, std::memory_order_relaxed);
  active_readers_.store(0, std::memory_order_relaxed);
  active_inserters_.store(0, std::memory_order_relaxed);
  update_state_.store(0, std::memory_order_relaxed);

  ts_window_.init();
  thread_num_ = thread_num;
}

uint32_t VersionManager::acquire_read_timestamp() {
  // Pre-check: avoid incrementing if in commit phase
  int state = update_state_.load(std::memory_order_acquire);
  if (NEUG_UNLIKELY(state == 2)) {
    return acquire_read_timestamp_slow();
  }

  // Optimistically increment counter
  active_readers_.fetch_add(1, std::memory_order_acq_rel);

  // Double-check: ensure commit didn't start between pre-check and increment.
  // This eliminates the ABA race where a reader increments active_readers_
  // but misses a concurrent update_state_ 0->1->2 transition.
  state = update_state_.load(std::memory_order_acquire);
  if (NEUG_LIKELY(state != 2)) {
    return read_ts_.load(std::memory_order_acquire);
  }

  // Rollback: commit started while we were incrementing
  active_readers_.fetch_sub(1, std::memory_order_acq_rel);

  // Slow path
  return acquire_read_timestamp_slow();
}

uint32_t VersionManager::acquire_read_timestamp_slow() {
  // Spin wait until update commit completes
  while (update_state_.load(std::memory_order_acquire) == 2) {
    // Tight spin loop for minimal latency
  }

  // Retry
  return acquire_read_timestamp();
}

void VersionManager::release_read_timestamp() {
  active_readers_.fetch_sub(1, std::memory_order_acq_rel);
}

uint32_t VersionManager::acquire_insert_timestamp() {
  // Check state first (fast path)
  int state = update_state_.load(std::memory_order_acquire);
  if (NEUG_UNLIKELY(state != 0)) {
    return acquire_insert_timestamp_slow();
  }

  // Increment counter
  active_inserters_.fetch_add(1, std::memory_order_acq_rel);

  // Double check: ensure update didn't start between checks
  state = update_state_.load(std::memory_order_acquire);
  if (NEUG_LIKELY(state == 0)) {
    return write_ts_.fetch_add(1, std::memory_order_acq_rel);
  }

  // Slow path: update just started
  active_inserters_.fetch_sub(1, std::memory_order_acq_rel);
  return acquire_insert_timestamp_slow();
}

uint32_t VersionManager::acquire_insert_timestamp_slow() {
  // Spin wait until update completes
  while (update_state_.load(std::memory_order_acquire) != 0) {
    // Tight spin loop for minimal latency
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
  lock_.lock();
  uint32_t current_read_ts = read_ts_.load(std::memory_order_relaxed);
  if (ts == current_read_ts + 1) {
    // May need to advance, safe under lock protection
    advance_read_ts_locked();
  }
  lock_.unlock();
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
}

uint32_t VersionManager::acquire_update_timestamp() {
  // Wait to enter update state (0 -> 1)
  while (true) {
    int expected = 0;
    if (update_state_.compare_exchange_strong(expected, 1,
                                              std::memory_order_acq_rel,
                                              std::memory_order_acquire)) {
      break;  // Successfully entered update execution phase
    }
    // Tight spin loop for minimal latency
  }

  // Wait for all active insert transactions to finish
  while (active_inserters_.load(std::memory_order_acquire) > 0) {
    // Tight spin loop for minimal latency
  }

  return write_ts_.fetch_add(1, std::memory_order_acq_rel);
}

void VersionManager::begin_update_commit(uint32_t ts) {
  (void) ts;

  // Enter commit state (1 -> 2) — blocks new reads, does NOT wait for existing
  // readers. Use seq_cst to ensure the store is globally visible before we
  // proceed, which closes the ABA window for concurrent acquire_read_timestamp
  // callers that may have passed their pre-check but not yet reached their
  // double-check.
  update_state_.store(2, std::memory_order_seq_cst);

  // Drain ABA-window readers: any reader that incremented active_readers_
  // just before this store will see update_state_==2 in their double-check
  // and roll back (decrement active_readers_). We wait until active_readers_
  // stops decreasing to ensure all such readers have completed their rollback.
  // This does NOT wait for legitimate existing readers — they may hold their
  // timestamps for an arbitrarily long time. The drain is fast because ABA-
  // window readers roll back within nanoseconds (one atomic decrement).
  int prev = active_readers_.load(std::memory_order_acquire);
  while (true) {
    int curr = active_readers_.load(std::memory_order_acquire);
    if (curr >= prev) {
      break;
    }
    prev = curr;
  }
}

void VersionManager::release_update_timestamp(uint32_t ts) {
  complete_write_timestamp(ts);

  // Restore to normal state (1 -> 0 or 2 -> 0)
  update_state_.store(0, std::memory_order_release);
}

uint32_t VersionManager::acquire_compact_timestamp() {
  // Wait to enter compact state (0 -> 2)
  while (true) {
    int expected = 0;
    if (update_state_.compare_exchange_strong(expected, 2,
                                              std::memory_order_acq_rel,
                                              std::memory_order_acquire)) {
      break;  // Successfully entered compact phase
    }
    // Tight spin loop for minimal latency
  }

  // Wait for all active insert transactions to finish
  while (active_inserters_.load(std::memory_order_acquire) > 0) {
    // Tight spin loop for minimal latency
  }

  // Wait for all active readers to finish — compact rewrites storage
  // timestamps.
  while (active_readers_.load(std::memory_order_acquire) > 0) {
    // Tight spin loop for minimal latency
  }

  return write_ts_.fetch_add(1, std::memory_order_acq_rel);
}

void VersionManager::release_compact_timestamp(uint32_t ts) {
  // Compact must be in state 2
  if (update_state_.load(std::memory_order_acquire) != 2) {
    THROW_INTERNAL_EXCEPTION(
        "release_compact_timestamp called while not in compact state");
  }

  complete_write_timestamp(ts);

  // Restore to normal state (2 -> 0)
  update_state_.store(0, std::memory_order_release);
}

void VersionManager::revert_compact_timestamp(uint32_t ts) {
  // Compact must be in state 2
  if (update_state_.load(std::memory_order_acquire) != 2) {
    THROW_INTERNAL_EXCEPTION(
        "revert_compact_timestamp called while not in compact state");
  }

  // Close the timestamp gap so later commits can advance read_ts_.
  complete_write_timestamp(ts);

  // Revert to normal state (2 -> 0)
  update_state_.store(0, std::memory_order_release);
}

}  // namespace neug
