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
#include <limits>
#include <mutex>

#include "neug/utils/exception/exception.h"
#include "neug/utils/likely.h"

namespace neug {

// VersionManager implementation

VersionManager::VersionManager() : runtime_wait_(&NativeRuntimeWait) {}

void VersionManager::wait_for_gate_state_change(uint64_t observed) const {
  RuntimeBackoff wait = make_runtime_backoff();
  while (operation_gate_state_.load(std::memory_order_acquire) == observed) {
    wait();
  }
}

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
  operation_gate_state_.store(OperationGateWord::empty(AdmissionState::kOpen),
                              std::memory_order_relaxed);

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

  uint64_t expected = OperationGateWord::empty(AdmissionState::kOpen);
  const uint64_t blocked =
      OperationGateWord::empty(AdmissionState::kAllBlocked);
  if (!operation_gate_state_.compare_exchange_strong(
          expected, blocked, std::memory_order_acq_rel,
          std::memory_order_acquire)) {
    return false;
  }

  runtime_wait_.store(runtime_wait, std::memory_order_release);
  operation_gate_state_.store(OperationGateWord::empty(AdmissionState::kOpen),
                              std::memory_order_release);
  return true;
}

PublishedReadView VersionManager::acquire_read_view() {
  uint64_t observed = operation_gate_state_.load(std::memory_order_acquire);
  while (true) {
    if (NEUG_UNLIKELY(OperationGateWord::phase(observed) ==
                      AdmissionState::kAllBlocked)) {
      wait_for_gate_state_change(observed);
      observed = operation_gate_state_.load(std::memory_order_acquire);
      continue;
    }
    if (NEUG_UNLIKELY(OperationGateWord::readers(observed) ==
                      OperationGateWord::kMaxCount)) {
      THROW_RUNTIME_ERROR("Reader admission counter exhausted");
    }

    const uint64_t desired = OperationGateWord::increment_reader(observed);
    if (operation_gate_state_.compare_exchange_weak(
            observed, desired, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return UnpackPublishedReadView(
          published_read_view_.load(std::memory_order_acquire));
    }
  }
}

void VersionManager::release_read_view() {
  uint64_t observed = operation_gate_state_.load(std::memory_order_acquire);
  while (true) {
    if (NEUG_UNLIKELY(OperationGateWord::readers(observed) == 0)) {
      THROW_INTERNAL_EXCEPTION("release_read_view without admission");
    }
    const uint64_t desired = OperationGateWord::decrement_reader(observed);
    if (operation_gate_state_.compare_exchange_weak(
            observed, desired, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return;
    }
  }
}

uint32_t VersionManager::acquire_insert_timestamp() {
  uint64_t observed = operation_gate_state_.load(std::memory_order_acquire);
  while (true) {
    if (NEUG_UNLIKELY(OperationGateWord::phase(observed) !=
                      AdmissionState::kOpen)) {
      wait_for_gate_state_change(observed);
      observed = operation_gate_state_.load(std::memory_order_acquire);
      continue;
    }
    if (NEUG_UNLIKELY(OperationGateWord::inserters(observed) ==
                      OperationGateWord::kMaxCount)) {
      THROW_RUNTIME_ERROR("Inserter admission counter exhausted");
    }

    const uint64_t desired = OperationGateWord::increment_inserter(observed);
    if (operation_gate_state_.compare_exchange_weak(
            observed, desired, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return write_ts_.fetch_add(1, std::memory_order_acq_rel);
    }
  }
}

void VersionManager::release_insert_timestamp(uint32_t ts) {
  complete_write_timestamp(ts);

  uint64_t observed = operation_gate_state_.load(std::memory_order_acquire);
  while (true) {
    if (NEUG_UNLIKELY(OperationGateWord::inserters(observed) == 0)) {
      THROW_INTERNAL_EXCEPTION("release_insert_timestamp without admission");
    }
    const uint64_t desired = OperationGateWord::decrement_inserter(observed);
    if (operation_gate_state_.compare_exchange_weak(
            observed, desired, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return;
    }
  }
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

void VersionManager::enter_admission_phase(AdmissionState desired_phase) {
  uint64_t observed = operation_gate_state_.load(std::memory_order_acquire);
  while (true) {
    if (OperationGateWord::phase(observed) != AdmissionState::kOpen) {
      wait_for_gate_state_change(observed);
      observed = operation_gate_state_.load(std::memory_order_acquire);
      continue;
    }
    const uint64_t desired =
        OperationGateWord::with_phase(observed, desired_phase);
    if (operation_gate_state_.compare_exchange_weak(
            observed, desired, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return;
    }
  }
}

void VersionManager::transition_admission_phase(AdmissionState expected_phase,
                                                AdmissionState desired_phase) {
  uint64_t observed = operation_gate_state_.load(std::memory_order_acquire);
  while (true) {
    if (OperationGateWord::phase(observed) != expected_phase) {
      THROW_INTERNAL_EXCEPTION("Invalid transaction admission transition");
    }
    const uint64_t desired =
        OperationGateWord::with_phase(observed, desired_phase);
    if (operation_gate_state_.compare_exchange_weak(
            observed, desired, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return;
    }
  }
}

void VersionManager::reopen_admission_after_update() {
  uint64_t observed = operation_gate_state_.load(std::memory_order_acquire);
  while (true) {
    const AdmissionState phase = OperationGateWord::phase(observed);
    if (phase != AdmissionState::kInsertsBlocked &&
        phase != AdmissionState::kAllBlocked) {
      THROW_INTERNAL_EXCEPTION("Update released outside update state");
    }
    const uint64_t desired =
        OperationGateWord::with_phase(observed, AdmissionState::kOpen);
    if (operation_gate_state_.compare_exchange_weak(
            observed, desired, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return;
    }
  }
}

void VersionManager::wait_until_operations_drained(bool readers,
                                                   bool inserters) {
  while (true) {
    const uint64_t observed =
        operation_gate_state_.load(std::memory_order_acquire);
    if ((!readers || OperationGateWord::readers(observed) == 0) &&
        (!inserters || OperationGateWord::inserters(observed) == 0)) {
      return;
    }
    wait_for_gate_state_change(observed);
  }
}

RuntimeWaitFn VersionManager::runtime_wait_impl() const noexcept {
  return runtime_wait_.load(std::memory_order_acquire);
}

uint32_t VersionManager::acquire_update_timestamp() {
  enter_admission_phase(AdmissionState::kInsertsBlocked);
  wait_until_operations_drained(false, true);

  return write_ts_.fetch_add(1, std::memory_order_acq_rel);
}

void VersionManager::begin_update_commit(uint32_t ts) {
  (void) ts;

  // Block new readers before publishing the new snapshot. Existing readers
  // keep using their pinned snapshots; a racing reader is either counted
  // before this transition or rejected after it.
  transition_admission_phase(AdmissionState::kInsertsBlocked,
                             AdmissionState::kAllBlocked);
}

void VersionManager::drain_readers() {
  if (OperationGateWord::phase(operation_gate_state_.load(
          std::memory_order_acquire)) != AdmissionState::kAllBlocked) {
    THROW_INTERNAL_EXCEPTION(
        "drain_readers called while readers are not blocked");
  }
  wait_until_operations_drained(true, false);
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
  // through published_read_view_ before the packed gate admits new work.
  reopen_admission_after_update();
}

uint32_t VersionManager::acquire_compact_timestamp() {
  enter_admission_phase(AdmissionState::kAllBlocked);
  wait_until_operations_drained(true, true);

  return write_ts_.fetch_add(1, std::memory_order_acq_rel);
}

void VersionManager::release_compact_timestamp(uint32_t ts) {
  // Compact must have blocked new readers.
  if (OperationGateWord::phase(operation_gate_state_.load(
          std::memory_order_acquire)) != AdmissionState::kAllBlocked) {
    THROW_INTERNAL_EXCEPTION(
        "release_compact_timestamp called while not in compact state");
  }

  complete_write_timestamp(ts);

  // Restore normal operation.
  transition_admission_phase(AdmissionState::kAllBlocked,
                             AdmissionState::kOpen);
}

void VersionManager::revert_compact_timestamp(uint32_t ts) {
  // Compact must have blocked new readers.
  if (OperationGateWord::phase(operation_gate_state_.load(
          std::memory_order_acquire)) != AdmissionState::kAllBlocked) {
    THROW_INTERNAL_EXCEPTION(
        "revert_compact_timestamp called while not in compact state");
  }

  // Close the timestamp gap so later commits can advance read_ts_.
  complete_write_timestamp(ts);

  // Restore normal operation.
  transition_admission_phase(AdmissionState::kAllBlocked,
                             AdmissionState::kOpen);
}

}  // namespace neug
