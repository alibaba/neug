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
  std::optional<RuntimeBackoff> wait;
  uint64_t observed = operation_gate_state_.load(std::memory_order_relaxed);
  while (true) {
    if (NEUG_UNLIKELY(OperationGateWord::phase(observed) ==
                      AdmissionState::kAllBlocked)) {
      if (!wait) {
        wait.emplace(make_runtime_backoff());
      }
      (*wait)();
      observed = operation_gate_state_.load(std::memory_order_relaxed);
      continue;
    }

    // The returned word linearizes admission with phase changes. The relaxed
    // check above avoids unnecessary RMWs while readers are already blocked;
    // this old-word check closes the race with a concurrent phase transition.
    const uint64_t previous = operation_gate_state_.fetch_add(
        OperationGateWord::kReaderUnit, std::memory_order_acquire);
    const bool counter_exhausted = OperationGateWord::readers(previous) >=
                                   OperationGateWord::kMaxReaderCount;
    if (NEUG_LIKELY(OperationGateWord::phase(previous) !=
                        AdmissionState::kAllBlocked &&
                    !counter_exhausted)) {
      return UnpackPublishedReadView(
          published_read_view_.load(std::memory_order_acquire));
    }

    // Roll back the speculative increment and retry with backoff.
    operation_gate_state_.fetch_sub(OperationGateWord::kReaderUnit,
                                    std::memory_order_release);
    if (NEUG_UNLIKELY(counter_exhausted)) {
      THROW_RUNTIME_ERROR("Reader admission counter exhausted");
    }
    if (!wait) {
      wait.emplace(make_runtime_backoff());
    }
    (*wait)();
    observed = operation_gate_state_.load(std::memory_order_relaxed);
  }
}

void VersionManager::release_read_view() {
  const uint64_t observed =
      operation_gate_state_.load(std::memory_order_relaxed);
  if (NEUG_UNLIKELY(OperationGateWord::readers(observed) == 0)) {
    THROW_INTERNAL_EXCEPTION("release_read_view without admission");
  }
  // A valid caller owns one count, so valid concurrent releases cannot borrow
  // from an adjacent field.
  const uint64_t previous = operation_gate_state_.fetch_sub(
      OperationGateWord::kReaderUnit, std::memory_order_release);
  DCHECK_GT(OperationGateWord::readers(previous), 0U);
}

uint32_t VersionManager::acquire_insert_timestamp() {
  std::optional<RuntimeBackoff> wait;
  uint64_t observed = operation_gate_state_.load(std::memory_order_relaxed);
  while (true) {
    if (NEUG_UNLIKELY(OperationGateWord::phase(observed) !=
                      AdmissionState::kOpen)) {
      if (!wait) {
        wait.emplace(make_runtime_backoff());
      }
      (*wait)();
      observed = operation_gate_state_.load(std::memory_order_relaxed);
      continue;
    }
    if (NEUG_UNLIKELY(OperationGateWord::inserters(observed) ==
                      OperationGateWord::kMaxInserterCount)) {
      THROW_RUNTIME_ERROR("Inserter admission counter exhausted");
    }
    const uint64_t desired = OperationGateWord::increment_inserter(observed);
    // Atomic modification order linearizes admission with phase changes.
    // Acquire is only needed after this RMW succeeds.
    if (operation_gate_state_.compare_exchange_weak(
            observed, desired, std::memory_order_acquire,
            std::memory_order_relaxed)) {
      return write_ts_.fetch_add(1, std::memory_order_acq_rel);
    }
  }
}

void VersionManager::release_insert_timestamp(uint32_t ts) {
  complete_write_timestamp(ts);

  const uint64_t observed =
      operation_gate_state_.load(std::memory_order_relaxed);
  if (NEUG_UNLIKELY(OperationGateWord::inserters(observed) == 0)) {
    THROW_INTERNAL_EXCEPTION("release_insert_timestamp without admission");
  }
  // A valid caller owns one count, so valid concurrent releases cannot borrow
  // from an adjacent field.
  const uint64_t previous = operation_gate_state_.fetch_sub(
      OperationGateWord::kInserterUnit, std::memory_order_release);
  DCHECK_GT(OperationGateWord::inserters(previous), 0U);
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
  std::optional<RuntimeBackoff> wait;
  uint64_t observed = operation_gate_state_.load(std::memory_order_relaxed);
  while (true) {
    if (OperationGateWord::phase(observed) != AdmissionState::kOpen) {
      if (!wait) {
        wait.emplace(make_runtime_backoff());
      }
      (*wait)();
      observed = operation_gate_state_.load(std::memory_order_relaxed);
      continue;
    }
    if (OperationGateWord::try_change_phase(operation_gate_state_, observed,
                                            desired_phase)) {
      return;
    }
  }
}

void VersionManager::transition_admission_phase(AdmissionState expected_phase,
                                                AdmissionState desired_phase) {
  uint64_t observed = operation_gate_state_.load(std::memory_order_relaxed);
  while (true) {
    if (OperationGateWord::phase(observed) != expected_phase) {
      THROW_INTERNAL_EXCEPTION("Invalid transaction admission transition");
    }
    if (OperationGateWord::try_change_phase(operation_gate_state_, observed,
                                            desired_phase)) {
      return;
    }
  }
}

void VersionManager::wait_for_readers_to_drain() {
  uint64_t observed = operation_gate_state_.load(std::memory_order_acquire);
  if (OperationGateWord::readers(observed) == 0) {
    return;
  }

  RuntimeBackoff wait = make_runtime_backoff();
  do {
    wait();
    observed = operation_gate_state_.load(std::memory_order_acquire);
  } while (OperationGateWord::readers(observed) != 0);
}

void VersionManager::wait_for_inserters_to_drain() {
  uint64_t observed = operation_gate_state_.load(std::memory_order_acquire);
  if (OperationGateWord::inserters(observed) == 0) {
    return;
  }

  RuntimeBackoff wait = make_runtime_backoff();
  do {
    wait();
    observed = operation_gate_state_.load(std::memory_order_acquire);
  } while (OperationGateWord::inserters(observed) != 0);
}

RuntimeWaitFn VersionManager::runtime_wait_impl() const noexcept {
  return runtime_wait_.load(std::memory_order_acquire);
}

uint32_t VersionManager::acquire_update_timestamp() {
  enter_admission_phase(AdmissionState::kInsertsBlocked);
  wait_for_inserters_to_drain();

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
  wait_for_readers_to_drain();
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
  uint64_t observed = operation_gate_state_.load(std::memory_order_acquire);
  while (true) {
    const AdmissionState phase = OperationGateWord::phase(observed);
    DCHECK(phase == AdmissionState::kInsertsBlocked ||
           phase == AdmissionState::kAllBlocked)
        << "Update released outside update state";
    if (OperationGateWord::try_change_phase(operation_gate_state_, observed,
                                            AdmissionState::kOpen)) {
      return;
    }
  }
}

uint32_t VersionManager::acquire_compact_timestamp() {
  enter_admission_phase(AdmissionState::kAllBlocked);
  wait_for_readers_to_drain();
  wait_for_inserters_to_drain();

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
