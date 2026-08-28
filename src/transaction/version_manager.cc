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
#include <chrono>
#include <limits>
#include <mutex>
#include <ostream>
#include <thread>

#include "neug/utils/exception/exception.h"
#include "neug/utils/likely.h"

namespace neug {

namespace {

using AdmissionState = detail::AdmissionState;
using OperationGateWord = detail::OperationGateWord;

bool DeadlineExpired(std::chrono::steady_clock::time_point deadline) noexcept {
  return std::chrono::steady_clock::now() >= deadline;
}

enum class TimestampReservationState { kReserved, kWindowFull, kExhausted };

struct TimestampReservation {
  TimestampReservationState state;
  uint32_t timestamp{0};
};

TimestampReservation ReserveWriteTimestamp(
    std::atomic<uint32_t>& write_ts, const std::atomic<uint32_t>& read_ts) {
  uint32_t candidate = write_ts.load(std::memory_order_relaxed);
  while (true) {
    if (candidate == std::numeric_limits<uint32_t>::max()) {
      return {TimestampReservationState::kExhausted};
    }

    const uint32_t current_read_ts = read_ts.load(std::memory_order_acquire);
    const uint64_t outstanding = static_cast<uint64_t>(candidate) -
                                 static_cast<uint64_t>(current_read_ts);
    if (NEUG_UNLIKELY(outstanding > TimestampWindow::kWindowSize)) {
      if (candidate <= current_read_ts) {
        // A failed CAS leaves candidate at the then-current write_ts, but
        // another inserter may allocate and complete that timestamp before
        // this load of read_ts. The acquire load above orders this refresh
        // after frontier publication.
        candidate = write_ts.load(std::memory_order_relaxed);
        if (NEUG_UNLIKELY(candidate <= current_read_ts)) {
          THROW_INTERNAL_EXCEPTION(
              "Write timestamp reservation invariant broken after refresh: "
              "write_ts=" +
              std::to_string(candidate) + " must be greater than read_ts=" +
              std::to_string(current_read_ts));
        }
        continue;
      }
      return {TimestampReservationState::kWindowFull};
    }

    if (write_ts.compare_exchange_weak(candidate, candidate + 1,
                                       std::memory_order_acq_rel,
                                       std::memory_order_relaxed)) {
      return {TimestampReservationState::kReserved, candidate};
    }
    RuntimeCpuRelax();
  }
}

[[noreturn]] void throw_timestamp_reservation_failure(
    TimestampReservationState state, uint32_t read_ts, uint32_t write_ts) {
  if (state == TimestampReservationState::kWindowFull) {
    THROW_INTERNAL_EXCEPTION(
        "TimestampWindow invariant broken: write timestamp reservation found "
        "the window full despite exclusive write admission (read_ts=" +
        std::to_string(read_ts) + ", write_ts=" + std::to_string(write_ts) +
        ", window_size=" + std::to_string(TimestampWindow::kWindowSize) +
        "); this indicates admission/window bookkeeping corruption, not "
        "recoverable backpressure");
  }
  THROW_RUNTIME_ERROR(
      "Transaction timestamp space exhausted; checkpoint/reset the timeline "
      "before reopening the database");
}

}  // namespace

VersionManager::VersionManager() = default;

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
  gate_.reset_open();

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

  if (!gate_.try_seal_open_state()) {
    return false;
  }

  gate_.store_runtime_wait(runtime_wait);
  gate_.unseal_to_open();
  return true;
}

ReadOperationLease VersionManager::acquire_read_operation() {
  SharedOperationLease admission(gate_);
  const auto published_view = UnpackPublishedReadView(
      published_read_view_.load(std::memory_order_acquire));
  return {published_view, std::move(admission)};
}

uint32_t VersionManager::acquire_insert_timestamp() {
  std::optional<RuntimeBackoff> wait;
  while (true) {
    gate_.acquire_inserter();
    const auto reservation = ReserveWriteTimestamp(write_ts_, read_ts_);
    if (reservation.state == TimestampReservationState::kReserved) {
      return reservation.timestamp;
    }

    // A waiter that has not reserved a timestamp must not keep insert
    // admission while waiting for window capacity: an update would otherwise
    // close admission and wait for this count forever.
    gate_.release_inserter();
    if (reservation.state == TimestampReservationState::kExhausted) {
      throw_timestamp_reservation_failure(
          reservation.state, read_ts_.load(std::memory_order_relaxed),
          write_ts_.load(std::memory_order_relaxed));
    }
    if (!wait) {
      wait.emplace(make_runtime_backoff());
    }
    (*wait)();
  }
}

void VersionManager::release_insert_timestamp(uint32_t ts) {
  complete_write_timestamp(ts);

  gate_.release_inserter();
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

  published_read_view_.store(
      PackPublishedReadView({current, installed_snapshot_generation_.load(
                                          std::memory_order_relaxed)}),
      std::memory_order_release);
}

RuntimeWaitFn VersionManager::runtime_wait_impl() const noexcept {
  return gate_.runtime_wait();
}

uint32_t VersionManager::reserve_update_timestamp() {
  const auto reservation = ReserveWriteTimestamp(write_ts_, read_ts_);
  if (reservation.state == TimestampReservationState::kReserved) {
    return reservation.timestamp;
  }
  const uint32_t current_read_ts = read_ts_.load(std::memory_order_relaxed);
  const uint32_t current_write_ts = write_ts_.load(std::memory_order_relaxed);
  gate_.transition_phase(AdmissionState::kInsertsBlocked,
                         AdmissionState::kOpen);
  throw_timestamp_reservation_failure(reservation.state, current_read_ts,
                                      current_write_ts);
}

uint32_t VersionManager::acquire_update_timestamp() {
  gate_.enter_phase(AdmissionState::kInsertsBlocked);
  gate_.wait_inserters_drained();
  return reserve_update_timestamp();
}

uint32_t VersionManager::acquire_update_timestamp_until(
    std::chrono::steady_clock::time_point deadline) {
  if (!gate_.enter_phase_until(AdmissionState::kInsertsBlocked, deadline)) {
    THROW_TRANSACTION_TIMEOUT("waiting for update admission");
  }

  if (!gate_.wait_inserters_drained_until(deadline)) {
    gate_.transition_phase(AdmissionState::kInsertsBlocked,
                           AdmissionState::kOpen);
    THROW_TRANSACTION_TIMEOUT("waiting for active inserts to finish");
  }
  if (DeadlineExpired(deadline)) {
    gate_.transition_phase(AdmissionState::kInsertsBlocked,
                           AdmissionState::kOpen);
    THROW_TRANSACTION_TIMEOUT("reserving update timestamp");
  }
  return reserve_update_timestamp();
}

bool VersionManager::try_acquire_update_timestamp(uint32_t& timestamp) {
  if (!gate_.try_enter_phase(AdmissionState::kInsertsBlocked)) {
    return false;
  }
  const uint64_t gate = gate_.load_acquire();
  if (OperationGateWord::inserters(gate) != 0) {
    gate_.reopen();
    return false;
  }
  timestamp = reserve_update_timestamp();
  return true;
}

void VersionManager::begin_update_commit(uint32_t ts) {
  (void) ts;

  // Block new readers before publishing the new snapshot. Existing readers
  // keep using their pinned snapshots; a racing reader is either counted
  // before this transition or rejected after it.
  gate_.transition_phase(AdmissionState::kInsertsBlocked,
                         AdmissionState::kAllBlocked);
}

void VersionManager::drain_readers() {
  if (OperationGateWord::phase(gate_.load_acquire()) !=
      AdmissionState::kAllBlocked) {
    THROW_INTERNAL_EXCEPTION(
        "drain_readers called while readers are not blocked");
  }
  gate_.wait_readers_drained();
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
  gate_.reopen();
}

void VersionManager::finish_update_and_reset_timeline(uint32_t ts) noexcept {
  const uint64_t gate = gate_.load_acquire();
  CHECK(OperationGateWord::phase(gate) == AdmissionState::kAllBlocked);
  CHECK_EQ(OperationGateWord::inserters(gate), 0U);
  CHECK_EQ(write_ts_.load(std::memory_order_acquire), ts + 1);

  write_ts_.store(1, std::memory_order_relaxed);
  read_ts_.store(0, std::memory_order_relaxed);
  ts_window_.init();
  published_read_view_.store(
      PackPublishedReadView(
          {0, installed_snapshot_generation_.load(std::memory_order_relaxed)}),
      std::memory_order_release);

  // A reader delayed between its phase load and speculative fetch_add may
  // still touch the reader count after drain_readers() returned. Preserve the
  // packed counters while reopening admission so that its rollback cannot be
  // lost. The release side of the CAS publishes the reset timeline first.
  gate_.transition_phase(AdmissionState::kAllBlocked, AdmissionState::kOpen);
}

uint32_t VersionManager::acquire_compact_timestamp() {
  gate_.enter_phase(AdmissionState::kAllBlocked);
  gate_.wait_readers_drained();
  gate_.wait_inserters_drained();

  const auto reservation = ReserveWriteTimestamp(write_ts_, read_ts_);
  if (reservation.state == TimestampReservationState::kReserved) {
    return reservation.timestamp;
  }
  gate_.transition_phase(AdmissionState::kAllBlocked, AdmissionState::kOpen);
  throw_timestamp_reservation_failure(
      reservation.state, read_ts_.load(std::memory_order_relaxed),
      write_ts_.load(std::memory_order_relaxed));
}

void VersionManager::release_compact_timestamp(uint32_t ts) {
  // Compact must have blocked new readers.
  if (OperationGateWord::phase(gate_.load_acquire()) !=
      AdmissionState::kAllBlocked) {
    THROW_INTERNAL_EXCEPTION(
        "release_compact_timestamp called while not in compact state");
  }

  complete_write_timestamp(ts);

  // Restore normal operation.
  gate_.transition_phase(AdmissionState::kAllBlocked, AdmissionState::kOpen);
}

void VersionManager::revert_compact_timestamp(uint32_t ts) {
  // Compact must have blocked new readers.
  if (OperationGateWord::phase(gate_.load_acquire()) !=
      AdmissionState::kAllBlocked) {
    THROW_INTERNAL_EXCEPTION(
        "revert_compact_timestamp called while not in compact state");
  }

  // Close the timestamp gap so later commits can advance read_ts_.
  complete_write_timestamp(ts);

  // Restore normal operation.
  gate_.transition_phase(AdmissionState::kAllBlocked, AdmissionState::kOpen);
}

}  // namespace neug
