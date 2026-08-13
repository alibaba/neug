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

#include "neug/transaction/operation_gate.h"

#include <glog/logging.h>
#include <chrono>
#include <optional>

#include "neug/utils/exception/exception.h"
#include "neug/utils/likely.h"

namespace neug {

namespace {

using AdmissionState = detail::AdmissionState;
using OperationGateWord = detail::OperationGateWord;

bool DeadlineExpired(std::chrono::steady_clock::time_point deadline) noexcept {
  return std::chrono::steady_clock::now() >= deadline;
}

}  // namespace

void OperationGate::acquire_shared() {
  std::optional<RuntimeBackoff> wait;
  uint64_t observed = state_.load(std::memory_order_relaxed);
  while (true) {
    if (NEUG_UNLIKELY(OperationGateWord::phase(observed) ==
                      AdmissionState::kAllBlocked)) {
      if (!wait) {
        wait.emplace(make_runtime_backoff());
      }
      (*wait)();
      observed = state_.load(std::memory_order_relaxed);
      continue;
    }

    // The returned word linearizes admission with phase changes. The relaxed
    // check above avoids unnecessary RMWs while readers are already blocked;
    // this old-word check closes the race with a concurrent phase transition.
    const uint64_t previous = state_.fetch_add(OperationGateWord::kReaderUnit,
                                               std::memory_order_acquire);
    const bool counter_exhausted = OperationGateWord::readers(previous) >=
                                   OperationGateWord::kMaxReaderCount;
    if (NEUG_LIKELY(OperationGateWord::phase(previous) !=
                        AdmissionState::kAllBlocked &&
                    !counter_exhausted)) {
      return;
    }

    // Roll back the speculative increment and retry with backoff.
    state_.fetch_sub(OperationGateWord::kReaderUnit, std::memory_order_release);
    if (NEUG_UNLIKELY(counter_exhausted)) {
      THROW_RUNTIME_ERROR("Reader admission counter exhausted");
    }
    if (!wait) {
      wait.emplace(make_runtime_backoff());
    }
    (*wait)();
    observed = state_.load(std::memory_order_relaxed);
  }
}

bool OperationGate::acquire_shared_until(
    std::chrono::steady_clock::time_point deadline) {
  std::optional<RuntimeBackoff> wait;
  uint64_t observed = state_.load(std::memory_order_relaxed);
  while (true) {
    if (DeadlineExpired(deadline)) {
      return false;
    }
    if (NEUG_UNLIKELY(OperationGateWord::phase(observed) ==
                      AdmissionState::kAllBlocked)) {
      if (!wait) {
        wait.emplace(make_runtime_backoff());
      }
      (*wait)();
      observed = state_.load(std::memory_order_relaxed);
      continue;
    }

    const uint64_t previous = state_.fetch_add(OperationGateWord::kReaderUnit,
                                               std::memory_order_acquire);
    const bool counter_exhausted = OperationGateWord::readers(previous) >=
                                   OperationGateWord::kMaxReaderCount;
    if (NEUG_LIKELY(OperationGateWord::phase(previous) !=
                        AdmissionState::kAllBlocked &&
                    !counter_exhausted)) {
      // The task may have been suspended after the pre-attempt deadline check.
      // Confirm the successful admission before returning it to the caller.
      if (NEUG_LIKELY(!DeadlineExpired(deadline))) {
        return true;
      }
      state_.fetch_sub(OperationGateWord::kReaderUnit,
                       std::memory_order_release);
      return false;
    }

    state_.fetch_sub(OperationGateWord::kReaderUnit, std::memory_order_release);
    if (NEUG_UNLIKELY(counter_exhausted)) {
      THROW_RUNTIME_ERROR("Reader admission counter exhausted");
    }
    if (!wait) {
      wait.emplace(make_runtime_backoff());
    }
    (*wait)();
    observed = state_.load(std::memory_order_relaxed);
  }
}

void OperationGate::release_shared() {
  const uint64_t observed = state_.load(std::memory_order_relaxed);
  if (NEUG_UNLIKELY(OperationGateWord::readers(observed) == 0)) {
    THROW_INTERNAL_EXCEPTION("release_shared without admission");
  }
  // A valid caller owns one count, so valid concurrent releases cannot borrow
  // from an adjacent field.
  const uint64_t previous = state_.fetch_sub(OperationGateWord::kReaderUnit,
                                             std::memory_order_release);
  DCHECK_GT(OperationGateWord::readers(previous), 0U);
}

void OperationGate::acquire_inserter() {
  std::optional<RuntimeBackoff> wait;
  uint64_t observed = state_.load(std::memory_order_relaxed);
  while (true) {
    if (NEUG_UNLIKELY(OperationGateWord::phase(observed) !=
                      AdmissionState::kOpen)) {
      if (!wait) {
        wait.emplace(make_runtime_backoff());
      }
      (*wait)();
      observed = state_.load(std::memory_order_relaxed);
      continue;
    }
    if (NEUG_UNLIKELY(OperationGateWord::inserters(observed) ==
                      OperationGateWord::kMaxInserterCount)) {
      THROW_RUNTIME_ERROR("Inserter admission counter exhausted");
    }
    const uint64_t desired = OperationGateWord::increment_inserter(observed);
    // Atomic modification order linearizes admission with phase changes.
    // Acquire is only needed after this RMW succeeds.
    if (state_.compare_exchange_weak(observed, desired,
                                     std::memory_order_acquire,
                                     std::memory_order_relaxed)) {
      return;
    }
  }
}

void OperationGate::release_inserter() {
  const uint64_t observed = state_.load(std::memory_order_relaxed);
  if (NEUG_UNLIKELY(OperationGateWord::inserters(observed) == 0)) {
    THROW_INTERNAL_EXCEPTION("release_inserter without admission");
  }
  // A valid caller owns one count, so valid concurrent releases cannot borrow
  // from an adjacent field.
  const uint64_t previous = state_.fetch_sub(OperationGateWord::kInserterUnit,
                                             std::memory_order_release);
  DCHECK_GT(OperationGateWord::inserters(previous), 0U);
}

void OperationGate::enter_phase(AdmissionState desired_phase) {
  std::optional<RuntimeBackoff> wait;
  uint64_t observed = state_.load(std::memory_order_relaxed);
  while (true) {
    if (OperationGateWord::phase(observed) != AdmissionState::kOpen) {
      if (!wait) {
        wait.emplace(make_runtime_backoff());
      }
      (*wait)();
      observed = state_.load(std::memory_order_relaxed);
      continue;
    }
    if (OperationGateWord::try_change_phase(state_, observed, desired_phase)) {
      return;
    }
  }
}

bool OperationGate::enter_phase_until(
    AdmissionState desired_phase,
    std::chrono::steady_clock::time_point deadline) {
  RuntimeBackoff admission_wait = make_runtime_backoff();
  uint64_t observed = state_.load(std::memory_order_relaxed);
  while (true) {
    if (DeadlineExpired(deadline)) {
      return false;
    }
    if (OperationGateWord::phase(observed) == AdmissionState::kOpen &&
        OperationGateWord::try_change_phase(state_, observed, desired_phase)) {
      // The task may have been suspended between the deadline check and the
      // phase CAS. Do not hand phase ownership to a caller after its deadline.
      if (NEUG_LIKELY(!DeadlineExpired(deadline))) {
        return true;
      }
      reopen();
      return false;
    }
    admission_wait();
    observed = state_.load(std::memory_order_relaxed);
  }
}

void OperationGate::transition_phase(AdmissionState expected_phase,
                                     AdmissionState desired_phase) {
  uint64_t observed = state_.load(std::memory_order_relaxed);
  while (true) {
    if (OperationGateWord::phase(observed) != expected_phase) {
      THROW_INTERNAL_EXCEPTION("Invalid transaction admission transition");
    }
    if (OperationGateWord::try_change_phase(state_, observed, desired_phase)) {
      return;
    }
  }
}

void OperationGate::reopen() noexcept {
  uint64_t observed = state_.load(std::memory_order_acquire);
  while (true) {
    const AdmissionState phase = OperationGateWord::phase(observed);
    DCHECK(phase == AdmissionState::kInsertsBlocked ||
           phase == AdmissionState::kAllBlocked)
        << "Update released outside update state";
    if (OperationGateWord::try_change_phase(state_, observed,
                                            AdmissionState::kOpen)) {
      return;
    }
  }
}

void OperationGate::wait_readers_drained() {
  uint64_t observed = state_.load(std::memory_order_acquire);
  if (OperationGateWord::readers(observed) == 0) {
    return;
  }

  RuntimeBackoff wait = make_runtime_backoff();
  do {
    wait();
    observed = state_.load(std::memory_order_acquire);
  } while (OperationGateWord::readers(observed) != 0);
}

void OperationGate::wait_inserters_drained() {
  uint64_t observed = state_.load(std::memory_order_acquire);
  if (OperationGateWord::inserters(observed) == 0) {
    return;
  }

  RuntimeBackoff wait = make_runtime_backoff();
  do {
    wait();
    observed = state_.load(std::memory_order_acquire);
  } while (OperationGateWord::inserters(observed) != 0);
}

bool OperationGate::wait_readers_drained_until(
    std::chrono::steady_clock::time_point deadline) {
  uint64_t observed = state_.load(std::memory_order_acquire);
  if (OperationGateWord::readers(observed) == 0) {
    return !DeadlineExpired(deadline);
  }

  RuntimeBackoff wait = make_runtime_backoff();
  do {
    if (DeadlineExpired(deadline)) {
      return false;
    }
    wait();
    observed = state_.load(std::memory_order_acquire);
  } while (OperationGateWord::readers(observed) != 0);
  return !DeadlineExpired(deadline);
}

bool OperationGate::wait_inserters_drained_until(
    std::chrono::steady_clock::time_point deadline) {
  uint64_t observed = state_.load(std::memory_order_acquire);
  if (OperationGateWord::inserters(observed) == 0) {
    return !DeadlineExpired(deadline);
  }

  RuntimeBackoff wait = make_runtime_backoff();
  do {
    if (DeadlineExpired(deadline)) {
      return false;
    }
    wait();
    observed = state_.load(std::memory_order_acquire);
  } while (OperationGateWord::inserters(observed) != 0);
  return !DeadlineExpired(deadline);
}

bool OperationGate::try_seal_open_state() noexcept {
  uint64_t expected = OperationGateWord::empty(AdmissionState::kOpen);
  const uint64_t blocked =
      OperationGateWord::empty(AdmissionState::kAllBlocked);
  return state_.compare_exchange_strong(
      expected, blocked, std::memory_order_acq_rel, std::memory_order_acquire);
}

void OperationGate::unseal_to_open() noexcept {
  uint64_t observed = state_.load(std::memory_order_acquire);
  while (true) {
    DCHECK(OperationGateWord::phase(observed) == AdmissionState::kAllBlocked);
    if (OperationGateWord::try_change_phase(state_, observed,
                                            AdmissionState::kOpen)) {
      return;
    }
  }
}

SharedOperationLease::SharedOperationLease(OperationGate& gate) : gate_(&gate) {
  gate_->acquire_shared();
}

SharedOperationLease::SharedOperationLease(
    OperationGate& gate, std::chrono::steady_clock::time_point deadline) {
  if (!gate.acquire_shared_until(deadline)) {
    THROW_TRANSACTION_TIMEOUT("waiting for shared operation admission");
  }
  gate_ = &gate;
}

SharedOperationLease::SharedOperationLease(
    SharedOperationLease&& other) noexcept
    : gate_(std::exchange(other.gate_, nullptr)) {}

SharedOperationLease& SharedOperationLease::operator=(
    SharedOperationLease&& other) noexcept {
  if (this != &other) {
    release();
    gate_ = std::exchange(other.gate_, nullptr);
  }
  return *this;
}

SharedOperationLease::~SharedOperationLease() noexcept { release(); }

void SharedOperationLease::release() noexcept {
  auto* gate = std::exchange(gate_, nullptr);
  if (gate) {
    gate->release_shared();
  }
}

ExclusiveOperationLease::ExclusiveOperationLease(OperationGate& gate)
    : gate_(nullptr) {
  gate.enter_phase(detail::AdmissionState::kAllBlocked);
  gate.wait_inserters_drained();
  gate.wait_readers_drained();
  gate_ = &gate;
}

ExclusiveOperationLease::ExclusiveOperationLease(
    OperationGate& gate, std::chrono::steady_clock::time_point deadline) {
  if (!gate.enter_phase_until(detail::AdmissionState::kAllBlocked, deadline)) {
    THROW_TRANSACTION_TIMEOUT("waiting for exclusive operation admission");
  }

  try {
    if (!gate.wait_inserters_drained_until(deadline)) {
      THROW_TRANSACTION_TIMEOUT("waiting for active inserts to finish");
    }
    if (!gate.wait_readers_drained_until(deadline)) {
      THROW_TRANSACTION_TIMEOUT("waiting for active readers to finish");
    }
  } catch (...) {
    gate.reopen();
    throw;
  }
  gate_ = &gate;
}

ExclusiveOperationLease::ExclusiveOperationLease(
    ExclusiveOperationLease&& other) noexcept
    : gate_(std::exchange(other.gate_, nullptr)) {}

ExclusiveOperationLease& ExclusiveOperationLease::operator=(
    ExclusiveOperationLease&& other) noexcept {
  if (this != &other) {
    release();
    gate_ = std::exchange(other.gate_, nullptr);
  }
  return *this;
}

ExclusiveOperationLease::~ExclusiveOperationLease() noexcept { release(); }

void ExclusiveOperationLease::release() noexcept {
  auto* gate = std::exchange(gate_, nullptr);
  if (gate) {
    gate->reopen();
  }
}

}  // namespace neug
