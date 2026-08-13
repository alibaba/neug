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

#include <stdint.h>
#include <atomic>
#include <chrono>
#include <utility>

#include "neug/transaction/runtime_wait.h"

namespace neug {

namespace detail {

enum class AdmissionState : uint8_t { kOpen, kInsertsBlocked, kAllBlocked };

// [63:62] phase | [61:31] inserters | [30:0] readers.
struct OperationGateWord {
  static constexpr uint32_t kMaxReaderCount = 0x7fffffffu;
  static constexpr uint32_t kMaxInserterCount = 0x7fffffffu;
  static constexpr uint64_t kReaderMask = kMaxReaderCount;
  static constexpr uint64_t kInserterMask = uint64_t{kMaxInserterCount} << 31;
  static constexpr uint64_t kPhaseMask = uint64_t{3} << 62;
  static constexpr uint64_t kReaderUnit = 1;
  static constexpr uint64_t kInserterUnit = uint64_t{1} << 31;

  [[nodiscard]] static constexpr uint64_t empty(AdmissionState phase) noexcept {
    return static_cast<uint64_t>(phase) << 62;
  }

  [[nodiscard]] static constexpr AdmissionState phase(uint64_t word) noexcept {
    return static_cast<AdmissionState>(word >> 62);
  }

  [[nodiscard]] static constexpr uint32_t readers(uint64_t word) noexcept {
    return static_cast<uint32_t>(word & kReaderMask);
  }

  [[nodiscard]] static constexpr uint32_t inserters(uint64_t word) noexcept {
    return static_cast<uint32_t>((word >> 31) & kMaxInserterCount);
  }

  [[nodiscard]] static constexpr uint64_t with_phase(
      uint64_t word, AdmissionState desired_phase) noexcept {
    return (word & ~kPhaseMask) | (static_cast<uint64_t>(desired_phase) << 62);
  }

  // Callers validate the inserter counter before changing it.
  [[nodiscard]] static constexpr uint64_t increment_inserter(
      uint64_t word) noexcept {
    return word + kInserterUnit;
  }

  [[nodiscard]] static bool try_change_phase(
      std::atomic<uint64_t>& gate, uint64_t& observed,
      AdmissionState desired_phase) noexcept {
    const uint64_t desired = with_phase(observed, desired_phase);
    return gate.compare_exchange_weak(observed, desired,
                                      std::memory_order_acq_rel,
                                      std::memory_order_relaxed);
  }
};

static_assert((OperationGateWord::kPhaseMask &
               OperationGateWord::kReaderMask) == 0);
static_assert((OperationGateWord::kPhaseMask &
               OperationGateWord::kInserterMask) == 0);
static_assert((OperationGateWord::kReaderMask &
               OperationGateWord::kInserterMask) == 0);
static_assert((OperationGateWord::kPhaseMask | OperationGateWord::kReaderMask |
               OperationGateWord::kInserterMask) == ~uint64_t{0});

}  // namespace detail

/**
 * @brief Packed admission gate shared by all operation classes.
 *
 * One atomic word combines the admission phase with the active reader and
 * inserter counters. The phase transitions:
 * - Update: open → inserts-blocked → all-blocked → open.
 * - Compact/exclusive operation: open → all-blocked → open.
 *
 * Shared admission uses fetch_add; inserter admission and phase changes use
 * CAS on the same word. An operation is therefore unambiguously counted
 * before a blocking transition or rejected after it. Reader and inserter
 * counts cannot exceed 2^31 - 1.
 *
 * Blocking methods may suspend the logical task through the configured
 * runtime backoff. Callers must not hold an OS-thread-owned lock or retain
 * an ordinary TLS pointer across the call.
 */
class OperationGate {
 public:
  explicit OperationGate(
      RuntimeWaitFn runtime_wait = &NativeRuntimeWait) noexcept
      : state_(detail::OperationGateWord::empty(detail::AdmissionState::kOpen)),
        runtime_wait_(runtime_wait) {}

  OperationGate(const OperationGate&) = delete;
  OperationGate& operator=(const OperationGate&) = delete;

  RuntimeBackoff make_runtime_backoff() const noexcept {
    return RuntimeBackoff(runtime_wait());
  }
  RuntimeWaitFn runtime_wait() const noexcept {
    return runtime_wait_.load(std::memory_order_acquire);
  }
  // Lifecycle-only swap. Callers must seal the gate via try_seal_open_state
  // before calling this so no in-flight wait uses the previous callback.
  void store_runtime_wait(RuntimeWaitFn runtime_wait) noexcept {
    runtime_wait_.store(runtime_wait, std::memory_order_release);
  }

  // Reset to empty open admission. Only valid while no operation is active
  // (database initialization).
  void reset_open() noexcept {
    state_.store(
        detail::OperationGateWord::empty(detail::AdmissionState::kOpen),
        std::memory_order_relaxed);
  }

  // Shared admission: one reader count. Blocks while the gate blocks
  // readers; throws RuntimeError when the reader counter is exhausted.
  void acquire_shared();
  // Timed variant. Returns false when the deadline expires before admission;
  // no reader count is retained on failure.
  bool acquire_shared_until(std::chrono::steady_clock::time_point deadline);
  void release_shared();

  // Inserter admission: one inserter count. Blocks while the phase is not
  // open; throws RuntimeError when the inserter counter is exhausted.
  void acquire_inserter();
  void release_inserter();

  // Wait until the phase is open, then enter desired_phase. Waiters contend
  // on the phase CAS; acquisition order is intentionally unspecified.
  void enter_phase(detail::AdmissionState desired_phase);
  // Timed variant. Returns false when the deadline expires before admission;
  // the gate is left unchanged on failure.
  bool enter_phase_until(detail::AdmissionState desired_phase,
                         std::chrono::steady_clock::time_point deadline);
  // CAS from expected_phase to desired_phase. Throws InternalException when
  // the observed phase differs from expected_phase.
  void transition_phase(detail::AdmissionState expected_phase,
                        detail::AdmissionState desired_phase);
  // Reopen admission after an update completes. The phase must currently be
  // inserts-blocked or all-blocked.
  void reopen() noexcept;

  // Drain waits. The caller is responsible for having blocked the matching
  // admission class beforehand.
  void wait_readers_drained();
  void wait_inserters_drained();
  bool wait_readers_drained_until(
      std::chrono::steady_clock::time_point deadline);
  bool wait_inserters_drained_until(
      std::chrono::steady_clock::time_point deadline);

  // Seal the empty open gate for a lifecycle-only runtime-wait swap.
  bool try_seal_open_state() noexcept;
  // Reopen after a successful seal while preserving a delayed speculative
  // admission count until that admission observes the blocked phase and
  // rolls itself back.
  void unseal_to_open() noexcept;

  uint64_t load_acquire() const noexcept {
    return state_.load(std::memory_order_acquire);
  }

 private:
  std::atomic<uint64_t> state_;
  std::atomic<RuntimeWaitFn> runtime_wait_;
};

/**
 * @brief Move-only RAII owner of one shared (reader) admission count.
 */
class SharedOperationLease {
 public:
  explicit SharedOperationLease(OperationGate& gate);
  SharedOperationLease(OperationGate& gate,
                       std::chrono::steady_clock::time_point deadline);
  SharedOperationLease(SharedOperationLease&& other) noexcept;
  SharedOperationLease& operator=(SharedOperationLease&& other) noexcept;

  SharedOperationLease(const SharedOperationLease&) = delete;
  SharedOperationLease& operator=(const SharedOperationLease&) = delete;

  ~SharedOperationLease() noexcept;

  void release() noexcept;
  bool active() const noexcept { return gate_ != nullptr; }

 private:
  OperationGate* gate_{nullptr};
};

/**
 * @brief Move-only RAII owner of exclusive gate admission.
 *
 * Acquisition enters the all-blocked phase and drains both readers and
 * inserters; release reopens admission. No timestamp is reserved, so this
 * lease only owns operation admission; callers compose storage and durability
 * semantics separately.
 */
class ExclusiveOperationLease {
 public:
  explicit ExclusiveOperationLease(OperationGate& gate);
  ExclusiveOperationLease(OperationGate& gate,
                          std::chrono::steady_clock::time_point deadline);
  ExclusiveOperationLease(ExclusiveOperationLease&& other) noexcept;
  ExclusiveOperationLease& operator=(ExclusiveOperationLease&& other) noexcept;

  ExclusiveOperationLease(const ExclusiveOperationLease&) = delete;
  ExclusiveOperationLease& operator=(const ExclusiveOperationLease&) = delete;

  ~ExclusiveOperationLease() noexcept;

  void release() noexcept;
  bool active() const noexcept { return gate_ != nullptr; }

 private:
  OperationGate* gate_{nullptr};
};

}  // namespace neug
