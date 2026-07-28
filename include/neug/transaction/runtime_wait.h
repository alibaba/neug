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

#include <cstdint>

namespace neug {

/**
 * Runtime-specific scheduler operation used by AdaptiveBackoff.
 *
 * CPU relaxation is runtime-independent and handled directly by
 * AdaptiveBackoff. The callback is invoked only for yield and sleep actions.
 */
enum class RuntimeWaitAction { kYield, kSleep };

using RuntimeWaitFn = void (*)(RuntimeWaitAction action) noexcept;

inline constexpr uint32_t kRuntimeWaitSpinIterations = 64;
inline constexpr uint32_t kRuntimeWaitYieldIterations = 128;
inline constexpr uint64_t kRuntimeWaitSleepMicros = 50;

enum class RuntimeWaitPhase { kSpin, kYield, kSleep };

/**
 * Classify an iteration into the shared native/bthread backoff schedule.
 */
[[nodiscard]] constexpr RuntimeWaitPhase RuntimeWaitPhaseForIteration(
    uint32_t iteration) noexcept {
  if (iteration < kRuntimeWaitSpinIterations) {
    return RuntimeWaitPhase::kSpin;
  }
  if (iteration < kRuntimeWaitYieldIterations) {
    return RuntimeWaitPhase::kYield;
  }
  return RuntimeWaitPhase::kSleep;
}

/**
 * Issue an architecture-specific CPU relaxation hint.
 */
void RuntimeCpuRelax() noexcept;

/**
 * Per-wait adaptive backoff cursor. Each blocking condition gets its own
 * instance so a newly entered wait always starts with CPU relaxation.
 *
 * Callers must not enter a wait while holding a pthread-owned lock or
 * retaining a pointer into ordinary thread-local storage: a cooperative
 * runtime may resume the same logical task on another pthread worker.
 */
class AdaptiveBackoff final {
 public:
  explicit AdaptiveBackoff(RuntimeWaitFn runtime_wait) noexcept
      : runtime_wait_(runtime_wait) {}

  void operator()() noexcept {
    switch (RuntimeWaitPhaseForIteration(iteration_)) {
    case RuntimeWaitPhase::kSpin:
      RuntimeCpuRelax();
      break;
    case RuntimeWaitPhase::kYield:
      runtime_wait_(RuntimeWaitAction::kYield);
      break;
    case RuntimeWaitPhase::kSleep:
      runtime_wait_(RuntimeWaitAction::kSleep);
      break;
    }

    // No phase exists beyond sleep, so the cursor need not grow further.
    if (iteration_ < kRuntimeWaitYieldIterations) {
      ++iteration_;
    }
  }

 private:
  RuntimeWaitFn runtime_wait_;
  uint32_t iteration_{0};
};

/**
 * Native-thread implementation of yield and sleep.
 */
void NativeRuntimeWait(RuntimeWaitAction action) noexcept;

}  // namespace neug
