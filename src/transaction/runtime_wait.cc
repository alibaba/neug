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

#include "neug/transaction/runtime_wait.h"

#include <atomic>
#include <chrono>
#include <thread>

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || \
    defined(_M_IX86)
#include <immintrin.h>
#endif
#if defined(_MSC_VER) && defined(_M_ARM64)
#include <intrin.h>
#endif

namespace neug {

void NativeRuntimeWait(RuntimeWaitAction action) noexcept {
  switch (action) {
  case RuntimeWaitAction::kYield:
    std::this_thread::yield();
    break;
  case RuntimeWaitAction::kSleep:
    std::this_thread::sleep_for(
        std::chrono::microseconds(kRuntimeWaitSleepMicros));
    break;
  }
}

void RuntimeCpuRelax() noexcept {
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || \
    defined(_M_IX86)
  _mm_pause();
#elif defined(__aarch64__) || defined(__ARM_ARCH_ISA_A64) || defined(_M_ARM64)
#if defined(_MSC_VER)
  __yield();
#else
  __asm__ __volatile__("yield");
#endif
#else
  std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

}  // namespace neug
