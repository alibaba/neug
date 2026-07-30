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

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>
#include <thread>
#include <utility>
#include <vector>

#ifdef BUILD_HTTP_SERVER
#include "bthread/bthread.h"
#include "neug/server/bthread_runtime_wait.h"
#endif
#include "neug/transaction/version_manager.h"

namespace neug {

struct VersionManagerTestPeer {
  static void lock_advancement(VersionManager& manager) {
    manager.lock_.lock();
  }

  static void unlock_advancement(VersionManager& manager) {
    manager.lock_.unlock();
  }
};

namespace {

constexpr auto kWaitTimeout = std::chrono::seconds(10);
std::atomic<uint64_t> g_runtime_wait_calls{0};
std::atomic<uint64_t> g_yield_calls{0};
std::atomic<uint64_t> g_sleep_calls{0};

void RecordRuntimeWait(RuntimeWaitAction action) noexcept {
  g_runtime_wait_calls.fetch_add(1, std::memory_order_relaxed);
  if (action == RuntimeWaitAction::kYield) {
    g_yield_calls.fetch_add(1, std::memory_order_relaxed);
  } else {
    g_sleep_calls.fetch_add(1, std::memory_order_relaxed);
  }
}

void CountingRuntimeWait(RuntimeWaitAction action) noexcept {
  RecordRuntimeWait(action);
  std::this_thread::yield();
}

void CountingNativeRuntimeWait(RuntimeWaitAction action) noexcept {
  RecordRuntimeWait(action);
  NativeRuntimeWait(action);
}

void InitManager(VersionManager& manager) {
  manager.init_ts({1, 0}, 4);
  EXPECT_TRUE(manager.try_set_runtime_wait_if_quiescent(&CountingRuntimeWait));
}

void FinishUpdate(VersionManager& manager, uint32_t timestamp) {
  manager.finish_update_timestamp(timestamp, std::nullopt);
}

template <typename Predicate>
bool WaitUntil(Predicate predicate) {
  const auto deadline = std::chrono::steady_clock::now() + kWaitTimeout;
  while (!predicate() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return predicate();
}

bool WaitForRuntimeWait() {
  return WaitUntil([]() {
    return g_runtime_wait_calls.load(std::memory_order_relaxed) != 0;
  });
}

bool WaitForSleep() {
  return WaitUntil(
      []() { return g_sleep_calls.load(std::memory_order_relaxed) != 0; });
}

void ResetRuntimeWaitCalls() {
  g_runtime_wait_calls.store(0, std::memory_order_relaxed);
  g_yield_calls.store(0, std::memory_order_relaxed);
  g_sleep_calls.store(0, std::memory_order_relaxed);
}

template <typename WaitOperation, typename UnblockOperation>
void ExpectRuntimeWaitWhile(WaitOperation wait, UnblockOperation unblock) {
  ResetRuntimeWaitCalls();
  std::thread waiter(std::move(wait));
  EXPECT_TRUE(WaitForRuntimeWait());
  unblock();
  waiter.join();
}

TEST(VersionManagerWaitTest, UncontendedPathsDoNotInvokeBackoff) {
  VersionManager manager;
  InitManager(manager);
  ResetRuntimeWaitCalls();

  const auto read_ts = manager.acquire_read_view().visibility_ts;
  EXPECT_EQ(read_ts, 1U);
  manager.release_read_view();

  const auto insert_ts = manager.acquire_insert_timestamp();
  manager.release_insert_timestamp(insert_ts);

  const auto update_ts = manager.acquire_update_timestamp();
  manager.begin_update_commit(update_ts);
  manager.drain_readers();
  FinishUpdate(manager, update_ts);

  const auto compact_ts = manager.acquire_compact_timestamp();
  manager.release_compact_timestamp(compact_ts);

  EXPECT_EQ(g_runtime_wait_calls.load(std::memory_order_relaxed), 0U);
}

TEST(VersionManagerWaitTest, AllContendedPathsUseBackoff) {
  VersionManager manager;
  InitManager(manager);
  {
    SCOPED_TRACE("read slow path");
    const auto update_ts = manager.acquire_update_timestamp();
    manager.begin_update_commit(update_ts);
    ExpectRuntimeWaitWhile(
        [&]() {
          manager.acquire_read_view();
          manager.release_read_view();
        },
        [&]() { FinishUpdate(manager, update_ts); });
  }
  {
    SCOPED_TRACE("insert slow path");
    const auto update_ts = manager.acquire_update_timestamp();
    uint32_t insert_ts = 0;
    ExpectRuntimeWaitWhile(
        [&]() { insert_ts = manager.acquire_insert_timestamp(); },
        [&]() { FinishUpdate(manager, update_ts); });
    manager.release_insert_timestamp(insert_ts);
  }
  {
    SCOPED_TRACE("update inserter drain and CAS");
    const auto insert_ts = manager.acquire_insert_timestamp();
    uint32_t update_ts = 0;
    ExpectRuntimeWaitWhile(
        [&]() { update_ts = manager.acquire_update_timestamp(); },
        [&]() { manager.release_insert_timestamp(insert_ts); });
    uint32_t next_update_ts = 0;
    ExpectRuntimeWaitWhile(
        [&]() { next_update_ts = manager.acquire_update_timestamp(); },
        [&]() { FinishUpdate(manager, update_ts); });
    FinishUpdate(manager, next_update_ts);
  }
  {
    SCOPED_TRACE("explicit reader drain");
    manager.acquire_read_view();
    const auto update_ts = manager.acquire_update_timestamp();
    manager.begin_update_commit(update_ts);
    ExpectRuntimeWaitWhile([&]() { manager.drain_readers(); },
                           [&]() { manager.release_read_view(); });
    FinishUpdate(manager, update_ts);
    EXPECT_ANY_THROW(manager.drain_readers());
  }
  {
    SCOPED_TRACE("compact CAS");
    const auto update_ts = manager.acquire_update_timestamp();
    uint32_t compact_ts = 0;
    ExpectRuntimeWaitWhile(
        [&]() { compact_ts = manager.acquire_compact_timestamp(); },
        [&]() { FinishUpdate(manager, update_ts); });
    manager.release_compact_timestamp(compact_ts);
  }
  {
    SCOPED_TRACE("compact inserter drain");
    const auto insert_ts = manager.acquire_insert_timestamp();
    uint32_t compact_ts = 0;
    ExpectRuntimeWaitWhile(
        [&]() { compact_ts = manager.acquire_compact_timestamp(); },
        [&]() { manager.release_insert_timestamp(insert_ts); });
    manager.release_compact_timestamp(compact_ts);
  }
  {
    SCOPED_TRACE("compact reader drain");
    manager.acquire_read_view();
    uint32_t compact_ts = 0;
    ExpectRuntimeWaitWhile(
        [&]() { compact_ts = manager.acquire_compact_timestamp(); },
        [&]() { manager.release_read_view(); });
    manager.release_compact_timestamp(compact_ts);
  }
}

TEST(VersionManagerWaitTest, TimestampAdvancementLockUsesBackoffOnlyUnlocked) {
  VersionManager manager;
  InitManager(manager);
  const auto insert_ts = manager.acquire_insert_timestamp();
  VersionManagerTestPeer::lock_advancement(manager);
  ExpectRuntimeWaitWhile(
      [&]() { manager.release_insert_timestamp(insert_ts); },
      [&]() { VersionManagerTestPeer::unlock_advancement(manager); });
}

TEST(VersionManagerAdmissionTest,
     CompactDoesNotOverlapAdmittedReadersOrInserters) {
  VersionManager manager;
  InitManager(manager);

  std::atomic<bool> stop{false};
  std::atomic<bool> compact_active{false};
  std::atomic<int> observed_readers{0};
  std::atomic<int> observed_inserters{0};
  std::atomic<int> violations{0};

  auto reader = [&]() {
    while (!stop.load(std::memory_order_acquire)) {
      manager.acquire_read_view();
      observed_readers.fetch_add(1, std::memory_order_seq_cst);
      if (compact_active.load(std::memory_order_seq_cst)) {
        violations.fetch_add(1, std::memory_order_relaxed);
      }
      std::this_thread::yield();
      if (compact_active.load(std::memory_order_seq_cst)) {
        violations.fetch_add(1, std::memory_order_relaxed);
      }
      observed_readers.fetch_sub(1, std::memory_order_seq_cst);
      manager.release_read_view();
    }
  };
  auto inserter = [&]() {
    while (!stop.load(std::memory_order_acquire)) {
      const auto ts = manager.acquire_insert_timestamp();
      observed_inserters.fetch_add(1, std::memory_order_seq_cst);
      if (compact_active.load(std::memory_order_seq_cst)) {
        violations.fetch_add(1, std::memory_order_relaxed);
      }
      std::this_thread::yield();
      if (compact_active.load(std::memory_order_seq_cst)) {
        violations.fetch_add(1, std::memory_order_relaxed);
      }
      observed_inserters.fetch_sub(1, std::memory_order_seq_cst);
      manager.release_insert_timestamp(ts);
    }
  };

  std::thread reader_thread(reader);
  std::thread inserter_thread(inserter);
  for (int i = 0; i < 1000; ++i) {
    const auto ts = manager.acquire_compact_timestamp();
    compact_active.store(true, std::memory_order_seq_cst);
    if (observed_readers.load(std::memory_order_seq_cst) != 0 ||
        observed_inserters.load(std::memory_order_seq_cst) != 0) {
      violations.fetch_add(1, std::memory_order_relaxed);
    }
    std::this_thread::yield();
    compact_active.store(false, std::memory_order_seq_cst);
    manager.release_compact_timestamp(ts);
  }
  stop.store(true, std::memory_order_release);
  reader_thread.join();
  inserter_thread.join();

  EXPECT_EQ(violations.load(std::memory_order_relaxed), 0);
}

TEST(RuntimeBackoffTest, KeepsSpinLocalAndDispatchesRuntimeWaits) {
  ResetRuntimeWaitCalls();
  RuntimeBackoff wait(&CountingRuntimeWait);

  for (uint32_t i = 0; i < kRuntimeWaitSpinIterations; ++i) {
    wait();
  }
  EXPECT_EQ(g_runtime_wait_calls.load(std::memory_order_relaxed), 0U);

  for (uint32_t i = kRuntimeWaitSpinIterations; i < kRuntimeWaitYieldIterations;
       ++i) {
    wait();
  }
  EXPECT_EQ(g_yield_calls.load(std::memory_order_relaxed),
            kRuntimeWaitYieldIterations - kRuntimeWaitSpinIterations);
  EXPECT_EQ(g_sleep_calls.load(std::memory_order_relaxed), 0U);

  wait();
  wait();

  EXPECT_EQ(g_sleep_calls.load(std::memory_order_relaxed), 2U);
}

TEST(RuntimeWaitPhaseTest, UsesSpecifiedBoundaries) {
  EXPECT_EQ(RuntimeWaitPhaseForIteration(0), RuntimeWaitPhase::kSpin);
  EXPECT_EQ(RuntimeWaitPhaseForIteration(kRuntimeWaitSpinIterations - 1),
            RuntimeWaitPhase::kSpin);
  EXPECT_EQ(RuntimeWaitPhaseForIteration(kRuntimeWaitSpinIterations),
            RuntimeWaitPhase::kYield);
  EXPECT_EQ(RuntimeWaitPhaseForIteration(kRuntimeWaitYieldIterations - 1),
            RuntimeWaitPhase::kYield);
  EXPECT_EQ(RuntimeWaitPhaseForIteration(kRuntimeWaitYieldIterations),
            RuntimeWaitPhase::kSleep);
  EXPECT_EQ(RuntimeWaitPhaseForIteration(std::numeric_limits<uint32_t>::max()),
            RuntimeWaitPhase::kSleep);
}

TEST(VersionManagerWaitTest, RuntimeWaitSwitchRequiresQuiescence) {
  VersionManager manager;
  manager.init_ts({1, 0}, 4);

  EXPECT_FALSE(manager.try_set_runtime_wait_if_quiescent(nullptr));
  EXPECT_TRUE(manager.try_set_runtime_wait_if_quiescent(&CountingRuntimeWait));

  manager.acquire_read_view();
  EXPECT_FALSE(manager.try_set_runtime_wait_if_quiescent(&NativeRuntimeWait));
  manager.release_read_view();

  const auto insert_ts = manager.acquire_insert_timestamp();
  EXPECT_FALSE(manager.try_set_runtime_wait_if_quiescent(&NativeRuntimeWait));
  manager.release_insert_timestamp(insert_ts);

  const auto update_ts = manager.acquire_update_timestamp();
  EXPECT_FALSE(manager.try_set_runtime_wait_if_quiescent(&NativeRuntimeWait));
  FinishUpdate(manager, update_ts);

  const auto compact_ts = manager.acquire_compact_timestamp();
  EXPECT_FALSE(manager.try_set_runtime_wait_if_quiescent(&NativeRuntimeWait));
  manager.release_compact_timestamp(compact_ts);

  VersionManagerTestPeer::lock_advancement(manager);
  EXPECT_FALSE(manager.try_set_runtime_wait_if_quiescent(&NativeRuntimeWait));
  VersionManagerTestPeer::unlock_advancement(manager);

  EXPECT_TRUE(manager.try_set_runtime_wait_if_quiescent(&NativeRuntimeWait));
}

TEST(NativeRuntimeWaitTest, SleepPhaseCompletesContendedWait) {
  ResetRuntimeWaitCalls();

  VersionManager manager;
  manager.init_ts({1, 0}, 4);
  ASSERT_TRUE(
      manager.try_set_runtime_wait_if_quiescent(&CountingNativeRuntimeWait));
  const auto update_ts = manager.acquire_update_timestamp();
  manager.begin_update_commit(update_ts);

  std::atomic<bool> completed{false};
  std::thread waiter([&]() {
    manager.acquire_read_view();
    manager.release_read_view();
    completed.store(true, std::memory_order_release);
  });

  const bool sleep_phase_observed = WaitForSleep();
  FinishUpdate(manager, update_ts);
  waiter.join();

  EXPECT_TRUE(sleep_phase_observed);
  EXPECT_TRUE(completed.load(std::memory_order_acquire));
}

#ifdef BUILD_HTTP_SERVER

void CountingBthreadRuntimeWait(RuntimeWaitAction action) noexcept {
  RecordRuntimeWait(action);
  BthreadRuntimeWait(action);
}

struct ReaderState {
  VersionManager* manager;
  std::atomic<int>* started;
};

void* WaitForReadTimestamp(void* arg) {
  auto& state = *static_cast<ReaderState*>(arg);
  state.started->fetch_add(1, std::memory_order_relaxed);
  state.manager->acquire_read_view();
  state.manager->release_read_view();
  return nullptr;
}

void* MarkScheduled(void* arg) {
  static_cast<std::atomic<bool>*>(arg)->store(true, std::memory_order_release);
  return nullptr;
}

TEST(BthreadRuntimeWaitTest, SleepPhaseLeavesWorkerAvailableForNewBthread) {
  ResetRuntimeWaitCalls();

  VersionManager manager;
  manager.init_ts({1, 0}, 4);
  ASSERT_TRUE(
      manager.try_set_runtime_wait_if_quiescent(&CountingBthreadRuntimeWait));
  const auto update_ts = manager.acquire_update_timestamp();
  manager.begin_update_commit(update_ts);

  const int worker_count = std::max(2, bthread_getconcurrency());
  const int waiter_count = worker_count * 2;
  std::atomic<int> started{0};
  ReaderState reader_state{&manager, &started};
  std::vector<bthread_t> waiters(waiter_count);
  int started_waiters = 0;
  for (; started_waiters < waiter_count; ++started_waiters) {
    if (bthread_start_background(&waiters[started_waiters],
                                 &BTHREAD_ATTR_NORMAL, WaitForReadTimestamp,
                                 &reader_state) != 0) {
      break;
    }
  }
  if (started_waiters != waiter_count) {
    FinishUpdate(manager, update_ts);
    for (int i = 0; i < started_waiters; ++i) {
      EXPECT_EQ(bthread_join(waiters[i], nullptr), 0);
    }
    FAIL() << "Failed to start all reader bthreads";
  }

  const bool all_readers_started = WaitUntil([&]() {
    return started.load(std::memory_order_relaxed) == waiter_count;
  });
  const bool sleep_phase_observed = WaitForSleep();

  // Start this only after every waiter has reached the sleep phase. It does
  // not release the VM condition itself: the native test thread always does
  // that, so a scheduling regression cannot leave bthread_join blocked.
  std::atomic<bool> probe_scheduled{false};
  bthread_t probe;
  const int probe_start_result = bthread_start_background(
      &probe, &BTHREAD_ATTR_NORMAL, MarkScheduled, &probe_scheduled);
  const bool probe_ran =
      probe_start_result == 0 && WaitUntil([&]() {
        return probe_scheduled.load(std::memory_order_acquire);
      });

  FinishUpdate(manager, update_ts);
  if (probe_start_result == 0) {
    EXPECT_EQ(bthread_join(probe, nullptr), 0);
  }
  for (int i = 0; i < started_waiters; ++i) {
    EXPECT_EQ(bthread_join(waiters[i], nullptr), 0);
  }

  EXPECT_EQ(probe_start_result, 0);
  EXPECT_TRUE(probe_ran);
  EXPECT_TRUE(all_readers_started);
  EXPECT_TRUE(sleep_phase_observed);
}

TEST(BthreadRuntimeWaitTest, FallsBackOnNativePthread) {
  BthreadRuntimeWait(RuntimeWaitAction::kYield);
  BthreadRuntimeWait(RuntimeWaitAction::kSleep);
}

#endif

}  // namespace
}  // namespace neug
