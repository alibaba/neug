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
#include <condition_variable>
#include <cstdint>
#include <future>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#ifdef BUILD_HTTP_SERVER
#include "bthread/bthread.h"
#include "neug/server/bthread_runtime_wait.h"
#endif
#include "neug/transaction/timestamp_lease.h"
#include "neug/transaction/version_manager.h"
#include "neug/utils/exception/exception.h"

namespace neug {

namespace {

constexpr auto kWaitTimeout = std::chrono::seconds(10);
std::atomic<uint64_t> g_runtime_wait_calls{0};
std::atomic<uint64_t> g_yield_calls{0};
std::atomic<uint64_t> g_sleep_calls{0};
std::atomic<uint32_t> g_blocked_waiters{0};
std::mutex g_blocking_wait_lock;
std::condition_variable g_blocking_wait_cv;
bool g_block_waiters = false;

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

void BlockingRuntimeWait(RuntimeWaitAction) noexcept {
  g_blocked_waiters.fetch_add(1, std::memory_order_release);
  std::unique_lock lock(g_blocking_wait_lock);
  g_blocking_wait_cv.wait(lock, [] { return !g_block_waiters; });
}

void BlockRuntimeWaiters() {
  g_blocked_waiters.store(0, std::memory_order_relaxed);
  std::lock_guard lock(g_blocking_wait_lock);
  g_block_waiters = true;
}

void ReleaseRuntimeWaiters() {
  {
    std::lock_guard lock(g_blocking_wait_lock);
    g_block_waiters = false;
  }
  g_blocking_wait_cv.notify_all();
}

void InitManager(VersionManager& manager) {
  manager.init_ts({1, 0}, 4);
  EXPECT_TRUE(manager.try_set_runtime_wait_if_quiescent(&CountingRuntimeWait));
}

void FinishUpdate(VersionManager& manager, uint32_t timestamp) {
  manager.finish_update_timestamp(timestamp, std::nullopt);
}

void ChangeGatePhase(std::atomic<uint64_t>& gate,
                     detail::AdmissionState expected_phase,
                     detail::AdmissionState desired_phase) {
  uint64_t observed = gate.load(std::memory_order_acquire);
  while (!detail::OperationGateWord::try_change_phase(gate, observed,
                                                      desired_phase)) {
    ASSERT_EQ(detail::OperationGateWord::phase(observed), expected_phase);
  }
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

bool WaitForBlockedWaiters(uint32_t expected) {
  return WaitUntil([&] {
    return g_blocked_waiters.load(std::memory_order_acquire) >= expected;
  });
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

TEST(OperationGateWordTest, AdmissionsBeforeCompactTransitionRemainCounted) {
  std::atomic<uint64_t> gate{
      detail::OperationGateWord::empty(detail::AdmissionState::kOpen)};
  const uint64_t reader_observation = gate.fetch_add(
      detail::OperationGateWord::kReaderUnit, std::memory_order_acquire);
  ASSERT_EQ(detail::OperationGateWord::phase(reader_observation),
            detail::AdmissionState::kOpen);
  uint64_t inserter_observation = gate.load(std::memory_order_acquire);
  ASSERT_TRUE(gate.compare_exchange_strong(
      inserter_observation,
      detail::OperationGateWord::increment_inserter(inserter_observation),
      std::memory_order_acq_rel, std::memory_order_acquire));

  ChangeGatePhase(gate, detail::AdmissionState::kOpen,
                  detail::AdmissionState::kAllBlocked);

  const uint64_t blocked = gate.load(std::memory_order_acquire);
  EXPECT_EQ(detail::OperationGateWord::readers(blocked), 1U);
  EXPECT_EQ(detail::OperationGateWord::inserters(blocked), 1U);
}

TEST(OperationGateWordTest, DelayedReaderRetriesAfterUpdateCommitTransition) {
  std::atomic<uint64_t> gate{detail::OperationGateWord::empty(
      detail::AdmissionState::kInsertsBlocked)};

  ChangeGatePhase(gate, detail::AdmissionState::kInsertsBlocked,
                  detail::AdmissionState::kAllBlocked);

  const uint64_t reader_observation = gate.fetch_add(
      detail::OperationGateWord::kReaderUnit, std::memory_order_acquire);
  EXPECT_EQ(detail::OperationGateWord::phase(reader_observation),
            detail::AdmissionState::kAllBlocked);
  gate.fetch_sub(detail::OperationGateWord::kReaderUnit,
                 std::memory_order_release);
  EXPECT_EQ(
      detail::OperationGateWord::readers(gate.load(std::memory_order_acquire)),
      0U);
}

TEST(OperationGateWordTest,
     DelayedInserterRetriesAfterUpdateExecutionTransition) {
  std::atomic<uint64_t> gate{
      detail::OperationGateWord::empty(detail::AdmissionState::kOpen)};
  uint64_t delayed_observation = gate.load(std::memory_order_acquire);

  ChangeGatePhase(gate, detail::AdmissionState::kOpen,
                  detail::AdmissionState::kInsertsBlocked);

  EXPECT_FALSE(gate.compare_exchange_strong(
      delayed_observation,
      detail::OperationGateWord::increment_inserter(delayed_observation),
      std::memory_order_acq_rel, std::memory_order_acquire));
  EXPECT_EQ(detail::OperationGateWord::phase(delayed_observation),
            detail::AdmissionState::kInsertsBlocked);
  EXPECT_EQ(detail::OperationGateWord::inserters(
                gate.load(std::memory_order_acquire)),
            0U);
}

TEST(OperationGateWordTest,
     DelayedReaderAndInserterRetryAfterCompactTransition) {
  std::atomic<uint64_t> gate{
      detail::OperationGateWord::empty(detail::AdmissionState::kOpen)};
  uint64_t delayed_inserter_observation = gate.load(std::memory_order_acquire);

  ChangeGatePhase(gate, detail::AdmissionState::kOpen,
                  detail::AdmissionState::kAllBlocked);

  const uint64_t reader_observation = gate.fetch_add(
      detail::OperationGateWord::kReaderUnit, std::memory_order_acquire);
  EXPECT_EQ(detail::OperationGateWord::phase(reader_observation),
            detail::AdmissionState::kAllBlocked);
  gate.fetch_sub(detail::OperationGateWord::kReaderUnit,
                 std::memory_order_release);
  EXPECT_FALSE(gate.compare_exchange_strong(
      delayed_inserter_observation,
      detail::OperationGateWord::increment_inserter(
          delayed_inserter_observation),
      std::memory_order_acq_rel, std::memory_order_acquire));
  EXPECT_EQ(detail::OperationGateWord::phase(delayed_inserter_observation),
            detail::AdmissionState::kAllBlocked);

  const uint64_t blocked = gate.load(std::memory_order_acquire);
  EXPECT_EQ(detail::OperationGateWord::readers(blocked), 0U);
  EXPECT_EQ(detail::OperationGateWord::inserters(blocked), 0U);
}

TEST(OperationGateWordTest, DetectsReaderAndInserterCounterExhaustion) {
  constexpr uint32_t kInserters = 7;
  const uint64_t reader_word =
      detail::OperationGateWord::empty(detail::AdmissionState::kOpen) |
      detail::OperationGateWord::kReaderMask | (uint64_t{kInserters} << 31);
  std::atomic<uint64_t> gate{reader_word};
  // Admission rejects once the observed old word is already at the maximum,
  // so the counter never actually wraps into the adjacent inserter field.
  const uint64_t reader_observation = gate.fetch_add(
      detail::OperationGateWord::kReaderUnit, std::memory_order_acquire);
  EXPECT_EQ(detail::OperationGateWord::readers(reader_observation),
            detail::OperationGateWord::kMaxReaderCount);
  EXPECT_GE(detail::OperationGateWord::readers(reader_observation),
            detail::OperationGateWord::kMaxReaderCount);
  EXPECT_EQ(detail::OperationGateWord::phase(reader_observation),
            detail::AdmissionState::kOpen);
  EXPECT_EQ(detail::OperationGateWord::inserters(reader_observation),
            kInserters);

  gate.fetch_sub(detail::OperationGateWord::kReaderUnit,
                 std::memory_order_release);
  EXPECT_EQ(gate.load(std::memory_order_acquire), reader_word);

  const uint64_t inserter_word =
      detail::OperationGateWord::empty(detail::AdmissionState::kOpen) |
      detail::OperationGateWord::kInserterMask;
  EXPECT_EQ(detail::OperationGateWord::inserters(inserter_word),
            detail::OperationGateWord::kMaxInserterCount);
  EXPECT_EQ(detail::OperationGateWord::phase(inserter_word),
            detail::AdmissionState::kOpen);
  EXPECT_EQ(detail::OperationGateWord::readers(inserter_word), 0U);
}

TEST(UpdateTimestampLeaseTest, MoveTransfersAdmissionOwnership) {
  VersionManager manager;
  InitManager(manager);

  {
    UpdateTimestampLease original(manager);
    UpdateTimestampLease owner(std::move(original));
    EXPECT_FALSE(manager.try_set_runtime_wait_if_quiescent(&NativeRuntimeWait));
  }

  EXPECT_TRUE(manager.try_set_runtime_wait_if_quiescent(&NativeRuntimeWait));
}

TEST(UpdateTimestampLeaseTest, ScopeExitAfterExceptionReleasesAdmission) {
  VersionManager manager;
  InitManager(manager);

  EXPECT_THROW(
      {
        UpdateTimestampLease lease(manager);
        throw std::runtime_error("construction failed");
      },
      std::runtime_error);

  EXPECT_TRUE(manager.try_set_runtime_wait_if_quiescent(&NativeRuntimeWait));
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

TEST(VersionManagerUpdateAdmissionTest,
     ContendedTimeoutDoesNotBlockOtherWaiters) {
  VersionManager manager;
  InitManager(manager);
  ASSERT_TRUE(manager.try_set_runtime_wait_if_quiescent(&BlockingRuntimeWait));
  const auto holder = manager.acquire_update_timestamp();

  BlockRuntimeWaiters();
  std::promise<bool> waiter_timed_out;
  std::promise<uint32_t> successor_timestamp;
  auto timeout_result = waiter_timed_out.get_future();
  auto successor_result = successor_timestamp.get_future();
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(50);
  std::thread timed_waiter([&] {
    try {
      UpdateTimestampLease lease(manager, deadline);
      waiter_timed_out.set_value(false);
    } catch (const exception::TransactionTimeoutException&) {
      waiter_timed_out.set_value(true);
    }
  });
  ASSERT_TRUE(WaitForBlockedWaiters(1));
  std::thread successor([&] {
    const auto ts = manager.acquire_update_timestamp();
    successor_timestamp.set_value(ts);
    FinishUpdate(manager, ts);
  });
  ASSERT_TRUE(WaitForBlockedWaiters(2));

  std::this_thread::sleep_until(deadline);
  ReleaseRuntimeWaiters();
  EXPECT_EQ(timeout_result.wait_for(kWaitTimeout), std::future_status::ready);
  EXPECT_TRUE(timeout_result.get());
  FinishUpdate(manager, holder);
  EXPECT_EQ(successor_result.wait_for(kWaitTimeout), std::future_status::ready);
  EXPECT_EQ(successor_result.get(), 3U);
  timed_waiter.join();
  successor.join();
}

TEST(VersionManagerUpdateAdmissionTest,
     InserterDrainTimeoutRestoresAdmissionWithoutTimestamp) {
  VersionManager manager;
  InitManager(manager);
  ASSERT_TRUE(manager.try_set_runtime_wait_if_quiescent(&BlockingRuntimeWait));
  const auto insert_ts = manager.acquire_insert_timestamp();

  BlockRuntimeWaiters();
  std::promise<bool> timed_out;
  auto timeout_result = timed_out.get_future();
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(50);
  std::thread update([&] {
    try {
      UpdateTimestampLease lease(manager, deadline);
      timed_out.set_value(false);
    } catch (const exception::TransactionTimeoutException&) {
      timed_out.set_value(true);
    }
  });
  ASSERT_TRUE(WaitForBlockedWaiters(1));

  std::this_thread::sleep_until(deadline);
  ReleaseRuntimeWaiters();
  EXPECT_EQ(timeout_result.wait_for(kWaitTimeout), std::future_status::ready);
  EXPECT_TRUE(timeout_result.get());
  update.join();

  manager.release_insert_timestamp(insert_ts);
  const auto next_update = manager.acquire_update_timestamp();
  EXPECT_EQ(next_update, 3U);
  FinishUpdate(manager, next_update);
}

TEST(VersionManagerTimestampWindowTest,
     FullWindowBackpressuresWithoutBlockingUpdateDrain) {
  VersionManager manager;
  InitManager(manager);
  ASSERT_TRUE(manager.try_set_runtime_wait_if_quiescent(&BlockingRuntimeWait));
  const auto oldest = manager.acquire_insert_timestamp();
  for (size_t i = 0; i < TimestampWindow::kWindowSize - 1; ++i) {
    const auto ts = manager.acquire_insert_timestamp();
    manager.release_insert_timestamp(ts);
  }
  EXPECT_EQ(manager.acquire_read_view().visibility_ts, 1U);
  manager.release_read_view();

  BlockRuntimeWaiters();
  std::promise<uint32_t> waiting_insert;
  auto waiting_result = waiting_insert.get_future();
  std::thread waiter([&] {
    const auto ts = manager.acquire_insert_timestamp();
    waiting_insert.set_value(ts);
    manager.release_insert_timestamp(ts);
  });
  ASSERT_TRUE(WaitForBlockedWaiters(1));

  std::promise<uint32_t> update_timestamp;
  auto update_result = update_timestamp.get_future();
  std::thread update([&] {
    const auto ts = manager.acquire_update_timestamp();
    FinishUpdate(manager, ts);
    update_timestamp.set_value(ts);
  });
  // The full-window insert has released admission. The update can therefore
  // close insert admission and waits only for the still-active oldest insert.
  ASSERT_TRUE(WaitForBlockedWaiters(2));
  manager.release_insert_timestamp(oldest);
  ReleaseRuntimeWaiters();

  EXPECT_EQ(update_result.wait_for(kWaitTimeout), std::future_status::ready);
  EXPECT_EQ(update_result.get(), TimestampWindow::kWindowSize + 2);
  EXPECT_EQ(waiting_result.wait_for(kWaitTimeout), std::future_status::ready);
  EXPECT_EQ(waiting_result.get(), TimestampWindow::kWindowSize + 3);
  update.join();
  waiter.join();
}

TEST(VersionManagerTimestampWindowTest, TimestampExhaustionRestoresAdmission) {
  VersionManager manager;
  manager.init_ts({std::numeric_limits<uint32_t>::max() - 1, 0}, 1);

  EXPECT_THROW(manager.acquire_insert_timestamp(), exception::RuntimeError);
  EXPECT_THROW(manager.acquire_update_timestamp(), exception::RuntimeError);
  EXPECT_THROW(manager.acquire_compact_timestamp(), exception::RuntimeError);

  const auto read = manager.acquire_read_view();
  EXPECT_EQ(read.visibility_ts, std::numeric_limits<uint32_t>::max() - 1);
  manager.release_read_view();
  EXPECT_TRUE(manager.try_set_runtime_wait_if_quiescent(&NativeRuntimeWait));
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
