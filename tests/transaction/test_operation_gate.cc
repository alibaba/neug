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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

#include "neug/transaction/version_manager.h"
#include "neug/utils/exception/exception.h"

namespace neug {

namespace {

using AdmissionState = detail::AdmissionState;
using OperationGateWord = detail::OperationGateWord;

constexpr auto kWaitTimeout = std::chrono::seconds(10);
std::atomic<uint64_t> g_runtime_wait_calls{0};

void CountingRuntimeWait(RuntimeWaitAction) noexcept {
  g_runtime_wait_calls.fetch_add(1, std::memory_order_relaxed);
  std::this_thread::yield();
}

void ResetRuntimeWaitCalls() {
  g_runtime_wait_calls.store(0, std::memory_order_relaxed);
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

void InstallCountingRuntimeWait(OperationGate& gate) {
  ASSERT_TRUE(gate.try_seal_open_state());
  gate.store_runtime_wait(&CountingRuntimeWait);
  gate.unseal_to_open();
}

uint32_t Readers(const OperationGate& gate) {
  return OperationGateWord::readers(gate.load_acquire());
}

uint32_t Inserters(const OperationGate& gate) {
  return OperationGateWord::inserters(gate.load_acquire());
}

TEST(OperationGateLeaseTest, SharedLeaseCountsAndReleasesReaderAdmission) {
  OperationGate gate;
  {
    SharedOperationLease first(gate);
    EXPECT_TRUE(first.active());
    EXPECT_EQ(Readers(gate), 1U);
    {
      SharedOperationLease second(gate);
      EXPECT_EQ(Readers(gate), 2U);
    }
    EXPECT_EQ(Readers(gate), 1U);
    first.release();
    EXPECT_FALSE(first.active());
    EXPECT_EQ(Readers(gate), 0U);
  }
  EXPECT_EQ(OperationGateWord::phase(gate.load_acquire()),
            AdmissionState::kOpen);
}

TEST(OperationGateLeaseTest, LeaseMoveTransfersOwnershipWithoutReleasing) {
  OperationGate gate;
  {
    SharedOperationLease original(gate);
    SharedOperationLease owner(std::move(original));
    EXPECT_FALSE(original.active());
    EXPECT_TRUE(owner.active());
    EXPECT_EQ(Readers(gate), 1U);
    // A moved-from lease must not release the transferred count.
    original.release();
    EXPECT_EQ(Readers(gate), 1U);
  }
  EXPECT_EQ(Readers(gate), 0U);

  ExclusiveOperationLease moved_from_source(gate);
  ExclusiveOperationLease owner(std::move(moved_from_source));
  EXPECT_FALSE(moved_from_source.active());
  EXPECT_TRUE(owner.active());
  EXPECT_EQ(OperationGateWord::phase(gate.load_acquire()),
            AdmissionState::kAllBlocked);
  moved_from_source.release();
  EXPECT_EQ(OperationGateWord::phase(gate.load_acquire()),
            AdmissionState::kAllBlocked);
  owner.release();
  EXPECT_EQ(OperationGateWord::phase(gate.load_acquire()),
            AdmissionState::kOpen);
}

TEST(OperationGateLeaseTest, ExclusiveLeaseBlocksNewSharedAdmission) {
  OperationGate gate;
  InstallCountingRuntimeWait(gate);

  ExclusiveOperationLease exclusive(gate);
  EXPECT_TRUE(exclusive.active());
  EXPECT_EQ(OperationGateWord::phase(gate.load_acquire()),
            AdmissionState::kAllBlocked);
  EXPECT_FALSE(gate.try_seal_open_state());

  ResetRuntimeWaitCalls();
  std::atomic<bool> admitted{false};
  std::thread reader([&] {
    SharedOperationLease lease(gate);
    admitted.store(true, std::memory_order_release);
  });
  EXPECT_TRUE(WaitForRuntimeWait());
  EXPECT_FALSE(admitted.load(std::memory_order_acquire));

  exclusive.release();
  EXPECT_TRUE(
      WaitUntil([&] { return admitted.load(std::memory_order_acquire); }));
  reader.join();
  EXPECT_EQ(OperationGateWord::phase(gate.load_acquire()),
            AdmissionState::kOpen);
}

TEST(OperationGateLeaseTest, ExclusiveLeaseBlocksNewInserterAdmission) {
  OperationGate gate;
  InstallCountingRuntimeWait(gate);

  ExclusiveOperationLease exclusive(gate);
  ResetRuntimeWaitCalls();
  std::atomic<bool> admitted{false};
  std::thread inserter([&] {
    gate.acquire_inserter();
    admitted.store(true, std::memory_order_release);
    gate.release_inserter();
  });
  EXPECT_TRUE(WaitForRuntimeWait());
  EXPECT_FALSE(admitted.load(std::memory_order_acquire));

  exclusive.release();
  EXPECT_TRUE(
      WaitUntil([&] { return admitted.load(std::memory_order_acquire); }));
  inserter.join();
}

TEST(OperationGateLeaseTest, ExclusiveLeaseWaitsForExistingSharedAdmission) {
  OperationGate gate;
  InstallCountingRuntimeWait(gate);

  SharedOperationLease shared(gate);
  ResetRuntimeWaitCalls();
  std::promise<void> acquired;
  auto acquired_future = acquired.get_future();
  std::thread exclusive_thread([&] {
    ExclusiveOperationLease exclusive(gate);
    acquired.set_value();
    exclusive.release();
  });

  // The exclusive acquisition must park in the reader drain while the shared
  // lease is still held.
  EXPECT_TRUE(WaitForRuntimeWait());
  EXPECT_NE(acquired_future.wait_for(std::chrono::milliseconds(0)),
            std::future_status::ready);

  shared.release();
  ASSERT_EQ(acquired_future.wait_for(kWaitTimeout), std::future_status::ready);
  exclusive_thread.join();
  EXPECT_EQ(OperationGateWord::phase(gate.load_acquire()),
            AdmissionState::kOpen);
}

TEST(OperationGateLeaseTest,
     ExclusiveLeaseDoesNotOverlapAdmittedReadersOrInserters) {
  OperationGate gate;

  std::atomic<bool> stop{false};
  std::atomic<bool> exclusive_active{false};
  std::atomic<int> observed_readers{0};
  std::atomic<int> observed_inserters{0};
  std::atomic<int> violations{0};

  auto reader = [&]() {
    while (!stop.load(std::memory_order_acquire)) {
      SharedOperationLease lease(gate);
      observed_readers.fetch_add(1, std::memory_order_seq_cst);
      if (exclusive_active.load(std::memory_order_seq_cst)) {
        violations.fetch_add(1, std::memory_order_relaxed);
      }
      std::this_thread::yield();
      if (exclusive_active.load(std::memory_order_seq_cst)) {
        violations.fetch_add(1, std::memory_order_relaxed);
      }
      observed_readers.fetch_sub(1, std::memory_order_seq_cst);
    }
  };
  auto inserter = [&]() {
    while (!stop.load(std::memory_order_acquire)) {
      gate.acquire_inserter();
      observed_inserters.fetch_add(1, std::memory_order_seq_cst);
      if (exclusive_active.load(std::memory_order_seq_cst)) {
        violations.fetch_add(1, std::memory_order_relaxed);
      }
      std::this_thread::yield();
      if (exclusive_active.load(std::memory_order_seq_cst)) {
        violations.fetch_add(1, std::memory_order_relaxed);
      }
      observed_inserters.fetch_sub(1, std::memory_order_seq_cst);
      gate.release_inserter();
    }
  };

  std::thread reader_thread(reader);
  std::thread inserter_thread(inserter);
  for (int i = 0; i < 1000; ++i) {
    ExclusiveOperationLease exclusive(gate);
    exclusive_active.store(true, std::memory_order_seq_cst);
    if (observed_readers.load(std::memory_order_seq_cst) != 0 ||
        observed_inserters.load(std::memory_order_seq_cst) != 0) {
      violations.fetch_add(1, std::memory_order_relaxed);
    }
    std::this_thread::yield();
    exclusive_active.store(false, std::memory_order_seq_cst);
  }
  stop.store(true, std::memory_order_release);
  reader_thread.join();
  inserter_thread.join();

  EXPECT_EQ(violations.load(std::memory_order_relaxed), 0);
  EXPECT_EQ(Readers(gate), 0U);
  EXPECT_EQ(Inserters(gate), 0U);
  EXPECT_EQ(OperationGateWord::phase(gate.load_acquire()),
            AdmissionState::kOpen);
}

TEST(OperationGateAdmissionTest, EnterPhaseTimeoutLeavesGateUnchanged) {
  OperationGate gate;
  InstallCountingRuntimeWait(gate);
  gate.enter_phase(AdmissionState::kInsertsBlocked);

  ResetRuntimeWaitCalls();
  std::atomic<bool> result{true};
  std::thread waiter([&] {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(50);
    result.store(gate.enter_phase_until(AdmissionState::kAllBlocked, deadline),
                 std::memory_order_release);
  });
  EXPECT_TRUE(WaitForRuntimeWait());
  waiter.join();

  // The timed acquisition must not leak admission state on timeout: the
  // holder keeps its phase and no counter changed.
  EXPECT_FALSE(result.load(std::memory_order_acquire));
  EXPECT_EQ(OperationGateWord::phase(gate.load_acquire()),
            AdmissionState::kInsertsBlocked);
  EXPECT_EQ(Readers(gate), 0U);
  EXPECT_EQ(Inserters(gate), 0U);

  gate.transition_phase(AdmissionState::kInsertsBlocked, AdmissionState::kOpen);
  EXPECT_TRUE(gate.enter_phase_until(
      AdmissionState::kAllBlocked,
      std::chrono::steady_clock::now() + std::chrono::seconds(1)));
  gate.transition_phase(AdmissionState::kAllBlocked, AdmissionState::kOpen);
}

TEST(OperationGateAdmissionTest, SealRequiresEmptyOpenGate) {
  OperationGate gate;

  {
    SharedOperationLease lease(gate);
    EXPECT_FALSE(gate.try_seal_open_state());
  }
  gate.acquire_inserter();
  EXPECT_FALSE(gate.try_seal_open_state());
  gate.release_inserter();

  gate.enter_phase(AdmissionState::kInsertsBlocked);
  EXPECT_FALSE(gate.try_seal_open_state());
  gate.transition_phase(AdmissionState::kInsertsBlocked, AdmissionState::kOpen);

  EXPECT_TRUE(gate.try_seal_open_state());
  gate.unseal_to_open();
  EXPECT_EQ(OperationGateWord::phase(gate.load_acquire()),
            AdmissionState::kOpen);
}

TEST(OperationGateAdmissionTest,
     UnsealPreservesDelayedSpeculativeReaderRollback) {
  OperationGate gate;

  // Establish the same packed state produced when a reader loaded kOpen
  // before a lifecycle seal and performed its speculative fetch_add after the
  // seal CAS: kAllBlocked with one transient reader count.
  gate.acquire_shared();
  gate.enter_phase(AdmissionState::kAllBlocked);
  ASSERT_EQ(Readers(gate), 1U);

  gate.unseal_to_open();
  EXPECT_EQ(OperationGateWord::phase(gate.load_acquire()),
            AdmissionState::kOpen);
  EXPECT_EQ(Readers(gate), 1U);

  // Model the delayed reader observing kAllBlocked and rolling itself back.
  gate.release_shared();
  EXPECT_EQ(Readers(gate), 0U);
}

TEST(OperationGateAdmissionTest, TimedSharedLeaseDoesNotLeakReaderAdmission) {
  OperationGate gate;
  InstallCountingRuntimeWait(gate);
  ExclusiveOperationLease exclusive(gate);

  EXPECT_THROW(SharedOperationLease(gate, std::chrono::steady_clock::now() +
                                              std::chrono::milliseconds(20)),
               exception::TransactionTimeoutException);
  EXPECT_EQ(Readers(gate), 0U);
  EXPECT_EQ(OperationGateWord::phase(gate.load_acquire()),
            AdmissionState::kAllBlocked);

  exclusive.release();
  SharedOperationLease admitted(gate);
  EXPECT_TRUE(admitted.active());
}

TEST(OperationGateAdmissionTest, ExpiredOpenGateAdmissionLeavesGateUnchanged) {
  OperationGate gate;
  const auto expired = std::chrono::steady_clock::time_point::min();

  EXPECT_THROW(SharedOperationLease(gate, expired),
               exception::TransactionTimeoutException);
  EXPECT_EQ(Readers(gate), 0U);
  EXPECT_EQ(Inserters(gate), 0U);
  EXPECT_EQ(OperationGateWord::phase(gate.load_acquire()),
            AdmissionState::kOpen);

  EXPECT_FALSE(gate.enter_phase_until(AdmissionState::kAllBlocked, expired));
  EXPECT_EQ(Readers(gate), 0U);
  EXPECT_EQ(Inserters(gate), 0U);
  EXPECT_EQ(OperationGateWord::phase(gate.load_acquire()),
            AdmissionState::kOpen);
}

TEST(OperationGateAdmissionTest,
     TimedExclusiveReaderDrainRestoresOpenAdmission) {
  OperationGate gate;
  InstallCountingRuntimeWait(gate);
  SharedOperationLease active_reader(gate);

  EXPECT_THROW(ExclusiveOperationLease(gate, std::chrono::steady_clock::now() +
                                                 std::chrono::milliseconds(20)),
               exception::TransactionTimeoutException);
  EXPECT_EQ(OperationGateWord::phase(gate.load_acquire()),
            AdmissionState::kOpen);
  EXPECT_EQ(Readers(gate), 1U);
  EXPECT_EQ(Inserters(gate), 0U);

  SharedOperationLease next_reader(gate);
  EXPECT_EQ(Readers(gate), 2U);
}

TEST(OperationGateAdmissionTest,
     TimedExclusiveInserterDrainRestoresOpenAdmission) {
  OperationGate gate;
  InstallCountingRuntimeWait(gate);
  gate.acquire_inserter();

  EXPECT_THROW(ExclusiveOperationLease(gate, std::chrono::steady_clock::now() +
                                                 std::chrono::milliseconds(20)),
               exception::TransactionTimeoutException);
  EXPECT_EQ(OperationGateWord::phase(gate.load_acquire()),
            AdmissionState::kOpen);
  EXPECT_EQ(Readers(gate), 0U);
  EXPECT_EQ(Inserters(gate), 1U);

  gate.release_inserter();
  ExclusiveOperationLease next_writer(
      gate, std::chrono::steady_clock::now() + std::chrono::seconds(1));
  EXPECT_TRUE(next_writer.active());
}

TEST(OperationGateAdmissionTest, ReleaseWithoutAdmissionThrows) {
  OperationGate gate;
  EXPECT_THROW(gate.release_shared(), exception::InternalException);
  EXPECT_THROW(gate.release_inserter(), exception::InternalException);
  EXPECT_THROW(
      gate.transition_phase(AdmissionState::kAllBlocked, AdmissionState::kOpen),
      exception::InternalException);
}

}  // namespace

}  // namespace neug
