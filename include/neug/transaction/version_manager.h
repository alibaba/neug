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
#include <optional>

#include "neug/transaction/operation_gate.h"
#include "neug/transaction/runtime_wait.h"
#include "neug/transaction/timestamp_window.h"
#include "neug/utils/spinlock.h"

namespace neug {

class UpdateTimestampLease;

/**
 * @brief Atomically published reader-visible state.
 *
 * The visibility timestamp and snapshot generation form one logical value:
 * readers must never combine either field with a snapshot from another
 * publication generation.
 */
struct PublishedReadView {
  uint32_t visibility_ts;
  uint32_t snapshot_generation;
};

inline uint64_t PackPublishedReadView(const PublishedReadView& view) {
  return (static_cast<uint64_t>(view.snapshot_generation) << 32) |
         view.visibility_ts;
}

inline PublishedReadView UnpackPublishedReadView(uint64_t packed) {
  return {static_cast<uint32_t>(packed), static_cast<uint32_t>(packed >> 32)};
}

struct ReadOperationLease {
  PublishedReadView published_view;
  SharedOperationLease admission;
};

/**
 * @brief Unified interface for transaction timestamp and concurrency control.
 *
 * IVersionManager defines the contract for managing timestamp acquisition,
 * release, and inter-transaction synchronization. Each transaction type
 * (Read, Insert, Update, Compact) interacts with this interface to obtain
 * a timestamp and to coordinate exclusive/shared access with other
 * transaction types.
 *
 * The current implementation is VersionManager, which uses one packed atomic
 * for the admission phase and active-operation counters.
 *
 * @see VersionManager for the concrete implementation and its
 *      concurrency matrix.
 * @see OperationGate for the packed admission gate primitive.
 */
class IVersionManager {
 public:
  // Initialize the timestamp timeline and its coherently installed snapshot.
  virtual void init_ts(PublishedReadView initial_read_view, int thread_num) = 0;
  // Lifecycle-only operation. The implementation closes admission while
  // checking quiescence, but the caller must still prevent new transaction
  // attempts so no already-waiting caller retains the previous runtime wait.
  virtual bool try_set_runtime_wait_if_quiescent(
      RuntimeWaitFn runtime_wait) noexcept = 0;
  // Create a per-wait cursor without exposing the runtime callback itself.
  RuntimeBackoff make_runtime_backoff() const noexcept {
    return RuntimeBackoff(runtime_wait_impl());
  }
  // Acquire one shared operation lease and the TP coherent read view published
  // behind it. The returned lease owns the reader admission directly.
  virtual ReadOperationLease acquire_read_operation() = 0;
  virtual uint32_t acquire_insert_timestamp() = 0;
  virtual void release_insert_timestamp(uint32_t ts) = 0;
  // Waiters directly contend the admission phase. Acquisition order is
  // intentionally unspecified; the successful phase CAS linearizes ownership.
  virtual uint32_t acquire_update_timestamp() = 0;
  virtual void begin_update_commit(uint32_t ts) = 0;
  // May invoke the runtime waiter. Checkpoint callers must enter commit and
  // drain readers before acquiring checkpoint-manager or other
  // OS-thread-owned mutexes.
  virtual void drain_readers() = 0;
  // A present generation has already been installed in GraphSnapshotStore.
  // nullopt keeps the currently installed generation.
  virtual void finish_update_timestamp(
      uint32_t ts,
      std::optional<uint32_t> installed_snapshot_generation) noexcept = 0;
  virtual uint32_t acquire_compact_timestamp() = 0;
  virtual void release_compact_timestamp(uint32_t ts) = 0;
  virtual void revert_compact_timestamp(uint32_t ts) = 0;

  virtual ~IVersionManager() {}

 protected:
  virtual RuntimeWaitFn runtime_wait_impl() const noexcept = 0;

 private:
  friend class UpdateTimestampLease;

  // Timed acquisition is intentionally lease-only: callers must not receive a
  // raw timestamp without immediately establishing RAII ownership.
  virtual uint32_t acquire_update_timestamp_until(
      std::chrono::steady_clock::time_point deadline) = 0;

  /// Complete an exclusive update after external state has moved to a new
  /// timeline. Preserve the current snapshot generation and publish visibility
  /// timestamp zero before reopening admission.
  virtual void finish_update_and_reset_timeline(uint32_t ts) noexcept = 0;
};

/**
 * @brief VersionManager — concurrency control via atomic state machine.
 *
 * The packed operation gate transitions:
 * - Update: open → inserts-blocked → all-blocked → open.
 * - Compact: open → all-blocked → open.
 *
 * Layout: phase: 2 bits | active inserters: 31 bits | active readers: 31 bits.
 * Reader admission uses fetch_add; inserter admission and phase changes use
 * CAS on the same word. An operation is therefore unambiguously counted before
 * a blocking transition or rejected after it. Reader and inserter counts
 * cannot exceed 2^31 - 1.
 *
 * Concurrency (new acquisitions; in-flight ops are not interrupted):
 *
 *   |               | Read | Insert | Update-exec | Update-commit | Compact |
 *   | Read          | yes  | yes    | yes         |   no*         |   no    |
 *   | Insert        | yes  | yes    |   no        |   no          |   no    |
 *   | Update-exec   | yes  |  no    |   no        |    -          |   no    |
 *   | Update-commit |  no* |  no    |   -         |   no          |   no    |
 *   | Compact       |  no  |  no    |   no        |   no          |   no    |
 *   *New reads wait with the configured runtime backoff; already-acquired
 *   reads continue.
 *
 * Mechanism:
 * - write_ts_: next available write timestamp (monotonically increasing).
 *   Storage compaction may reset per-record visibility timestamps to zero, but
 *   transaction/WAL timestamps must never be reset within a WAL timeline.
 * - read_ts_: highest timestamp fully committed and visible to all readers.
 * - gate_: packed operation gate combining the admission phase and counters.
 *   Update execution blocks inserters; update commit and compaction block
 *   both readers and inserters.
 * - acquire_read_operation owns one shared gate lease and then captures the
 *   atomically published timestamp/generation pair.
 * - begin_update_commit blocks new readers through the same control word.
 *   A reader is therefore either counted before the transition or rejected
 *   after it and retried.
 * - SpinLock lock_: serializes read_ts advancement (check-and-advance
 *   in complete_write_timestamp).
 * - TimestampWindow ts_window_: tracks completed timestamps for read_ts
 * reclamation.
 */
class VersionManager : public IVersionManager {
 public:
  VersionManager();
  ~VersionManager() override = default;

  void init_ts(PublishedReadView initial_read_view, int thread_num) override;
  bool try_set_runtime_wait_if_quiescent(
      RuntimeWaitFn runtime_wait) noexcept override;

  ReadOperationLease acquire_read_operation() override;
  uint32_t acquire_insert_timestamp() override;
  void release_insert_timestamp(uint32_t ts) override;
  uint32_t acquire_update_timestamp() override;
  void begin_update_commit(uint32_t ts) override;
  void drain_readers() override;
  void finish_update_timestamp(
      uint32_t ts,
      std::optional<uint32_t> installed_snapshot_generation) noexcept override;
  uint32_t acquire_compact_timestamp() override;
  void release_compact_timestamp(uint32_t ts) override;
  void revert_compact_timestamp(uint32_t ts) override;

 private:
  uint32_t acquire_update_timestamp_until(
      std::chrono::steady_clock::time_point deadline) override;
  void finish_update_and_reset_timeline(uint32_t ts) noexcept override;

  int thread_num_;
  uint32_t reserve_update_timestamp();
  void complete_write_timestamp(uint32_t ts);
  void advance_read_ts_locked();
  RuntimeWaitFn runtime_wait_impl() const noexcept override;

  std::atomic<uint32_t> write_ts_{1};
  std::atomic<uint32_t> read_ts_{1};
  std::atomic<uint32_t> installed_snapshot_generation_{0};
  std::atomic<uint64_t> published_read_view_{PackPublishedReadView({1, 0})};

  OperationGate gate_;

  TimestampWindow ts_window_;

  SpinLock lock_;
};

}  // namespace neug
