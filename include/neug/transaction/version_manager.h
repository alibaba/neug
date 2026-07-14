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

#include "neug/transaction/timestamp_window.h"
#include "neug/utils/spinlock.h"

namespace neug {

/**
 * @brief Unified interface for transaction timestamp and concurrency control.
 *
 * IVersionManager defines the contract for managing timestamp acquisition,
 * release, and inter-transaction synchronization. Each transaction type
 * (Read, Insert, Update, Compact) interacts with this interface to obtain
 * a timestamp and to coordinate exclusive/shared access with other
 * transaction types.
 *
 * The current implementation is VersionManager, which uses an atomic
 * state machine (update_state_) plus atomic counters for concurrency
 * control, replacing the earlier rw_mutex_-based design.
 *
 * @see VersionManager for the concrete implementation and its
 *      concurrency matrix.
 */
class IVersionManager {
 public:
  virtual void init_ts(uint32_t ts, int thread_num) = 0;
  virtual uint32_t acquire_read_timestamp() = 0;
  virtual void release_read_timestamp() = 0;
  virtual uint32_t acquire_insert_timestamp() = 0;
  virtual void release_insert_timestamp(uint32_t ts) = 0;
  virtual uint32_t acquire_update_timestamp() = 0;
  virtual void begin_update_commit(uint32_t ts) = 0;
  virtual void release_update_timestamp(uint32_t ts) = 0;
  virtual uint32_t acquire_compact_timestamp() = 0;
  virtual void release_compact_timestamp(uint32_t ts) = 0;
  virtual void revert_compact_timestamp(uint32_t ts) = 0;

  virtual ~IVersionManager() {}
};

/**
 * @brief VersionManager — concurrency control via atomic state machine.
 *
 * update_state_ transitions: 0→1 (update-exec) →2 (update-commit);
 *                            0→2 (compact).
 *
 * Concurrency (new acquisitions; in-flight ops are not interrupted):
 *
 *   |               | Read | Insert | Update-exec | Update-commit | Compact |
 *   | Read          | yes  | yes    | yes         |   no*         |   no    |
 *   | Insert        | yes  | yes    |   no        |   no          |   no    |
 *   | Update-exec   | yes  |  no    |   no        |    -          |   no    |
 *   | Update-commit |  no* |  no    |   -         |   no          |   no    |
 *   | Compact       |  no  |  no    |   no        |   no          |   no    |
 *   *New reads spin-wait; already-acquired reads continue.
 *
 * Mechanism:
 * - write_ts_: next available write timestamp (monotonically increasing).
 *   Storage compaction may reset per-record visibility timestamps to zero, but
 *   transaction/WAL timestamps must never be reset within a WAL timeline.
 * - read_ts_: highest timestamp fully committed and visible to all readers.
 * - active_readers_/active_inserters_: atomic counters for in-flight ops.
 * - update_state_: 0=normal, 1=update-exec (inserters drained),
 *   2=update-commit (new reads block; existing reads continue) /
 *   compact (readers+inserters drained).
 * - acquire_read_timestamp uses a double-check pattern (pre-check + increment
 *   + post-check) to prevent ABA races with begin_update_commit.
 * - begin_update_commit uses seq_cst store + drain spin to ensure
 *   any reader in the ABA window has rolled back before proceeding.
 * - SpinLock lock_: serializes read_ts advancement (check-and-advance
 *   in complete_write_timestamp).
 * - TimestampWindow ts_window_: tracks completed timestamps for read_ts
 * reclamation.
 */
class VersionManager : public IVersionManager {
 public:
  VersionManager();
  ~VersionManager();

  void init_ts(uint32_t ts, int thread_num) override;

  uint32_t acquire_read_timestamp() override;
  void release_read_timestamp() override;
  uint32_t acquire_insert_timestamp() override;
  void release_insert_timestamp(uint32_t ts) override;
  uint32_t acquire_update_timestamp() override;
  void begin_update_commit(uint32_t ts) override;
  void release_update_timestamp(uint32_t ts) override;
  uint32_t acquire_compact_timestamp() override;
  void release_compact_timestamp(uint32_t ts) override;
  void revert_compact_timestamp(uint32_t ts) override;

 private:
  int thread_num_;
  uint32_t acquire_read_timestamp_slow();
  uint32_t acquire_insert_timestamp_slow();
  void complete_write_timestamp(uint32_t ts);
  void advance_read_ts_locked();

  std::atomic<uint32_t> write_ts_{1};
  std::atomic<uint32_t> read_ts_{1};

  std::atomic<int> active_readers_{0};
  std::atomic<int> active_inserters_{0};

  std::atomic<int> update_state_{0};

  TimestampWindow ts_window_;

  SpinLock lock_;
};

}  // namespace neug
