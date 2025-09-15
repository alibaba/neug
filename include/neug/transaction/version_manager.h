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

#ifndef ENGINES_GRAPH_DB_DATABASE_VERSION_MANAGER_H_
#define ENGINES_GRAPH_DB_DATABASE_VERSION_MANAGER_H_

#include <assert.h>
#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>
#include <array>
#include <atomic>
#include <bitset>
#include <condition_variable>
#include <mutex>
#include <shared_mutex>
#include <thread>

#include "glog/logging.h"
#include "libgrape-lite/grape/utils/bitset.h"
#include "libgrape-lite/grape/utils/concurrent_queue.h"

namespace gs {

class IVersionManager {
 public:
  virtual void init_ts(uint32_t ts, int thread_num) = 0;
  virtual uint32_t acquire_read_timestamp() = 0;
  virtual void release_read_timestamp() = 0;
  virtual uint32_t acquire_insert_timestamp() = 0;
  virtual void release_insert_timestamp(uint32_t ts) = 0;
  virtual uint32_t acquire_update_timestamp() = 0;
  virtual void release_update_timestamp(uint32_t ts) = 0;
  virtual bool revert_update_timestamp(uint32_t ts) = 0;
  virtual void clear() = 0;
  virtual ~IVersionManager() {}
};

/**
 * @brief APVersionManager implements the version manager for Analytical
 * Processing (AP) workloads. It allows multiple concurrent read and insert
 * transactions, but only one update transaction at a time. Update transactions
 * will wait for all ongoing read and insert transactions to complete before
 * proceeding. Read and insert transactions will wait if an update transaction
 * is in progress.
 *
 * It will always return the same initial timestamp for all transactions.
 */
class APVersionManager : public IVersionManager {
 public:
  APVersionManager();
  ~APVersionManager();

  void init_ts(uint32_t ts, int thread_num) override;
  void clear() override;
  uint32_t acquire_read_timestamp() override;
  void release_read_timestamp() override;
  uint32_t acquire_insert_timestamp() override;
  void release_insert_timestamp(uint32_t ts) override;
  uint32_t acquire_update_timestamp() override;
  void release_update_timestamp(uint32_t ts) override;
  bool revert_update_timestamp(uint32_t ts) override;

 private:
  std::shared_mutex rw_mutex_;
  std::mutex update_mutex_;
  std::condition_variable_any update_cv_;

  std::atomic<int> active_reads_inserts_{0};
  std::atomic<bool> update_in_progress_{false};

  uint32_t init_ts_;  // Initial timestamp
};

/**
 * @brief TPVersionManager implements the version manager for Transactional
 * Processing (TP) workloads. It supports multiple concurrent read and insert
 * transactions, each receiving the same initial timestamp. Update transactions
 * are exclusive and will wait for all ongoing read and insert transactions to
 * complete before proceeding. The version manager uses a ring buffer to track
 * released timestamps, allowing efficient reuse of timestamps.
 */
class TPVersionManager : public IVersionManager {
 public:
  TPVersionManager();
  ~TPVersionManager();

  void init_ts(uint32_t ts, int thread_num) override;

  void clear() override;
  uint32_t acquire_read_timestamp() override;
  void release_read_timestamp() override;
  uint32_t acquire_insert_timestamp() override;
  void release_insert_timestamp(uint32_t ts) override;
  uint32_t acquire_update_timestamp() override;
  void release_update_timestamp(uint32_t ts) override;
  bool revert_update_timestamp(uint32_t ts) override;

 private:
  int thread_num_;
  std::atomic<uint32_t> write_ts_{1};
  std::atomic<uint32_t> read_ts_{0};

  std::atomic<int> pending_reqs_{0};
  std::atomic<int> pending_update_reqs_{0};

  grape::Bitset buf_;
  grape::SpinLock lock_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_VERSION_MANAGER_H_
