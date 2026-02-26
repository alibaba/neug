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
#include <condition_variable>
#include <mutex>
#include <shared_mutex>

#include "neug/utils/bitset.h"
#include "neug/utils/spinlock.h"

namespace neug {

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

  Bitset buf_;
  SpinLock lock_;
};

}  // namespace neug
