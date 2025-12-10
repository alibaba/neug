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

#include "neug/transaction/version_manager.h"

#include <glog/logging.h>
#include <chrono>
#include <ostream>
#include <thread>

#include "neug/utils/bitset.h"
#include "neug/utils/likely.h"

namespace gs {

constexpr static uint32_t ring_buf_size = 1024 * 1024;
constexpr static uint32_t ring_index_mask = ring_buf_size - 1;

APVersionManager::APVersionManager()
    : active_reads_inserts_(0), update_in_progress_(false), init_ts_(0) {}

APVersionManager::~APVersionManager() {}

void APVersionManager::init_ts(uint32_t ts, int thread_num) {
  init_ts_ = ts;
  clear();
}

void APVersionManager::clear() {
  std::lock_guard<std::mutex> update_lock(update_mutex_);

  active_reads_inserts_.store(0);
  update_in_progress_.store(false);
}

uint32_t APVersionManager::acquire_read_timestamp() {
  std::unique_lock<std::mutex> update_lock(update_mutex_);
  update_cv_.wait(update_lock, [this] { return !update_in_progress_.load(); });

  active_reads_inserts_.fetch_add(1);
  return init_ts_;
}

void APVersionManager::release_read_timestamp() {
  // Decrement active reads counter
  if (active_reads_inserts_.load() <= 0) {
    LOG(ERROR) << "release_read_timestamp called without matching acquire";
    return;
  }
  active_reads_inserts_.fetch_sub(1);
  update_cv_.notify_all();  // Notify waiting update operations
}

uint32_t APVersionManager::acquire_insert_timestamp() {
  std::unique_lock<std::mutex> update_lock(update_mutex_);
  update_cv_.wait(update_lock, [this] { return !update_in_progress_.load(); });

  active_reads_inserts_.fetch_add(1);
  return init_ts_;
}

void APVersionManager::release_insert_timestamp(uint32_t ts) {
  active_reads_inserts_.fetch_sub(1);
  update_cv_.notify_all();
}

uint32_t APVersionManager::acquire_update_timestamp() {
  std::unique_lock<std::mutex> update_lock(update_mutex_);

  update_cv_.wait(update_lock, [this] {
    return active_reads_inserts_.load() == 0 && !update_in_progress_.load();
  });
  update_in_progress_.store(true);
  return init_ts_;
}

void APVersionManager::release_update_timestamp(uint32_t ts) {
  if (!update_in_progress_.load()) {
    LOG(ERROR) << "release_update_timestamp called without matching acquire";
    return;
  }
  update_in_progress_.store(false);
  update_cv_.notify_all();
}

bool APVersionManager::revert_update_timestamp(uint32_t ts) {
  std::lock_guard<std::mutex> update_lock(update_mutex_);

  if (update_in_progress_.load()) {
    update_in_progress_.store(false);
    update_cv_.notify_all();
    return true;
  }

  return false;
}

// TPVersionManager implementation

TPVersionManager::TPVersionManager() {
  buf_.resize(ring_buf_size);
  buf_.reset_all();
}

TPVersionManager::~TPVersionManager() {}

void TPVersionManager::init_ts(uint32_t ts, int thread_num) {
  write_ts_.store(ts + 1);
  read_ts_.store(ts);
  thread_num_ = thread_num;
}

void TPVersionManager::clear() {
  write_ts_.store(1);
  read_ts_.store(0);
  pending_reqs_.store(0);
  buf_.reset_all();
}

uint32_t TPVersionManager::acquire_read_timestamp() {
  int pr = pending_reqs_.fetch_add(1);
  if (NEUG_LIKELY(pr >= 0)) {
    return read_ts_.load();
  } else {
    --pending_reqs_;
    while (true) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
      if (pending_reqs_.load() >= 0) {
        pr = pending_reqs_.fetch_add(1);
        if (pr >= 0) {
          return read_ts_.load();
        } else {
          --pending_reqs_;
        }
      }
    }
  }
}

void TPVersionManager::release_read_timestamp() { pending_reqs_.fetch_sub(1); }

uint32_t TPVersionManager::acquire_insert_timestamp() {
  int pr = pending_reqs_.fetch_add(1);
  if (NEUG_LIKELY(pr >= 0)) {
    return write_ts_.fetch_add(1);
  } else {
    --pending_reqs_;
    while (true) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
      if (pending_reqs_.load() >= 0) {
        pr = pending_reqs_.fetch_add(1);
        if (pr >= 0) {
          return write_ts_.fetch_add(1);
        } else {
          --pending_reqs_;
        }
      }
    }
  }
}

void TPVersionManager::release_insert_timestamp(uint32_t ts) {
  lock_.lock();
  if (ts == read_ts_.load() + 1) {
    while (buf_.atomic_reset_with_ret((ts + 1) & ring_index_mask)) {
      ++ts;
    }
    read_ts_.store(ts);
  } else {
    buf_.atomic_set(ts & ring_index_mask);
  }
  lock_.unlock();

  pending_reqs_.fetch_sub(1);
}

uint32_t TPVersionManager::acquire_update_timestamp() {
  int expected_update_reqs = 0;
  while (
      !pending_update_reqs_.compare_exchange_strong(expected_update_reqs, 1)) {
    expected_update_reqs = 0;
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }

  int pr = pending_reqs_.fetch_sub(thread_num_);
  if (pr != 0) {
    while (pending_reqs_.load() != -thread_num_) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  }

  return write_ts_.fetch_add(1);
}
void TPVersionManager::release_update_timestamp(uint32_t ts) {
  lock_.lock();
  if (ts == read_ts_.load() + 1) {
    read_ts_.store(ts);
  } else {
    LOG(ERROR) << "read ts is expected to be " << ts - 1 << ", while it is "
               << read_ts_.load();
    buf_.atomic_set(ts & ring_index_mask);
  }
  lock_.unlock();

  pending_reqs_ += thread_num_;
  pending_update_reqs_.store(0);
}

bool TPVersionManager::revert_update_timestamp(uint32_t ts) {
  uint32_t expected_ts = ts + 1;
  if (write_ts_.compare_exchange_strong(expected_ts, ts)) {
    pending_reqs_ += thread_num_;
    pending_update_reqs_.store(0);
    return true;
  }
  return false;
}

}  // namespace gs
