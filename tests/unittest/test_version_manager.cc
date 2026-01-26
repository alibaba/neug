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

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

namespace neug {
namespace test {

class APVersionManagerTest : public ::testing::Test {
 protected:
  void SetUp() override { vm_ = std::make_unique<APVersionManager>(); }

  void TearDown() override { vm_.reset(); }

  std::unique_ptr<APVersionManager> vm_;
};

// Test basic initialization and clearing
TEST_F(APVersionManagerTest, InitializationAndClear) {
  const uint32_t init_ts = 100;
  const int thread_num = 4;

  vm_->init_ts(init_ts, thread_num);

  // After initialization, should be able to acquire read timestamp
  uint32_t read_ts = vm_->acquire_read_timestamp();
  EXPECT_EQ(read_ts, init_ts);
  vm_->release_read_timestamp();

  // Clear should reset the state
  vm_->clear();

  // Should still work after clear
  read_ts = vm_->acquire_read_timestamp();
  EXPECT_EQ(read_ts, init_ts);
  vm_->release_read_timestamp();
}

// Test basic read timestamp operations
TEST_F(APVersionManagerTest, BasicReadOperations) {
  const uint32_t init_ts = 42;
  vm_->init_ts(init_ts, 1);

  // Test single read acquire/release
  uint32_t ts1 = vm_->acquire_read_timestamp();
  EXPECT_EQ(ts1, init_ts);
  vm_->release_read_timestamp();

  // Test multiple sequential reads
  uint32_t ts2 = vm_->acquire_read_timestamp();
  uint32_t ts3 = vm_->acquire_read_timestamp();
  EXPECT_EQ(ts2, init_ts);
  EXPECT_EQ(ts3, init_ts);
  vm_->release_read_timestamp();
  vm_->release_read_timestamp();
}

// Test basic insert timestamp operations
TEST_F(APVersionManagerTest, BasicInsertOperations) {
  const uint32_t init_ts = 123;
  vm_->init_ts(init_ts, 1);

  // Test single insert acquire/release
  uint32_t ts1 = vm_->acquire_insert_timestamp();
  EXPECT_EQ(ts1, init_ts);
  vm_->release_insert_timestamp(ts1);

  // Test multiple sequential inserts
  uint32_t ts2 = vm_->acquire_insert_timestamp();
  uint32_t ts3 = vm_->acquire_insert_timestamp();
  EXPECT_EQ(ts2, init_ts);
  EXPECT_EQ(ts3, init_ts);
  vm_->release_insert_timestamp(ts2);
  vm_->release_insert_timestamp(ts3);
}

// Test basic update timestamp operations
TEST_F(APVersionManagerTest, BasicUpdateOperations) {
  const uint32_t init_ts = 456;
  vm_->init_ts(init_ts, 1);

  // Test single update acquire/release
  uint32_t ts = vm_->acquire_update_timestamp();
  EXPECT_EQ(ts, init_ts);
  vm_->release_update_timestamp(ts);

  // Test sequential updates
  uint32_t ts1 = vm_->acquire_update_timestamp();
  vm_->release_update_timestamp(ts1);
  uint32_t ts2 = vm_->acquire_update_timestamp();
  vm_->release_update_timestamp(ts2);
  EXPECT_EQ(ts1, init_ts);
  EXPECT_EQ(ts2, init_ts);
}

// Test update revert functionality
TEST_F(APVersionManagerTest, UpdateRevert) {
  vm_->init_ts(100, 1);

  // Test revert when update is in progress
  uint32_t ts = vm_->acquire_update_timestamp();
  bool reverted = vm_->revert_update_timestamp(ts);
  EXPECT_TRUE(reverted);

  // Test revert when no update is in progress
  bool reverted_again = vm_->revert_update_timestamp(ts);
  EXPECT_FALSE(reverted_again);

  // After revert, should be able to acquire read timestamp
  uint32_t read_ts = vm_->acquire_read_timestamp();
  EXPECT_EQ(read_ts, 100);
  vm_->release_read_timestamp();
}

// Test concurrent read operations
TEST_F(APVersionManagerTest, ConcurrentReads) {
  const uint32_t init_ts = 200;
  const int num_threads = 10;
  const int operations_per_thread = 100;

  vm_->init_ts(init_ts, num_threads);

  std::vector<std::thread> threads;
  std::atomic<int> successful_operations{0};
  std::atomic<bool> all_timestamps_correct{true};

  // Launch multiple threads performing read operations
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < operations_per_thread; ++j) {
        uint32_t ts = vm_->acquire_read_timestamp();
        if (ts != init_ts) {
          all_timestamps_correct.store(false);
        }

        // Simulate some work
        std::this_thread::sleep_for(std::chrono::microseconds(1));

        vm_->release_read_timestamp();
        successful_operations.fetch_add(1);
      }
    });
  }

  // Wait for all threads to complete
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(successful_operations.load(), num_threads * operations_per_thread);
  EXPECT_TRUE(all_timestamps_correct.load());
}

// Test concurrent insert operations
TEST_F(APVersionManagerTest, ConcurrentInserts) {
  const uint32_t init_ts = 300;
  const int num_threads = 8;
  const int operations_per_thread = 50;

  vm_->init_ts(init_ts, num_threads);

  std::vector<std::thread> threads;
  std::atomic<int> successful_operations{0};

  // Launch multiple threads performing insert operations
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < operations_per_thread; ++j) {
        uint32_t ts = vm_->acquire_insert_timestamp();
        EXPECT_EQ(ts, init_ts);

        // Simulate some work
        std::this_thread::sleep_for(std::chrono::microseconds(1));

        vm_->release_insert_timestamp(ts);
        successful_operations.fetch_add(1);
      }
    });
  }

  // Wait for all threads to complete
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(successful_operations.load(), num_threads * operations_per_thread);
}

// Test mixed concurrent read and insert operations
TEST_F(APVersionManagerTest, ConcurrentReadsAndInserts) {
  const uint32_t init_ts = 400;
  const int num_read_threads = 5;
  const int num_insert_threads = 5;
  const int operations_per_thread = 50;

  vm_->init_ts(init_ts, num_read_threads + num_insert_threads);

  std::vector<std::thread> threads;
  std::atomic<int> successful_reads{0};
  std::atomic<int> successful_inserts{0};

  // Launch read threads
  for (int i = 0; i < num_read_threads; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < operations_per_thread; ++j) {
        uint32_t ts = vm_->acquire_read_timestamp();
        EXPECT_EQ(ts, init_ts);
        std::this_thread::sleep_for(std::chrono::microseconds(1));
        vm_->release_read_timestamp();
        successful_reads.fetch_add(1);
      }
    });
  }

  // Launch insert threads
  for (int i = 0; i < num_insert_threads; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < operations_per_thread; ++j) {
        uint32_t ts = vm_->acquire_insert_timestamp();
        EXPECT_EQ(ts, init_ts);
        std::this_thread::sleep_for(std::chrono::microseconds(1));
        vm_->release_insert_timestamp(ts);
        successful_inserts.fetch_add(1);
      }
    });
  }

  // Wait for all threads to complete
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(successful_reads.load(), num_read_threads * operations_per_thread);
  EXPECT_EQ(successful_inserts.load(),
            num_insert_threads * operations_per_thread);
}

// Test update blocks read operations
TEST_F(APVersionManagerTest, UpdateBlocksReads) {
  const uint32_t init_ts = 500;
  vm_->init_ts(init_ts, 4);

  std::atomic<bool> update_acquired{false};
  std::atomic<bool> read_blocked{true};
  std::atomic<bool> read_completed{false};

  // Thread that acquires update timestamp
  std::thread update_thread([&]() {
    uint32_t ts = vm_->acquire_update_timestamp();
    update_acquired.store(true);

    // Hold update for a while
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    vm_->release_update_timestamp(ts);
  });

  // Thread that tries to acquire read timestamp (should be blocked)
  std::thread read_thread([&]() {
    // Wait a bit to ensure update thread starts first
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // This should block until update is released
    uint32_t ts = vm_->acquire_read_timestamp();
    EXPECT_EQ(ts, init_ts);
    read_blocked.store(false);
    vm_->release_read_timestamp();
    read_completed.store(true);
  });

  update_thread.join();
  read_thread.join();

  EXPECT_TRUE(update_acquired.load());
  EXPECT_FALSE(read_blocked.load());
  EXPECT_TRUE(read_completed.load());
}

// Test update blocks insert operations
TEST_F(APVersionManagerTest, UpdateBlocksInserts) {
  const uint32_t init_ts = 600;
  vm_->init_ts(init_ts, 4);

  std::atomic<bool> update_acquired{false};
  std::atomic<bool> insert_blocked{true};
  std::atomic<bool> insert_completed{false};

  // Thread that acquires update timestamp
  std::thread update_thread([&]() {
    uint32_t ts = vm_->acquire_update_timestamp();
    update_acquired.store(true);

    // Hold update for a while
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    vm_->release_update_timestamp(ts);
  });

  // Thread that tries to acquire insert timestamp (should be blocked)
  std::thread insert_thread([&]() {
    // Wait a bit to ensure update thread starts first
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // This should block until update is released
    uint32_t ts = vm_->acquire_insert_timestamp();
    insert_blocked.store(false);
    vm_->release_insert_timestamp(ts);
    insert_completed.store(true);
  });

  update_thread.join();
  insert_thread.join();

  EXPECT_TRUE(update_acquired.load());
  EXPECT_FALSE(insert_blocked.load());
  EXPECT_TRUE(insert_completed.load());
}

// Test update waits for active reads/inserts to complete
TEST_F(APVersionManagerTest, UpdateWaitsForActiveOperations) {
  const uint32_t init_ts = 700;
  vm_->init_ts(init_ts, 4);

  std::atomic<bool> read_active{false};
  std::atomic<bool> update_acquired{false};
  std::atomic<int> execution_order{0};

  // Thread that holds read timestamp
  std::thread read_thread([&]() {
    uint32_t ts = vm_->acquire_read_timestamp();
    EXPECT_EQ(ts, init_ts);
    read_active.store(true);

    // Hold read for a while
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    vm_->release_read_timestamp();
    if (execution_order.load() == 0) {
      execution_order.store(1);  // Read finished first
    }
  });

  // Thread that tries to acquire update timestamp (should wait)
  std::thread update_thread([&]() {
    // Wait a bit to ensure read thread starts first
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // This should wait until read is released
    uint32_t ts = vm_->acquire_update_timestamp();
    update_acquired.store(true);
    if (execution_order.load() == 1) {
      execution_order.store(2);  // Update acquired after read finished
    }
    vm_->release_update_timestamp(ts);
  });

  read_thread.join();
  update_thread.join();

  EXPECT_TRUE(read_active.load());
  EXPECT_TRUE(update_acquired.load());
  EXPECT_EQ(execution_order.load(), 2);  // Correct order: read->update
}

// Test stress scenario with mixed operations
TEST_F(APVersionManagerTest, StressTestMixedOperations) {
  const uint32_t init_ts = 800;
  const int num_iterations = 20;

  vm_->init_ts(init_ts, 8);

  std::atomic<int> total_operations{0};
  std::atomic<bool> error_occurred{false};

  for (int iteration = 0; iteration < num_iterations; ++iteration) {
    std::vector<std::thread> threads;

    // Mix of read, insert, and update operations
    threads.emplace_back([&]() {
      try {
        for (int i = 0; i < 10; ++i) {
          uint32_t ts = vm_->acquire_read_timestamp();
          if (ts != init_ts)
            error_occurred.store(true);
          std::this_thread::sleep_for(std::chrono::microseconds(10));
          vm_->release_read_timestamp();
          total_operations.fetch_add(1);
        }
      } catch (...) { error_occurred.store(true); }
    });

    threads.emplace_back([&]() {
      try {
        for (int i = 0; i < 5; ++i) {
          uint32_t ts = vm_->acquire_insert_timestamp();
          if (ts != init_ts)
            error_occurred.store(true);
          std::this_thread::sleep_for(std::chrono::microseconds(20));
          vm_->release_insert_timestamp(ts);
          total_operations.fetch_add(1);
        }
      } catch (...) { error_occurred.store(true); }
    });

    threads.emplace_back([&]() {
      try {
        uint32_t ts = vm_->acquire_update_timestamp();
        if (ts != init_ts)
          error_occurred.store(true);
        std::this_thread::sleep_for(std::chrono::microseconds(30));
        vm_->release_update_timestamp(ts);
        total_operations.fetch_add(1);
      } catch (...) { error_occurred.store(true); }
    });

    for (auto& thread : threads) {
      thread.join();
    }
  }

  EXPECT_FALSE(error_occurred.load());
  EXPECT_EQ(total_operations.load(), num_iterations * (10 + 5 + 1));
}

}  // namespace test
}  // namespace neug