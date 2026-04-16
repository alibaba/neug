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
#include <atomic>
#include <chrono>
#include <filesystem>
#include <thread>
#include <vector>

#include "neug/main/neug_db.h"
#include "neug/server/neug_db_service.h"
#include "utils.h"

namespace neug {
namespace test {

class NeugDBServiceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create temporary directory for test database
    test_dir_ = std::filesystem::temp_directory_path() / "neug_test_db";
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
    std::filesystem::create_directories(test_dir_);

    // Create and open database
    std::string db_path = test_dir_ / "graph";
    db_ = std::make_unique<neug::NeugDB>();
    db_->Open(db_path, 4);  // 4 threads

    // Load modern graph
    auto conn = db_->Connect();
    load_modern_graph(conn);

    // Configure service
    config_.query_port = 19999;  // Use non-standard port to avoid conflicts
    config_.host_str = "127.0.0.1";
  }

  void TearDown() override {
    if (db_ && !db_->IsClosed()) {
      db_->Close();
    }
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }

  std::unique_ptr<neug::NeugDB> db_;
  neug::ServiceConfig config_;
  std::filesystem::path test_dir_;
};

// Test 1: Service initialization
TEST_F(NeugDBServiceTest, ServiceInitialization) {
  neug::NeugDBService service(*db_, config_);

  EXPECT_FALSE(service.IsRunning());

  auto status = service.service_status();
  EXPECT_TRUE(status);
  EXPECT_EQ(status.value(), "NeugDB service has not been started!");
}

// Test 3: Session acquisition
TEST_F(NeugDBServiceTest, AcquireSession) {
  neug::NeugDBService service(*db_, config_);

  // Acquire a session
  auto guard = service.AcquireSession();
  EXPECT_TRUE(guard);
}

// Test 4: Multiple concurrent sessions
TEST_F(NeugDBServiceTest, ConcurrentSessions) {
  neug::NeugDBService service(*db_, config_);
  const int num_threads = 4;
  std::vector<std::thread> threads;
  std::atomic<int> success_count(0);

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&]() {
      try {
        auto guard = service.AcquireSession();
        if (guard) {
          success_count++;
        }
      } catch (const std::exception& e) {
        GTEST_LOG_(ERROR) << "Thread exception: " << e.what();
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(success_count, num_threads);
}

// Test 5: Service configuration retrieval
TEST_F(NeugDBServiceTest, GetServiceConfig) {
  neug::NeugDBService service(*db_, config_);

  auto retrieved_config = service.GetServiceConfig();
  EXPECT_EQ(retrieved_config.query_port, config_.query_port);
  EXPECT_EQ(retrieved_config.host_str, config_.host_str);
}

// Test 6: Session pool size
TEST_F(NeugDBServiceTest, SessionPoolSize) {
  neug::NeugDBService service(*db_, config_);

  size_t pool_size = service.SessionNum();
  EXPECT_GT(pool_size, 0);
}

// Test 7: Service status
TEST_F(NeugDBServiceTest, ServiceStatus) {
  neug::NeugDBService service(*db_, config_);

  // Not initialized
  auto status = service.service_status();
  EXPECT_TRUE(status);
  auto str = status.value();
  EXPECT_EQ(str, "NeugDB service has not been started!");
}

// Test 8: Executed query count
TEST_F(NeugDBServiceTest, ExecutedQueryCount) {
  neug::NeugDBService service(*db_, config_);

  size_t query_count = service.getExecutedQueryNum();
  EXPECT_GE(query_count, 0);
}

// Test 9: Rapid session acquisition/release
TEST_F(NeugDBServiceTest, RapidSessionAcquisition) {
  neug::NeugDBService service(*db_, config_);
  const int num_iterations = 100;

  for (int i = 0; i < num_iterations; ++i) {
    auto guard = service.AcquireSession();
    EXPECT_TRUE(guard);
  }
}

// Test 10: Multi-threaded stress test
TEST_F(NeugDBServiceTest, MultiThreadedStress) {
  neug::NeugDBService service(*db_, config_);
  const int num_threads = 8;
  const int iterations_per_thread = 50;
  std::vector<std::thread> threads;
  std::atomic<int> total_operations(0);
  std::atomic<int> errors(0);

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&]() {
      for (int i = 0; i < iterations_per_thread; ++i) {
        try {
          auto guard = service.AcquireSession();
          if (guard) {
            total_operations++;
          } else {
            errors++;
          }
        } catch (const std::exception& e) { errors++; }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(total_operations, num_threads * iterations_per_thread);
  EXPECT_EQ(errors, 0);
}

// Test 11: Service state consistency
TEST_F(NeugDBServiceTest, StateConsistency) {
  neug::NeugDBService service(*db_, config_);

  EXPECT_FALSE(service.IsRunning());

  // State should not change unexpectedly
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_FALSE(service.IsRunning());
}

// Test 12: Session guard cleanup
TEST_F(NeugDBServiceTest, SessionGuardCleanup) {
  neug::NeugDBService service(*db_, config_);

  {
    auto guard = service.AcquireSession();
    EXPECT_TRUE(guard);
    // guard goes out of scope, should release session
  }

  // Should be able to acquire another session
  auto guard2 = service.AcquireSession();
  EXPECT_TRUE(guard2);
}

// Test 13: Concurrent session operations
TEST_F(NeugDBServiceTest, ConcurrentSessionOperations) {
  neug::NeugDBService service(*db_, config_);
  const int num_threads = 4;
  const int session_count = 25;
  std::vector<std::thread> threads;
  std::atomic<int> total_sessions(0);

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&]() {
      for (int s = 0; s < session_count; ++s) {
        auto guard = service.AcquireSession();
        if (guard) {
          total_sessions++;
          // Simulate some work
          std::this_thread::yield();
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(total_sessions, num_threads * session_count);
}

}  // namespace test
}  // namespace neug
