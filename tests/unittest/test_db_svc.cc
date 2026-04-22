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

#include <brpc/controller.h>
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
    std::string db_path = (test_dir_ / "graph").string();
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

TEST_F(NeugDBServiceTest, GetServiceConfig) {
  neug::NeugDBService service(*db_, config_);
  EXPECT_FALSE(service.IsRunning());

  auto status = service.service_status();
  EXPECT_TRUE(status);
  EXPECT_EQ(status.value(), "NeugDB service has not been started!");

  auto retrieved_config = service.GetServiceConfig();
  EXPECT_EQ(retrieved_config.query_port, config_.query_port);
  EXPECT_EQ(retrieved_config.host_str, config_.host_str);
}

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

TEST_F(NeugDBServiceTest, NotRunningBeforeStart) {
  neug::ServiceConfig cfg;
  cfg.query_port = 0;
  cfg.host_str = "127.0.0.1";
  neug::NeugDBService service(*db_, cfg);

  EXPECT_FALSE(service.IsRunning());
  auto status = service.service_status();
  ASSERT_TRUE(status);
  EXPECT_EQ(status.value(), "NeugDB service has not been started!");
}

TEST_F(NeugDBServiceTest, StartSetsRunningTrue) {
  neug::ServiceConfig cfg;
  cfg.query_port = 0;
  cfg.host_str = "127.0.0.1";
  neug::NeugDBService service(*db_, cfg);

  service.Start();

  EXPECT_TRUE(service.IsRunning());
  auto status = service.service_status();
  ASSERT_TRUE(status);
  EXPECT_EQ(status.value(), "NeugDB service is running ...");

  service.Stop();
}

TEST_F(NeugDBServiceTest, StopClearsRunningFlag) {
  neug::ServiceConfig cfg;
  cfg.query_port = 0;
  cfg.host_str = "127.0.0.1";
  neug::NeugDBService service(*db_, cfg);

  service.Start();
  ASSERT_TRUE(service.IsRunning());

  service.Stop();

  EXPECT_FALSE(service.IsRunning());
  auto status = service.service_status();
  ASSERT_TRUE(status);
  EXPECT_EQ(status.value(), "NeugDB service has not been started!");
}

TEST_F(NeugDBServiceTest, StartThrowsWhenAlreadyRunning) {
  neug::ServiceConfig cfg;
  cfg.query_port = 0;
  cfg.host_str = "127.0.0.1";
  neug::NeugDBService service(*db_, cfg);

  service.Start();
  ASSERT_TRUE(service.IsRunning());

  // Second Start() must throw; running_ must remain true.
  EXPECT_THROW(service.Start(), neug::exception::RuntimeError);
  EXPECT_TRUE(service.IsRunning());
  EXPECT_EQ(service.service_status().value(), "NeugDB service is running ...");

  service.Stop();
}

TEST_F(NeugDBServiceTest, RunAndWaitForExitSetsAndClearsRunning) {
  neug::ServiceConfig cfg;
  cfg.query_port = 0;
  cfg.host_str = "127.0.0.1";
  neug::NeugDBService service(*db_, cfg);

  ASSERT_FALSE(service.IsRunning());

  // run_and_wait_for_exit() blocks; run it on a background thread.
  std::thread svc_thread([&]() { service.run_and_wait_for_exit(); });

  // Spin-wait until running_ flips to true (set synchronously before
  // RunUntilAskedToQuit() blocks).
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!service.IsRunning() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_TRUE(service.IsRunning())
      << "Service did not become running within 5 s";
  EXPECT_EQ(service.service_status().value(), "NeugDB service is running ...");

  // Signal the brpc server to quit directly – without going through
  // service.Stop() – so that running_ is cleared exclusively by
  // run_and_wait_for_exit() itself (the code path this test exercises).
  brpc::AskToQuit();
  svc_thread.join();

  EXPECT_FALSE(service.IsRunning());
  EXPECT_EQ(service.service_status().value(),
            "NeugDB service has not been started!");
}

}  // namespace test
}  // namespace neug
