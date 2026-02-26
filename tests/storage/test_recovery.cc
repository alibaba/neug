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
#include "httplib.h"
#include "neug/main/neug_db.h"
#include "neug/server/neug_db_service.h"
#include "neug/utils/service_utils.h"

#include <sys/types.h>
#include <unistd.h>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <thread>
#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

using namespace neug;

class NeugDBWALRecoveryTest : public ::testing::TestWithParam<bool> {
 protected:
  std::string data_dir_;
  std::string neugdb_host_ = "127.0.0.1";
  int neugdb_port_ = 7071;
  std::unique_ptr<neug::NeugDB> db_;
  std::unique_ptr<neug::NeugDBService> service_;

  void SetUp() override {
    data_dir_ = "/tmp/neugdb_recovery_test_" + std::to_string(::getpid());
    std::filesystem::remove_all(data_dir_);
    std::filesystem::create_directories(data_dir_);
  }

  void TearDown() override {
    StopService();
    std::filesystem::remove_all(data_dir_);
  }

  void StartService() {
    db_ = std::make_unique<neug::NeugDB>();
    NeugDBConfig db_config(data_dir_, std::thread::hardware_concurrency());
    db_config.checkpoint_on_close = GetParam();
    db_config.mode = DBMode::READ_WRITE;
    db_config.compact_on_close = db_config.checkpoint_on_close;
    ASSERT_TRUE(db_->Open(db_config));
    neug::ServiceConfig config;
    config.host_str = neugdb_host_;
    config.query_port = neugdb_port_;
    service_ = std::make_unique<neug::NeugDBService>(*db_, config);
    service_->Start();
    // Wait for service to be ready
    for (int i = 0; i < 30; ++i) {
      httplib::Client cli(neugdb_host_, neugdb_port_);
      auto res = cli.Get("/service_status");
      if (res && res->status == 200)
        return;
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    FAIL() << "Failed to start neugdb service";
  }

  void StopService() {
    if (service_) {
      service_->Stop();
      service_.reset();
    }
    if (db_) {
      db_->Close();
      db_.reset();
    }
  }

  std::string PostCypher(const std::string& cypher) {
    httplib::Client cli(neugdb_host_, neugdb_port_);
    httplib::Headers headers = {{"Content-Type", "application/json"}};
    std::string body = std::string("{\"query\":\"") + cypher + "\"}";
    auto res = cli.Post("/cypher", headers, body, "application/json");
    EXPECT_TRUE(res);
    auto query_result = QueryResult::From(res->body);
    return query_result.ToString();
  }

  std::string data_dir() const { return data_dir_; }
};

INSTANTIATE_TEST_SUITE_P(CheckpointOnCloseValues, NeugDBWALRecoveryTest,
                         ::testing::Values(true, false));

TEST_P(NeugDBWALRecoveryTest, WALRecoveryViaCypher) {
  StartService();
  PostCypher(
      "CREATE NODE TABLE Person (id INT64, age INT64, PRIMARY KEY(id));");

  PostCypher("CREATE (:Person {id: 1, age: 42})");
  PostCypher("CREATE (:Person {id: 2, age: 43})");
  PostCypher("CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64);");
  PostCypher(
      "MATCH (a:Person {id: 1}), (b:Person {id: 2}) "
      "CREATE (a)-[:KNOWS {since: 2020}]->(b);");
  StopService();

  StartService();
  auto resp2 = PostCypher("MATCH (n:Person {id: 1}) RETURN n.age");
  ASSERT_FALSE(resp2.empty());
  EXPECT_NE(resp2.find("42"), std::string::npos)
      << "Failed to find age 42 in response: " + resp2;

  resp2 = PostCypher(
      "MATCH (a:Person {id: 1})-[e:KNOWS]->(b:Person {id: 2}) RETURN e.since");
  ASSERT_FALSE(resp2.empty());
  EXPECT_NE(resp2.find("2020"), std::string::npos)
      << "Failed to find since 2020 in response: " + resp2;
  StopService();

  // Then Open the DB in read-write mode
  neug::NeugDB db;
  neug::NeugDBConfig db_config(data_dir(), std::thread::hardware_concurrency());
  db_config.compact_on_close = true;
  db_config.checkpoint_on_close = true;
  ASSERT_TRUE(db.Open(data_dir(), 4, neug::DBMode::READ_WRITE));
  db.Close();

  StartService();

  resp2 = PostCypher("MATCH (n:Person {id: 1}) RETURN n.age");
  ASSERT_FALSE(resp2.empty());
  EXPECT_NE(resp2.find("42"), std::string::npos)
      << "Failed to find age 42 in response: " + resp2;

  resp2 = PostCypher(
      "MATCH (a:Person {id: 1})-[e:KNOWS]->(b:Person {id: 2}) RETURN e.since");
  ASSERT_FALSE(resp2.empty());
  EXPECT_NE(resp2.find("2020"), std::string::npos)
      << "Failed to find since 2020 in response: " + resp2;
  StopService();
}

class NeugDBWALRecoverySubprocessTest : public ::testing::Test {
 protected:
  std::string data_dir_;
  std::string log_file_;
  std::string neugdb_host_ = "127.0.0.1";
  std::string neugdb_bin_;
  int neugdb_port_ = 7072;  // Use a different port to avoid conflict
  pid_t neugdb_pid_ = -1;
  std::string neugdb_bin_suffix = "./bin/rt_server";  // Adjust path if needed

  void SetUp() override {
    data_dir_ = "/tmp/neugdb_recovery_subproc_test";
    log_file_ = data_dir_ + "/log.txt";
    std::filesystem::remove_all(data_dir_);
    std::filesystem::create_directories(data_dir_);
// Determine neugdb binary path
#ifdef __APPLE__
    // Get current binary path on macos
    char pathbuf[1024];
    uint32_t bufsize = sizeof(pathbuf);
    if (_NSGetExecutablePath(pathbuf, &bufsize) == 0) {
      neugdb_bin_ = std::string(pathbuf);
      LOG(INFO) << "Current executable path: " << neugdb_bin_;
      auto pos = neugdb_bin_.rfind('/');
      if (pos != std::string::npos) {
        neugdb_bin_ =
            neugdb_bin_.substr(0, pos + 1) + "/../../" + neugdb_bin_suffix;
      } else {
        neugdb_bin_ = "/../../" + neugdb_bin_suffix;
      }
    } else {
      throw std::runtime_error("Failed to get executable path");
    }
#else
    // Get current binary path on linux
    char buf[1024];
    ssize_t len = readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    if (len != -1) {
      buf[len] = '\0';
      neugdb_bin_ = std::string(buf);
      LOG(INFO) << "Current executable path: " << neugdb_bin_;
      auto pos = neugdb_bin_.rfind('/');
      if (pos != std::string::npos) {
        neugdb_bin_ =
            neugdb_bin_.substr(0, pos + 1) + "/../../" + neugdb_bin_suffix;
      } else {
        neugdb_bin_ = "/../../" + neugdb_bin_suffix;
      }
    } else {
      throw std::runtime_error("Failed to get executable path");
    }
#endif
    if (!std::filesystem::exists(neugdb_bin_)) {
      throw std::runtime_error("NeugDB binary not found at " + neugdb_bin_);
    }
    neugdb_bin_ = std::filesystem::absolute(neugdb_bin_).string();
    LOG(INFO) << "Using neugdb binary: " << neugdb_bin_;
  }

  void TearDown() override {
    StopService();
    // If fails, print log.txt
    if (std::filesystem::exists(log_file_)) {
      std::ifstream log_stream(log_file_);
      std::string line;
      std::cout << "=== NeugDB Log Start ===" << std::endl;
      while (std::getline(log_stream, line)) {
        std::cout << line << std::endl;
      }
      std::cout << "=== NeugDB Log End ===" << std::endl;
    }
    std::filesystem::remove_all(data_dir_);
  }

  void StartService() {
    auto lock_file = data_dir_ + "/neugdb.lock";
    if (std::filesystem::exists(lock_file)) {
      std::filesystem::remove(lock_file);
    }
    std::string cmd = neugdb_bin_ + " -d " + data_dir_ + " -p " +
                      std::to_string(neugdb_port_);
    neugdb_pid_ = fork();
    if (neugdb_pid_ == 0) {
      setsid();
      // Child process
      int fd = ::open(log_file_.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
      if (fd == -1) {
        std::perror("open log file failed");
        std::exit(1);
      }
      ::dup2(fd, STDOUT_FILENO);
      ::dup2(fd, STDERR_FILENO);
      ::close(fd);
      LOG(INFO) << "Starting neugdb subprocess with command: " << cmd;
      execl("/bin/sh", "sh", "-c", cmd.c_str(), (char*) nullptr);
      std::exit(127);  // exec failed
    }
    // Wait for service to be ready
    for (int i = 0; i < 30; ++i) {
      httplib::Client cli(neugdb_host_, neugdb_port_);
      auto res = cli.Get("/service_status");
      if (res && res->status == 200) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    FAIL() << "Failed to start neugdb subprocess";
  }

  void StopService() {
    if (neugdb_pid_ > 0) {
      kill(neugdb_pid_, SIGKILL);
      int status = 0;
      waitpid(neugdb_pid_, &status, 0);
      // check if process is really terminated
      int32_t max_checks = 50;  // wait up to 5 seconds
      while (max_checks-- > 0) {
        if (kill(neugdb_pid_, 0) == -1 && errno == ESRCH) {
          break;  // Process has terminated
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
      if (max_checks <= 0) {
        FAIL() << "Failed to terminate neugdb subprocess, errno: " << errno;
      }
      LOG(INFO) << "NeugDB subprocess terminated successfully";
      neugdb_pid_ = -1;
    } else {
      LOG(INFO) << "NeugDB subprocess not running";
    }
  }

  std::string PostCypher(const std::string& cypher) {
    httplib::Client cli(neugdb_host_, neugdb_port_);
    httplib::Headers headers = {{"Content-Type", "application/json"}};
    std::string body = std::string("{\"query\":\"") + cypher + "\"}";
    auto res = cli.Post("/cypher", headers, body, "application/json");
    if (!res)
      return "";
    auto query_result = QueryResult::From(res->body);
    return query_result.ToString();
  }
};

TEST_F(NeugDBWALRecoverySubprocessTest, SimulateCrashAndRecoverFromWAL) {
  StartService();
  PostCypher(
      "CREATE NODE TABLE Person (id INT64, age INT64, PRIMARY KEY(id));");
  PostCypher("CREATE (:Person {id: 1, age: 42})");
  PostCypher("CREATE (:Person {id: 2, age: 43})");
  auto resp = PostCypher("MATCH (n:Person {id: 1}) RETURN n.age");
  ASSERT_FALSE(resp.empty());
  EXPECT_NE(resp.find("42"), std::string::npos)
      << "Failed to find age 42 in response: " + resp;
  // Simulate crash (kill the process without graceful shutdown)
  StopService();

  StartService();
  resp = PostCypher("MATCH (n:Person {id: 1}) RETURN n.age");
  ASSERT_FALSE(resp.empty());
  EXPECT_NE(resp.find("42"), std::string::npos)
      << "Failed to find age 42 in response: " + resp;
  StopService();
}
