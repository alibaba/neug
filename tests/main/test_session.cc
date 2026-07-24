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

#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "neug/main/connection.h"
#include "neug/main/neug_db.h"
#include "neug/main/session.h"
#include "neug/storages/allocators.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/dummy_wal_writer.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace test {

TEST(SessionCoreTest, ExecutesWithoutServerRuntime) {
  const auto db_dir = std::filesystem::temp_directory_path() /
                      ("neug_core_session_test_" + std::to_string(::getpid()));
  std::filesystem::remove_all(db_dir);

  NeugDBConfig config(db_dir.string(), 1);
  config.checkpoint_on_close = false;
  config.memory_level = MemoryLevel::kInMemory;

  NeugDB db;
  ASSERT_TRUE(db.Open(config));
  {
    auto connection = db.Connect();
    auto create_table = connection->Query(
        "CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));", "schema");
    ASSERT_TRUE(create_table) << create_table.error().ToString();
  }

  VersionManager version_manager;
  version_manager.init_ts(0, 1);
  Allocator allocator(MemoryLevel::kInMemory, "");
  DummyWalWriter wal_writer;
  wal_writer.open();

  {
    Session session(db.graph_snapshot_store(), db.GetPlanner(),
                    db.GetQueryCache(), version_manager, allocator, wal_writer,
                    db.config(), 0);
    auto result = session.Eval(
        R"({"query":"MATCH (n:person) RETURN count(n);","access_mode":"read","parameters":{}})");
    ASSERT_TRUE(result) << result.error().ToString();
    EXPECT_EQ(session.SessionId(), 0);

    std::atomic<bool> resumed_on_another_thread{false};
    std::thread resumed_worker([&]() {
      resumed_on_another_thread.store(
          session
              .Eval(
                  R"({"query":"MATCH (n:person) RETURN count(n);","access_mode":"read","parameters":{}})")
              .has_value());
    });
    resumed_worker.join();
    EXPECT_TRUE(resumed_on_another_thread.load());
  }

  wal_writer.close();
  db.Close();
  std::filesystem::remove_all(db_dir);
}

TEST(NeugDBLifecycleTest, SupportsRepeatedCloseAndReopen) {
  const auto db_dir =
      std::filesystem::temp_directory_path() /
      ("neug_reopen_lifecycle_test_" + std::to_string(::getpid()));
  std::filesystem::remove_all(db_dir);

  NeugDBConfig config(db_dir.string(), 1);
  config.checkpoint_on_close = false;

  NeugDB db;
  ASSERT_TRUE(db.Open(config));
  EXPECT_THROW(db.Open(config), exception::RuntimeError);
  db.Close();
  EXPECT_NO_THROW(db.Close());
  ASSERT_TRUE(db.Open(config));
  db.Close();

  std::filesystem::remove_all(db_dir);
}

TEST(NeugDBLifecycleTest, FailedOpenCanBeRetried) {
  const auto db_dir =
      std::filesystem::temp_directory_path() /
      ("neug_failed_open_retry_test_" + std::to_string(::getpid()));
  std::filesystem::remove_all(db_dir);

  NeugDBConfig invalid_config(db_dir.string(), 1);
  invalid_config.checkpoint_on_close = false;
  invalid_config.planner_kind = "invalid";

  NeugDB db;
  EXPECT_THROW(db.Open(invalid_config), exception::InvalidArgumentException);

  auto valid_config = invalid_config;
  valid_config.planner_kind = "gopt";
  ASSERT_TRUE(db.Open(valid_config));
  db.Close();

  std::filesystem::remove_all(db_dir);
}

TEST(NeugDBLifecycleTest, FailedInMemoryOpenRemovesWorkspaceImmediately) {
  NeugDBConfig invalid_config(":memory:", 1);
  invalid_config.checkpoint_on_close = false;
  invalid_config.planner_kind = "invalid";

  NeugDB db;
  for (int attempt = 0; attempt < 2; ++attempt) {
    EXPECT_THROW(db.Open(invalid_config), exception::InvalidArgumentException);
    EXPECT_FALSE(std::filesystem::exists(db.config().data_dir));
  }

  NeugDBConfig valid_config(":memory:", 1);
  valid_config.checkpoint_on_close = false;
  ASSERT_TRUE(db.Open(valid_config));
  EXPECT_TRUE(std::filesystem::is_directory(db.config().data_dir));
  db.Close();
}

TEST(NeugDBLifecycleTest, InMemoryWorkspaceIsRemovedOnClose) {
  NeugDBConfig config(":memory:", 1);
  config.checkpoint_on_close = false;

  NeugDB db;
  for (int attempt = 0; attempt < 2; ++attempt) {
    ASSERT_TRUE(db.Open(config));
    const auto workspace = std::filesystem::path(db.config().data_dir);
    ASSERT_TRUE(std::filesystem::is_directory(workspace));
    db.Close();
    EXPECT_FALSE(std::filesystem::exists(workspace));
  }
}

TEST(NeugDBLifecycleTest, ClosedInMemoryPathCanBeReusedByAnotherDatabase) {
  std::filesystem::path workspace;
  NeugDB reopened_db;
  {
    NeugDBConfig memory_config(":memory:", 1);
    memory_config.checkpoint_on_close = false;

    NeugDB memory_db;
    ASSERT_TRUE(memory_db.Open(memory_config));
    workspace = memory_db.config().data_dir;
    memory_db.Close();
    ASSERT_FALSE(std::filesystem::exists(workspace));

    NeugDBConfig reopened_config(workspace.string(), 1);
    reopened_config.checkpoint_on_close = false;
    ASSERT_TRUE(reopened_db.Open(reopened_config));
    ASSERT_TRUE(std::filesystem::is_directory(workspace));
  }

  EXPECT_TRUE(std::filesystem::is_directory(workspace));
  reopened_db.Close();
  std::filesystem::remove_all(workspace);
}

TEST(NeugDBLifecycleTest, ConcurrentInMemoryDatabasesUseDistinctWorkspaces) {
  NeugDBConfig config(":memory:", 1);
  config.checkpoint_on_close = false;

  NeugDB first_db;
  NeugDB second_db;
  ASSERT_TRUE(first_db.Open(config));
  ASSERT_TRUE(second_db.Open(config));

  const auto first_workspace =
      std::filesystem::path(first_db.config().data_dir);
  const auto second_workspace =
      std::filesystem::path(second_db.config().data_dir);
  EXPECT_NE(first_workspace, second_workspace);
  EXPECT_TRUE(std::filesystem::is_directory(first_workspace));
  EXPECT_TRUE(std::filesystem::is_directory(second_workspace));

  first_db.Close();
  EXPECT_FALSE(std::filesystem::exists(first_workspace));
  EXPECT_TRUE(std::filesystem::is_directory(second_workspace));

  second_db.Close();
  EXPECT_FALSE(std::filesystem::exists(second_workspace));
}

}  // namespace test
}  // namespace neug
