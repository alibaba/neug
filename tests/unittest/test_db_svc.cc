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
#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <brpc/channel.h>
#include <brpc/controller.h>
#include "bthread/bthread.h"
#include "neug/common/types/value.h"
#include "neug/generated/proto/response/response.pb.h"
#include "neug/main/connection_manager.h"
#include "neug/main/neug_db.h"
#include "neug/main/query_request.h"
#include "neug/server/neug_db_service.h"
#include "neug/storages/graph/graph_interface.h"
#include "utils.h"

namespace neug {

namespace test {

namespace {

constexpr auto kBthreadTestTimeout = std::chrono::seconds(10);

uint64_t ReadPlanningGeneration(GraphSnapshotStore& store) {
  SnapshotGuard current(store);
  return current.get().planning_generation();
}

bool WaitForFlag(const std::atomic<bool>& flag) {
  const auto deadline = std::chrono::steady_clock::now() + kBthreadTestTimeout;
  while (!flag.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return flag.load(std::memory_order_acquire);
}

using BthreadTask = std::function<void()>;

void* RunBthreadTask(void* arg) {
  (*static_cast<BthreadTask*>(arg))();
  return nullptr;
}

int StartBthread(bthread_t& tid, BthreadTask& task) {
  return bthread_start_background(&tid, &BTHREAD_ATTR_NORMAL, RunBthreadTask,
                                  &task);
}

}  // namespace

timestamp_t InsertModernPersonAndReturnTimestamp(NeugDBService& service,
                                                 int64_t id) {
  auto slot = service.AcquireExecutionSlot();
  auto transaction = slot->GetInsertTransaction();
  const auto timestamp = transaction.timestamp();
  StorageTPInsertInterface graph(transaction);
  const auto person_label = transaction.schema().get_vertex_label_id("person");
  vid_t vid = 0;
  EXPECT_TRUE(graph.AddVertex(
      person_label, Value::INT64(id),
      {Value::STRING("session-" + std::to_string(id)), Value::INT64(30)}, vid));
  EXPECT_TRUE(transaction.Commit());
  return timestamp;
}

using WalPrefixSnapshot = std::vector<std::pair<std::string, std::string>>;

WalPrefixSnapshot readWalPrefixes(const std::filesystem::path& wal_dir) {
  std::vector<std::filesystem::path> paths;
  for (const auto& entry : std::filesystem::directory_iterator(wal_dir)) {
    if (entry.is_regular_file() && entry.path().extension() == ".wal") {
      paths.emplace_back(entry.path());
    }
  }
  std::sort(paths.begin(), paths.end());

  constexpr std::streamsize kPrefixSize = 4096;
  WalPrefixSnapshot snapshot;
  for (const auto& path : paths) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
      throw std::runtime_error("Failed to read WAL: " + path.string());
    }
    std::string prefix(kPrefixSize, '\0');
    input.read(prefix.data(), kPrefixSize);
    prefix.resize(static_cast<size_t>(input.gcount()));
    snapshot.emplace_back(path.filename().string(), std::move(prefix));
  }
  return snapshot;
}

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
    conn->Close();

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

TEST_F(NeugDBServiceTest, ConcurrentExecutionSlots) {
  neug::NeugDBService service(*db_, config_);
  const int num_threads = 4;
  std::vector<std::thread> threads;
  std::atomic<int> success_count(0);

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&]() {
      try {
        auto lease = service.AcquireExecutionSlot();
        if (lease) {
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

TEST_F(NeugDBServiceTest, ExecutionSlotLeaseMoveTransfersSingleLease) {
  neug::NeugDBService service(*db_, config_);

  auto original = service.AcquireExecutionSlot();
  ASSERT_TRUE(original);
  auto* slot = original.get();

  ExecutionSlotLease moved(std::move(original));
  EXPECT_FALSE(original);
  EXPECT_EQ(moved.get(), slot);

  ExecutionSlotLease assigned;
  assigned = std::move(moved);
  EXPECT_FALSE(moved);
  EXPECT_EQ(assigned.get(), slot);
}

TEST_F(NeugDBServiceTest,
       SingleExecutionSlotLeaseSurvivesBthreadWaitsExclusively) {
  const auto single_slot_path = (test_dir_ / "single_slot_graph").string();
  NeugDB single_slot_db;
  single_slot_db.Open(single_slot_path, 1);
  auto connection = single_slot_db.Connect();
  load_modern_graph(connection);
  connection->Close();

  ServiceConfig config;
  config.query_port = 0;
  config.host_str = "127.0.0.1";
  {
    NeugDBService service(single_slot_db, config);
    ASSERT_EQ(service.ExecutionSlotNum(), 1U);

    std::atomic<bool> first_acquired{false};
    std::atomic<bool> second_acquired{false};
    std::atomic<bool> release_first{false};
    bool identity_stable = true;
    bool stack_stable = true;
    bool guard_stable = true;
    bool transaction_stable = true;
    bool pthread_migrated = false;
    int first_slot_id = -1;
    int second_slot_id = -1;

    BthreadTask first_task = [&]() {
      auto guard = service.AcquireExecutionSlot();
      auto transaction = guard->GetReadTransaction();
      const auto logical_thread = bthread_self();
      const auto physical_thread = std::this_thread::get_id();
      int stack_marker = 0;
      const void* stack_address = &stack_marker;
      const void* guard_address = &guard;
      const void* transaction_address = &transaction;
      first_slot_id = guard->SlotId();
      first_acquired.store(true, std::memory_order_release);

      while (!release_first.load(std::memory_order_acquire)) {
        (void) bthread_yield();
        (void) bthread_usleep(50);
        identity_stable &= bthread_equal(logical_thread, bthread_self()) != 0;
        stack_stable &= stack_address == &stack_marker;
        guard_stable &= guard_address == &guard;
        transaction_stable &= transaction_address == &transaction;
        pthread_migrated |= physical_thread != std::this_thread::get_id();
      }
      transaction.Commit();
    };
    bthread_t first;
    ASSERT_EQ(StartBthread(first, first_task), 0);
    if (!WaitForFlag(first_acquired)) {
      release_first.store(true, std::memory_order_release);
      EXPECT_EQ(bthread_join(first, nullptr), 0);
      FAIL() << "First bthread did not acquire the only execution slot";
    }

    BthreadTask second_task = [&]() {
      auto guard = service.AcquireExecutionSlot();
      second_slot_id = guard->SlotId();
      second_acquired.store(true, std::memory_order_release);
    };
    bthread_t second;
    const int second_start_result = StartBthread(second, second_task);
    if (second_start_result != 0) {
      release_first.store(true, std::memory_order_release);
      EXPECT_EQ(bthread_join(first, nullptr), 0);
      FAIL() << "Failed to start second bthread";
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    const bool acquired_while_held = second_acquired.load();

    release_first.store(true, std::memory_order_release);
    ASSERT_EQ(bthread_join(first, nullptr), 0);
    ASSERT_EQ(bthread_join(second, nullptr), 0);

    EXPECT_FALSE(acquired_while_held);
    EXPECT_TRUE(second_acquired.load());
    EXPECT_EQ(first_slot_id, 0);
    EXPECT_EQ(second_slot_id, 0);
    EXPECT_TRUE(identity_stable);
    EXPECT_TRUE(stack_stable);
    EXPECT_TRUE(guard_stable);
    EXPECT_TRUE(transaction_stable);
    SUCCEED() << "pthread migration observed: " << pthread_migrated;
  }

  single_slot_db.Close();
}

TEST_F(NeugDBServiceTest, ExecutionSlotsRemainExclusiveUnderBthreadStress) {
  NeugDBService service(*db_, config_);
  constexpr int kTaskCount = 32;
  constexpr int kIterations = 64;
  std::vector<std::atomic<int>> owners(service.ExecutionSlotNum());
  for (auto& owner : owners) {
    owner.store(0, std::memory_order_relaxed);
  }
  std::atomic<int> violations{0};
  std::vector<BthreadTask> tasks;
  std::vector<bthread_t> tids(kTaskCount);
  tasks.reserve(kTaskCount);
  int started_tasks = 0;
  for (; started_tasks < kTaskCount; ++started_tasks) {
    const int task_id = started_tasks + 1;
    tasks.emplace_back([&, task_id]() {
      for (int i = 0; i < kIterations; ++i) {
        auto guard = service.AcquireExecutionSlot();
        auto& owner = owners.at(guard->SlotId());
        int expected = 0;
        if (!owner.compare_exchange_strong(expected, task_id)) {
          violations.fetch_add(1);
        }

        auto transaction = guard->GetReadTransaction();
        const auto logical_thread = bthread_self();
        (void) bthread_yield();
        (void) bthread_usleep(50);
        if (owner.load() != task_id ||
            bthread_equal(logical_thread, bthread_self()) == 0) {
          violations.fetch_add(1);
        }
        transaction.Commit();

        expected = task_id;
        if (!owner.compare_exchange_strong(expected, 0)) {
          violations.fetch_add(1);
        }
      }
    });
    if (StartBthread(tids[started_tasks], tasks.back()) != 0) {
      tasks.pop_back();
      break;
    }
  }
  for (int i = 0; i < started_tasks; ++i) {
    EXPECT_EQ(bthread_join(tids[i], nullptr), 0);
  }

  EXPECT_EQ(started_tasks, kTaskCount);
  EXPECT_EQ(violations.load(std::memory_order_relaxed), 0);
  for (const auto& owner : owners) {
    EXPECT_EQ(owner.load(std::memory_order_relaxed), 0);
  }
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
  EXPECT_EQ(retrieved_config.thread_num, config_.thread_num);
  EXPECT_EQ(retrieved_config.auto_compaction, config_.auto_compaction);
}

TEST_F(NeugDBServiceTest, DefaultServiceThreadsFollowDatabaseMaxThreadNum) {
  neug::ServiceConfig cfg;
  cfg.query_port = 0;
  cfg.host_str = "127.0.0.1";
  ASSERT_EQ(cfg.thread_num, 0U);

  neug::NeugDBService service(*db_, cfg);

  EXPECT_EQ(service.GetServiceConfig().thread_num, 0U);
  EXPECT_EQ(service.ExecutionSlotNum(),
            static_cast<size_t>(db_->config().max_thread_num));
}

TEST_F(NeugDBServiceTest, AutoDatabaseMaxThreadNumFeedsServiceDefaults) {
  const auto db_path = (test_dir_ / "auto_thread_graph").string();
  neug::NeugDB db;
  neug::NeugDBConfig db_cfg(db_path, 0);
  db.Open(db_cfg);

  auto expected_thread_num = std::thread::hardware_concurrency();
  if (expected_thread_num == 0) {
    expected_thread_num = 1;
  }
  EXPECT_EQ(db.config().max_thread_num, static_cast<int>(expected_thread_num));

  neug::ServiceConfig service_cfg;
  service_cfg.query_port = 0;
  service_cfg.host_str = "127.0.0.1";

  {
    neug::NeugDBService service(db, service_cfg);
    EXPECT_EQ(service.GetServiceConfig().thread_num, 0U);
    EXPECT_EQ(service.ExecutionSlotNum(),
              static_cast<size_t>(db.config().max_thread_num));
  }
  db.Close();
}

TEST_F(NeugDBServiceTest, ServiceThreadNumCannotExceedDatabaseMaxThreadNum) {
  neug::ServiceConfig cfg;
  cfg.query_port = 0;
  cfg.host_str = "127.0.0.1";
  cfg.thread_num = static_cast<uint32_t>(db_->config().max_thread_num + 1);

  EXPECT_THROW(neug::NeugDBService service(*db_, cfg),
               neug::exception::InvalidArgumentException);
}

TEST_F(NeugDBServiceTest, ConcurrentExecutionSlotOperations) {
  neug::NeugDBService service(*db_, config_);
  const int num_threads = 4;
  const int slot_count = 25;
  std::vector<std::thread> threads;
  std::atomic<int> total_slots(0);

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&]() {
      for (int s = 0; s < slot_count; ++s) {
        auto lease = service.AcquireExecutionSlot();
        if (lease) {
          total_slots++;
          // Simulate some work
          std::this_thread::yield();
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(total_slots, num_threads * slot_count);
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

TEST_F(NeugDBServiceTest, SecondServiceOnSameDbThrows) {
  neug::ServiceConfig cfg;
  cfg.query_port = 0;
  cfg.host_str = "127.0.0.1";

  {
    neug::NeugDBService service(*db_, cfg);
    EXPECT_TRUE(db_->HasActiveService());

    // A second service on the same database must be rejected.
    EXPECT_THROW(neug::NeugDBService service2(*db_, cfg),
                 neug::exception::RuntimeError);
  }

  // After the first service is destructed, a new service can be created.
  EXPECT_FALSE(db_->HasActiveService());
  EXPECT_NO_THROW(neug::NeugDBService service(*db_, cfg));
  EXPECT_FALSE(db_->HasActiveService());
}

TEST_F(NeugDBServiceTest, VersionTimelineSurvivesServiceRecreation) {
  timestamp_t first_timestamp = INVALID_TIMESTAMP;
  {
    neug::NeugDBService service(*db_, config_);
    first_timestamp = InsertModernPersonAndReturnTimestamp(service, 1001);
  }

  {
    neug::NeugDBService service(*db_, config_);
    const auto second_timestamp =
        InsertModernPersonAndReturnTimestamp(service, 1002);
    EXPECT_GT(second_timestamp, first_timestamp);
  }
}

TEST_F(NeugDBServiceTest, TpDmlKeepsPlanningGenerationAndDdlAdvancesIt) {
  neug::NeugDBService service(*db_, config_);
  auto slot = service.AcquireExecutionSlot();
  ASSERT_TRUE(slot);

  const auto initial_generation =
      ReadPlanningGeneration(db_->graph_snapshot_store());

  auto insert =
      slot->ExecuteTransactionalRequest(RequestSerializer::SerializeRequest(
          "CREATE (:person {id: 10001, name: 'cache-test', age: 1});", "insert",
          {}));
  ASSERT_TRUE(insert) << insert.error().ToString();
  EXPECT_EQ(ReadPlanningGeneration(db_->graph_snapshot_store()),
            initial_generation);

  auto update =
      slot->ExecuteTransactionalRequest(RequestSerializer::SerializeRequest(
          "MATCH (n:person {id: 10001}) SET n.age = 2;", "update", {}));
  ASSERT_TRUE(update) << update.error().ToString();
  EXPECT_EQ(ReadPlanningGeneration(db_->graph_snapshot_store()),
            initial_generation);

  auto ddl =
      slot->ExecuteTransactionalRequest(RequestSerializer::SerializeRequest(
          "CREATE NODE TABLE cache_probe(id INT64, PRIMARY KEY(id));", "schema",
          {}));
  ASSERT_TRUE(ddl) << ddl.error().ToString();
  EXPECT_EQ(ReadPlanningGeneration(db_->graph_snapshot_store()),
            initial_generation + 1);

  auto read =
      slot->ExecuteTransactionalRequest(RequestSerializer::SerializeRequest(
          "MATCH (n:person) RETURN count(n);", "read", {}));
  ASSERT_TRUE(read) << read.error().ToString();
}

TEST_F(NeugDBServiceTest, DirectPlanningGenerationTracksActualDdlMutations) {
  const auto initial_generation =
      ReadPlanningGeneration(db_->graph_snapshot_store());

  auto connection = db_->Connect();
  auto explain = connection->Query(
      "EXPLAIN CREATE NODE TABLE direct_schema_probe("
      "id INT64, PRIMARY KEY(id));",
      "schema");
  ASSERT_TRUE(explain) << explain.error().ToString();
  EXPECT_EQ(ReadPlanningGeneration(db_->graph_snapshot_store()),
            initial_generation);

  auto create = connection->Query(
      "CREATE NODE TABLE direct_schema_probe(id INT64, PRIMARY KEY(id));",
      "schema");
  ASSERT_TRUE(create) << create.error().ToString();
  EXPECT_EQ(ReadPlanningGeneration(db_->graph_snapshot_store()),
            initial_generation + 1);

  auto read = connection->Query("MATCH (n:person) RETURN count(n);", "read");
  ASSERT_TRUE(read) << read.error().ToString();

  auto no_op = connection->Query(
      "CREATE NODE TABLE IF NOT EXISTS direct_schema_probe("
      "id INT64, PRIMARY KEY(id));",
      "schema");
  ASSERT_TRUE(no_op) << no_op.error().ToString();
  EXPECT_EQ(ReadPlanningGeneration(db_->graph_snapshot_store()),
            initial_generation + 1);
  connection->Close();
}

TEST_F(NeugDBServiceTest,
       DirectUpdateAndBulkLoadInvalidateCacheWithoutSchemaChange) {
  const auto initial_generation =
      ReadPlanningGeneration(db_->graph_snapshot_store());

  auto connection = db_->Connect();
  auto insert = connection->Query(
      "CREATE (:person {id: 20001, name: 'direct-cache-test', age: 1});",
      "insert");
  ASSERT_TRUE(insert) << insert.error().ToString();
  EXPECT_EQ(ReadPlanningGeneration(db_->graph_snapshot_store()),
            initial_generation);

  auto update = connection->Query("MATCH (n:person {id: 20001}) SET n.age = 2;",
                                  "update");
  ASSERT_TRUE(update) << update.error().ToString();
  EXPECT_EQ(ReadPlanningGeneration(db_->graph_snapshot_store()),
            initial_generation + 1);

  const auto copy_path = test_dir_ / "direct-cache-copy.csv";
  {
    std::ofstream copy_file(copy_path);
    copy_file << "id|name|age\n"
              << "20002|direct-copy-cache-test|3\n";
  }
  auto copy = connection->Query(
      "COPY person FROM \"" + copy_path.string() + "\";", "update");
  ASSERT_TRUE(copy) << copy.error().ToString();
  EXPECT_EQ(ReadPlanningGeneration(db_->graph_snapshot_store()),
            initial_generation + 2);
  connection->Close();
}

TEST_F(NeugDBServiceTest, QueryCacheSeparatesPlanningGenerations) {
  neug::NeugDBService service(*db_, config_);
  auto slot = service.AcquireExecutionSlot();
  ASSERT_TRUE(slot);

  // Keep the old snapshot pinned while a DDL publishes a new schema.
  auto old_txn = slot->GetReadTransaction();
  ASSERT_FALSE(old_txn.schema().is_vertex_label_valid("cache_gen_probe"));
  const auto old_generation =
      ReadPlanningGeneration(db_->graph_snapshot_store());

  auto ddl_slot = service.AcquireExecutionSlot();
  ASSERT_TRUE(ddl_slot);
  auto ddl =
      ddl_slot->ExecuteTransactionalRequest(RequestSerializer::SerializeRequest(
          "CREATE NODE TABLE cache_gen_probe(id INT64, PRIMARY KEY(id));",
          "schema", {}));
  ASSERT_TRUE(ddl) << ddl.error().ToString();

  const auto query_cache = db_->GetQueryCache();
  execution::LocalQueryCache local_cache(query_cache);
  const std::string query = "MATCH (n:person) RETURN count(n);";

  // Cache generation advances lazily when a newer published snapshot first
  // requests a plan. Until then, an old pinned snapshot may still use the old
  // generation cache without affecting queries on the new generation.
  auto old_plan = local_cache.Get(old_txn.statistic(), query);
  ASSERT_TRUE(old_plan) << old_plan.error().ToString();
  auto old_plan_again = local_cache.Get(old_txn.statistic(), query);
  ASSERT_TRUE(old_plan_again) << old_plan_again.error().ToString();
  EXPECT_EQ(old_plan.value().get(), old_plan_again.value().get());

  auto new_txn = slot->GetReadTransaction();
  ASSERT_TRUE(new_txn.schema().is_vertex_label_valid("cache_gen_probe"));
  const auto new_generation =
      ReadPlanningGeneration(db_->graph_snapshot_store());
  ASSERT_EQ(new_generation, old_generation + 1);
  auto new_plan = local_cache.Get(new_txn.statistic(), query);
  ASSERT_TRUE(new_plan) << new_plan.error().ToString();
  EXPECT_NE(old_plan.value().get(), new_plan.value().get());
  auto new_plan_again = local_cache.Get(new_txn.statistic(), query);
  ASSERT_TRUE(new_plan_again) << new_plan_again.error().ToString();
  EXPECT_EQ(new_plan.value().get(), new_plan_again.value().get());

  // Old snapshots continue to compile without global write-back. The local
  // cache may retain that result until its generation changes.
  auto old_plan_after_new = local_cache.Get(old_txn.statistic(), query);
  ASSERT_TRUE(old_plan_after_new) << old_plan_after_new.error().ToString();
  EXPECT_NE(old_plan.value().get(), old_plan_after_new.value().get());
  auto new_plan_after_old = local_cache.Get(new_txn.statistic(), query);
  ASSERT_TRUE(new_plan_after_old) << new_plan_after_old.error().ToString();
  EXPECT_EQ(new_plan.value().get(), new_plan_after_old.value().get());
}

TEST_F(NeugDBServiceTest, TransactionalRequestBindsBooleanParameters) {
  auto connection = db_->Connect();
  ASSERT_TRUE(
      connection->Query("CREATE NODE TABLE tp_param_bool("
                        "id INT64, enabled BOOL, PRIMARY KEY(id));",
                        "schema"));
  ASSERT_TRUE(connection->Query(
      "CREATE (:tp_param_bool {id: 1, enabled: true});", "insert"));
  connection->Close();

  neug::NeugDBService service(*db_, config_);
  auto slot = service.AcquireExecutionSlot();
  ASSERT_TRUE(slot);

  auto read = slot->ExecuteTransactionalRequest(R"json(
      {"query":"MATCH (n:tp_param_bool) WHERE n.enabled = $value RETURN n.enabled;",
       "access_mode":"read","parameters":{"value":true}})json");
  ASSERT_TRUE(read) << read.error().ToString();

  QueryResponse response;
  ASSERT_TRUE(response.ParseFromString(read.value()));
  EXPECT_EQ(response.row_count(), 1u);
}

TEST_F(NeugDBServiceTest, TransactionalRequestIgnoresUnexpectedParameters) {
  neug::NeugDBService service(*db_, config_);
  auto slot = service.AcquireExecutionSlot();
  ASSERT_TRUE(slot);

  auto result = slot->ExecuteTransactionalRequest(R"json(
      {"query":"MATCH (n:person) RETURN n.id;",
       "access_mode":"read","parameters":{"unused":1}})json");

  ASSERT_TRUE(result) << result.error().ToString();

  QueryResponse response;
  ASSERT_TRUE(response.ParseFromString(result.value()));
  EXPECT_GT(response.row_count(), 0u);
}

TEST_F(NeugDBServiceTest, TransactionalRequestRejectsNonObjectParameters) {
  neug::NeugDBService service(*db_, config_);
  auto slot = service.AcquireExecutionSlot();
  ASSERT_TRUE(slot);

  auto result = slot->ExecuteTransactionalRequest(R"json(
      {"query":"MATCH (n:person) RETURN n.id;",
       "access_mode":"read","parameters":[]})json");

  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().error_code(), StatusCode::ERR_INVALID_ARGUMENT);
  EXPECT_NE(result.error().error_message().find(
                "Query parameters must be a JSON object."),
            std::string::npos);
}

TEST_F(NeugDBServiceTest, TransactionalSlotRejectsEmbeddedEntryPoint) {
  neug::NeugDBService service(*db_, config_);
  auto slot = service.AcquireExecutionSlot();
  ASSERT_TRUE(slot);

  const auto query_num_before = slot->query_num();
  auto result = slot->ExecuteQuery("MATCH (n:person) RETURN n.id;", "read");

  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().error_code(), StatusCode::ERR_NOT_SUPPORTED);
  EXPECT_EQ(slot->query_num(), query_num_before);
}

TEST_F(NeugDBServiceTest, TransactionalSlotGetsSchemaThroughReadTransaction) {
  neug::NeugDBService service(*db_, config_);
  auto slot = service.AcquireExecutionSlot();
  ASSERT_TRUE(slot);

  const auto schema = slot->GetSchema();

  EXPECT_NE(schema.find("person"), std::string::npos);
  auto read =
      slot->ExecuteTransactionalRequest(RequestSerializer::SerializeRequest(
          "MATCH (n:person) RETURN count(n);", "read", {}));
  ASSERT_TRUE(read) << read.error().ToString();
}

TEST_F(NeugDBServiceTest, ApUpdateAfterTpUsesCurrentReadTimestamp) {
  timestamp_t tp_timestamp = INVALID_TIMESTAMP;
  {
    neug::NeugDBService service(*db_, config_);
    tp_timestamp = InsertModernPersonAndReturnTimestamp(service, 1001);
  }
  ASSERT_GT(tp_timestamp, 0);

  auto connection = db_->Connect();
  auto update_result = connection->Query(
      "MATCH (n:person {id: 1001}) "
      "SET n.age = 31 "
      "RETURN n.age;",
      "update");
  ASSERT_TRUE(update_result) << update_result.error().ToString();
  EXPECT_EQ(update_result.value().response().row_count(), 1);

  auto read_result =
      connection->Query("MATCH (n:person {id: 1001}) RETURN n.age;", "read");
  ASSERT_TRUE(read_result) << read_result.error().ToString();
  const auto& response = read_result.value().response();
  ASSERT_EQ(response.row_count(), 1);
  ASSERT_EQ(response.arrays_size(), 1);
  ASSERT_EQ(response.arrays(0).int64_array().values_size(), 1);
  EXPECT_EQ(response.arrays(0).int64_array().values(0), 31);
}

TEST_F(NeugDBServiceTest, PrepareForServingResetsSharedApTpTimeline) {
  timestamp_t timestamp_before_checkpoint = INVALID_TIMESTAMP;
  {
    neug::NeugDBService service(*db_, config_);
    timestamp_before_checkpoint =
        InsertModernPersonAndReturnTimestamp(service, 1001);
    EXPECT_GT(timestamp_before_checkpoint, 1)
        << "TP must continue the VersionManager timeline used by AP loading";
  }

  db_->PrepareForServing();

  {
    neug::NeugDBService service(*db_, config_);
    EXPECT_EQ(InsertModernPersonAndReturnTimestamp(service, 1002), 1);
  }
}

TEST_F(NeugDBServiceTest,
       TransactionalCapabilityMatrixRejectsWithoutStorageOrWalSideEffects) {
  const char* csv_dir = std::getenv("MODERN_GRAPH_DATA_DIR");
  ASSERT_NE(csv_dir, nullptr);

  const std::string person_csv =
      (std::filesystem::path(csv_dir) / "person.csv").string();
  const std::string copy_from = "COPY person FROM \"" + person_csv + "\";";
  const std::string copy_temp =
      "COPY TEMP TempPerson FROM \"" + person_csv + "\" (header=true);";
  const std::string load_from = "LOAD FROM \"" + person_csv + "\" RETURN *;";
  const auto export_path = test_dir_ / "rejected-copy-to.csv";
  const std::string copy_to = "COPY (MATCH (n:person) RETURN n.*) TO '" +
                              export_path.string() + "' (header=true);";

  struct CapabilityCase {
    const char* name;
    std::string query;
    const char* access_mode;
    int repetitions;
  };
  const std::vector<CapabilityCase> cases{
      // This exact COPY FROM was executed during fixture loading, so the first
      // rejection is a global-cache hit and the second a local-cache hit.
      {"COPY FROM global/local cache", copy_from, "update", 2},
      {"COPY FROM spoofed read mode", copy_from, "read", 1},
      {"COPY TEMP", copy_temp, "update", 1},
      {"LOAD FROM", load_from, "update", 1},
      {"COPY TO", copy_to, "read", 1},
      {"EXPLAIN COPY FROM", "EXPLAIN " + copy_from, "update", 1},
      {"PROFILE COPY FROM", "PROFILE " + copy_from, "update", 1},
  };

  neug::NeugDBService service(*db_, config_);
  auto slot = service.AcquireExecutionSlot();
  ASSERT_TRUE(slot);
  EXPECT_LT(static_cast<size_t>(slot->SlotId()), service.ExecutionSlotNum());

  const auto person_label = db_->schema().get_vertex_label_id("person");
  const auto person_count_before =
      db_->graph().VertexNum(person_label, MAX_TIMESTAMP);
  const auto storage_modified_before = db_->graph().IsModified();
  const auto wal_dir = db_->graph().checkpoint().wal_dir();
  const auto wal_before = readWalPrefixes(wal_dir);
  ASSERT_FALSE(wal_before.empty());
  ASSERT_FALSE(std::filesystem::exists(export_path));
  ASSERT_FALSE(db_->schema().is_vertex_label_valid("TempPerson"));

  for (const auto& capability_case : cases) {
    SCOPED_TRACE(capability_case.name);
    const auto request = RequestSerializer::SerializeRequest(
        capability_case.query, capability_case.access_mode, {});
    for (int i = 0; i < capability_case.repetitions; ++i) {
      auto result = slot->ExecuteTransactionalRequest(request);
      ASSERT_FALSE(result);
      EXPECT_EQ(result.error().error_code(), StatusCode::ERR_NOT_SUPPORTED)
          << result.error().ToString();
    }
  }

  EXPECT_EQ(readWalPrefixes(wal_dir), wal_before)
      << "Rejected plans must not append WAL";
  EXPECT_EQ(db_->graph().VertexNum(person_label, MAX_TIMESTAMP),
            person_count_before);
  EXPECT_EQ(db_->graph().IsModified(), storage_modified_before);
  EXPECT_FALSE(db_->schema().is_vertex_label_valid("TempPerson"));
  EXPECT_FALSE(std::filesystem::exists(export_path));
}

// TP counterpart of the embedded insert-mode compatibility (P2 review
// Major-1, see ConnectionTest.ExplicitInsertAccessModeAllowsMixedPlan): TP
// selects InsertTransaction for access_mode="insert", so a plan that reads
// or updates must be rejected before execution, without WAL or storage side
// effects; a pure CREATE plan must still be accepted.
TEST_F(NeugDBServiceTest, InsertModeRejectsMixedPlanWithoutSideEffects) {
  neug::NeugDBService service(*db_, config_);
  auto slot = service.AcquireExecutionSlot();
  ASSERT_TRUE(slot);

  const auto person_label = db_->schema().get_vertex_label_id("person");
  const auto person_count_before =
      db_->graph().VertexNum(person_label, MAX_TIMESTAMP);
  const auto wal_dir = db_->graph().checkpoint().wal_dir();
  const auto wal_before = readWalPrefixes(wal_dir);
  ASSERT_FALSE(wal_before.empty());

  // A non-primary-key MATCH needs a graph scan, so the plan is genuinely
  // mixed read + CREATE rather than the atomic key lookup supported by
  // InsertTransaction for relationship insertion.
  auto rejected =
      slot->ExecuteTransactionalRequest(RequestSerializer::SerializeRequest(
          "MATCH (a:person {name: 'vadas'}), (b:person {name: 'josh'}) "
          "CREATE (a)-[:knows {weight: 7.5}]->(b);",
          "insert", {}));
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error().error_code(), StatusCode::ERR_INVALID_ARGUMENT)
      << rejected.error().ToString();
  EXPECT_NE(rejected.error().ToString().find("Insert-only mode"),
            std::string::npos)
      << rejected.error().ToString();

  EXPECT_EQ(readWalPrefixes(wal_dir), wal_before)
      << "Rejected plans must not append WAL";
  EXPECT_EQ(db_->graph().VertexNum(person_label, MAX_TIMESTAMP),
            person_count_before);

  // Pure CREATE is insert-only and stays accepted.
  auto accepted =
      slot->ExecuteTransactionalRequest(RequestSerializer::SerializeRequest(
          "CREATE (:person {id: 90001, name: 'tp-insert', age: 1});", "insert",
          {}));
  ASSERT_TRUE(accepted) << accepted.error().ToString();
}

TEST_F(NeugDBServiceTest, UnsupportedCapabilityMapsToHttp501) {
  const char* csv_dir = std::getenv("MODERN_GRAPH_DATA_DIR");
  ASSERT_NE(csv_dir, nullptr);

  neug::NeugDBService service(*db_, config_);
  const auto uri = service.Start();

  brpc::ChannelOptions options;
  options.protocol = "http";
  options.timeout_ms = 5000;
  options.max_retry = 0;
  brpc::Channel channel;
  ASSERT_EQ(channel.Init(uri.c_str(), "", &options), 0);

  const std::string query =
      "COPY person FROM \"" +
      (std::filesystem::path(csv_dir) / "person.csv").string() + "\";";
  const auto request = RequestSerializer::SerializeRequest(query, "update", {});
  brpc::Controller controller;
  controller.http_request().uri() = (uri + "/cypher").c_str();
  controller.http_request().set_method(brpc::HTTP_METHOD_POST);
  controller.request_attachment().append(request);
  channel.CallMethod(nullptr, &controller, nullptr, nullptr, nullptr);

  EXPECT_TRUE(controller.Failed());
  EXPECT_EQ(controller.http_response().status_code(),
            brpc::HTTP_STATUS_NOT_IMPLEMENTED);
  service.Stop();
}

TEST_F(NeugDBServiceTest, PrepareForServingWhileServiceExistsThrows) {
  neug::NeugDBService service(*db_, config_);
  EXPECT_THROW(db_->PrepareForServing(), neug::exception::RuntimeError);
}

TEST_F(NeugDBServiceTest, ServiceConstructionRejectsOpenConnection) {
  auto conn = db_->Connect();

  EXPECT_THROW({ neug::NeugDBService service(*db_, config_); },
               neug::exception::RuntimeError);
  EXPECT_FALSE(db_->HasActiveService());

  auto result = conn->Query("MATCH (n) RETURN count(n);", "read");
  EXPECT_TRUE(result) << result.error().ToString();
  conn->Close();
  EXPECT_FALSE(db_->HasOpenConnections());
  EXPECT_NO_THROW({ neug::NeugDBService service(*db_, config_); });
}

TEST_F(NeugDBServiceTest, ConnectionManagerCountsOnlyOpenConnections) {
  ConnectionManager connection_manager(*db_, db_->config());
  auto conn = connection_manager.CreateConnection();

  EXPECT_EQ(connection_manager.ConnectionNum(), 1);
  EXPECT_TRUE(connection_manager.HasOpenConnections());

  conn->Close();

  EXPECT_EQ(connection_manager.ConnectionNum(), 0);
  EXPECT_FALSE(connection_manager.HasOpenConnections());
}

TEST_F(NeugDBServiceTest, ServiceInitFailureReleasesRegistration) {
  neug::ServiceConfig bad_cfg;
  bad_cfg.query_port = 0;
  bad_cfg.host_str = "127.0.0.1";
  bad_cfg.thread_num = static_cast<uint32_t>(db_->config().max_thread_num + 1);

  // Construction failure must release all service lifecycle state so the
  // database can serve again.
  EXPECT_THROW(neug::NeugDBService service(*db_, bad_cfg),
               neug::exception::InvalidArgumentException);
  EXPECT_FALSE(db_->HasActiveService());

  neug::ServiceConfig good_cfg;
  good_cfg.query_port = 0;
  good_cfg.host_str = "127.0.0.1";
  {
    neug::NeugDBService service(*db_, good_cfg);
    EXPECT_TRUE(db_->HasActiveService());
  }
  EXPECT_FALSE(db_->HasActiveService());

  // A second successful lifecycle verifies that teardown leaves no hidden
  // service state behind.
  EXPECT_NO_THROW(neug::NeugDBService service(*db_, good_cfg));
  EXPECT_FALSE(db_->HasActiveService());
}

TEST_F(NeugDBServiceTest, ConnectWhileServingThrows) {
  neug::ServiceConfig cfg;
  cfg.query_port = 0;
  cfg.host_str = "127.0.0.1";

  {
    neug::NeugDBService service(*db_, cfg);
    EXPECT_THROW(db_->Connect(), neug::exception::RuntimeError);
  }

  // After the TP-owned slots and their WAL writers are destroyed, a new
  // embedded connection receives a fresh, WAL-free slot.
  auto conn = db_->Connect();
  ASSERT_TRUE(conn != nullptr);
  EXPECT_TRUE(conn->Query("MATCH (n) RETURN count(n);", "read"));
}

TEST_F(NeugDBServiceTest, CloseWhileServingThrows) {
  neug::ServiceConfig cfg;
  cfg.query_port = 0;
  cfg.host_str = "127.0.0.1";

  {
    neug::NeugDBService service(*db_, cfg);
    EXPECT_THROW(db_->Close(), neug::exception::RuntimeError);
    // The failed Close() must not mark the database as closed.
    EXPECT_FALSE(db_->IsClosed());
  }

  // After the service is destructed, Close() works again.
  EXPECT_NO_THROW(db_->Close());
  EXPECT_TRUE(db_->IsClosed());
}

}  // namespace test
}  // namespace neug
