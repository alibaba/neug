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

#include <signal.h>
#include <sys/wait.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

#include <poll.h>

#include <gtest/gtest.h>

#include "neug/compiler/extension/extension_api.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/compiler/main/metadata_registry.h"
#include "neug/compiler/transaction/transaction.h"
#include "neug/main/connection.h"
#include "neug/main/file_lock.h"
#include "neug/main/neug_db.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/exception/exception.h"
#include "unittest/utils.h"

namespace neug {

namespace test {
namespace {

class PrepareForServingTestFunction : public function::NeugCallFunction {
 public:
  PrepareForServingTestFunction()
      : NeugCallFunction(
            "PREPARE_FOR_SERVING_TEST_EXTENSION", function::call_input_types{},
            {{"name", ::neug::DataType(::neug::DataTypeId::kVarchar)}}) {}
};

struct PrepareForServingTestFunctionSet {
  static constexpr const char* name = "PREPARE_FOR_SERVING_TEST_EXTENSION";
  static function::function_set getFunctionSet() {
    function::function_set function_set;
    function_set.emplace_back(
        std::make_unique<PrepareForServingTestFunction>());
    return function_set;
  }
};

bool HasPrepareForServingTestFunction() {
  return main::MetadataRegistry::getCatalog()->containsFunction(
      &transaction::DUMMY_TRANSACTION, PrepareForServingTestFunctionSet::name,
      false);
}

}  // namespace

class ConnectionTest : public ::testing::Test {
 protected:
  static constexpr const char* DB_DIR = "/tmp/connection_test";
  void SetUp() override {
    if (std::filesystem::exists(DB_DIR)) {
      std::filesystem::remove_all(DB_DIR);
    }
    std::filesystem::create_directories(DB_DIR);

    std::unique_ptr<neug::NeugDB> db_ = std::make_unique<neug::NeugDB>();
    neug::NeugDBConfig config;
    config.data_dir = DB_DIR;
    config.checkpoint_on_close = true;
    db_->Open(config);
    auto conn = db_->Connect();

    load_modern_graph(conn);
    LOG(INFO) << "[Setup] Modern graph loaded.";
    conn->Close();
    db_->Close();
    db_.reset();
  }
  void TearDown() override {
    if (std::filesystem::exists(DB_DIR)) {
      std::filesystem::remove_all(DB_DIR);
    }
  }

  void InitParameterizedQueryData(std::shared_ptr<Connection> conn) {
    EXPECT_TRUE(conn->Query(
        "CREATE NODE TABLE PERSON2 (id INT64, id2 INT64, name STRING, "
        "emails STRING, PRIMARY KEY(id));"));
    EXPECT_TRUE(
        conn->Query("CREATE REL TABLE atomic_knows(FROM PERSON2 TO PERSON2, "
                    "since INT64);"));

    EXPECT_TRUE(
        conn->Query("CREATE (u: PERSON2 { id: 1, id2: 1, name: 'Alice', "
                    "emails: 'alice@example.com' });"));
    EXPECT_TRUE(
        conn->Query("CREATE (u: PERSON2 { id: 2, id2: 1, name: 'Bob', "
                    "emails: 'bob@example.com;bobby@hotmail.com' });"));
  }
};

TEST_F(ConnectionTest, TestReadWriteConnection) {
  NeugDB db;
  NeugDBConfig config;
  config.data_dir = DB_DIR;
  config.mode = DBMode::READ_WRITE;
  db.Open(config);

  auto conn1 = db.Connect();
  EXPECT_NE(conn1, nullptr);

  EXPECT_THROW({ auto conn2 = db.Connect(); },
               neug::exception::TxStateConflictException);
}

TEST_F(ConnectionTest, TestReadOnlyConnections) {
  NeugDB db;
  NeugDBConfig config;
  config.data_dir = DB_DIR;
  config.mode = DBMode::READ_ONLY;
  db.Open(config);

  std::vector<std::shared_ptr<Connection>> connections;
  const int num_connections = 5;
  for (int i = 0; i < num_connections; ++i) {
    auto conn = db.Connect();
    EXPECT_NE(conn, nullptr);
    connections.emplace_back(conn);
  }
  // Run DDL query on read-only database should fail
  auto res = connections[0]->Query(
      "CREATE NODE TABLE test_node (id INT64 PRIMARY KEY, name STRING);");
  EXPECT_FALSE(res);
  auto res2 = connections[0]->Query("MATCH(n) return count(n);");
  EXPECT_TRUE(res2);
  // A read-only plan must still be rejected when the caller requests a
  // write transaction mode.
  auto res_read_as_update =
      connections[0]->Query("MATCH(n) return count(n);", "update");
  EXPECT_FALSE(res_read_as_update);
  auto res3 =
      connections[0]->Query("MATCH(n) where n.id = 1 SET n.name = 'Alice';");
  EXPECT_FALSE(res3);
}

TEST_F(ConnectionTest, ReadOnlyConnectionsExecuteConcurrently) {
  NeugDB db;
  NeugDBConfig config;
  config.data_dir = DB_DIR;
  config.mode = DBMode::READ_ONLY;
  db.Open(config);

  constexpr int kConnectionCount = 5;
  constexpr int kQueriesPerConnection = 20;
  std::vector<std::shared_ptr<Connection>> connections;
  for (int i = 0; i < kConnectionCount; ++i) {
    connections.emplace_back(db.Connect());
  }

  std::atomic<int> successful_queries{0};
  std::vector<std::thread> workers;
  for (const auto& connection : connections) {
    workers.emplace_back([connection, &successful_queries]() {
      for (int query_id = 0; query_id < kQueriesPerConnection; ++query_id) {
        if (connection->Query("MATCH (n) RETURN count(n);", "read")) {
          successful_queries.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  for (auto& worker : workers) {
    worker.join();
  }

  EXPECT_EQ(successful_queries.load(),
            kConnectionCount * kQueriesPerConnection);
}

TEST(ConnectionReadOnlyTest, MultipleProcessesShareDatabase) {
  const auto db_dir =
      std::filesystem::temp_directory_path() /
      ("neug_read_only_process_test_" + std::to_string(::getpid()));
  std::filesystem::remove_all(db_dir);

  NeugDBConfig write_config(db_dir.string(), 1);
  write_config.checkpoint_on_close = true;
  write_config.memory_level = MemoryLevel::kInMemory;
  {
    NeugDB db;
    ASSERT_TRUE(db.Open(write_config));
    auto connection = db.Connect();
    ASSERT_TRUE(connection->Query(
        "CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));", "schema"));
    connection->Close();
    db.Close();
  }

  const auto allocator_marker =
      db_dir / "checkpoint-1" / "allocator" / "read_only_marker";
  ASSERT_TRUE(std::filesystem::is_directory(allocator_marker.parent_path()));
  {
    std::ofstream marker(allocator_marker);
    ASSERT_TRUE(marker.is_open());
    marker << "preserve across read-only opens";
  }
  ASSERT_TRUE(std::filesystem::exists(allocator_marker));

  NeugDBConfig parent_read_config(db_dir.string(), 1);
  parent_read_config.mode = DBMode::READ_ONLY;
  parent_read_config.memory_level = MemoryLevel::kSyncToFile;
  NeugDB parent_reader;
  ASSERT_TRUE(parent_reader.Open(parent_read_config));

  int ready_pipe[2];
  int release_pipe[2];
  ASSERT_EQ(::pipe(ready_pipe), 0);
  ASSERT_EQ(::pipe(release_pipe), 0);

  const auto child = [&]() -> pid_t {
    const pid_t pid = ::fork();
    if (pid != 0) {
      return pid;
    }
    ::close(ready_pipe[0]);
    ::close(release_pipe[1]);
    NeugDBConfig read_config(db_dir.string(), 1);
    read_config.mode = DBMode::READ_ONLY;
    read_config.memory_level = MemoryLevel::kSyncToFile;
    char status = '0';
    NeugDB db;
    try {
      const bool opened = db.Open(read_config);
      auto connection = db.Connect();
      const bool queried =
          connection->Query("MATCH (n:person) RETURN count(n);", "read")
              .has_value();
      status = opened && queried ? '1' : '0';
    } catch (...) {}
    (void) ::write(ready_pipe[1], &status, 1);
    char release = 0;
    (void) ::read(release_pipe[0], &release, 1);
    if (status == '1') {
      db.Close();
    }
    ::_exit(status == '1' ? 0 : 1);
  };

  const auto read_status = [&]() {
    pollfd fd{ready_pipe[0], POLLIN, 0};
    char status = '0';
    if (::poll(&fd, 1, 5000) == 1 && ::read(ready_pipe[0], &status, 1) == 1) {
      return status;
    }
    return '0';
  };

  // Fully open the first reader before starting the second. This ensures the
  // second Checkpoint::Open sees the first process's active runtime files and
  // exercises the cross-process orphan-cleanup race deterministically.
  const pid_t first = child();
  EXPECT_GT(first, 0);
  const char first_ready = read_status();
  EXPECT_EQ(first_ready, '1');

  std::vector<std::filesystem::path> first_runtime_files;
  const auto runtime_dir = db_dir / "checkpoint-1" / "runtime";
  for (const auto& entry : std::filesystem::directory_iterator(runtime_dir)) {
    if (entry.is_regular_file()) {
      first_runtime_files.emplace_back(entry.path());
    }
  }
  EXPECT_FALSE(first_runtime_files.empty());

  const pid_t second = child();
  EXPECT_GT(second, 0);
  ::close(ready_pipe[1]);
  ::close(release_pipe[0]);
  const char second_ready = read_status();
  EXPECT_EQ(second_ready, '1');
  for (const auto& path : first_runtime_files) {
    EXPECT_TRUE(std::filesystem::exists(path));
  }
  EXPECT_TRUE(std::filesystem::exists(allocator_marker));

  // The children were forked while this reader was open. After releasing the
  // parent's lock, their independently reacquired locks must still exclude a
  // writer.
  parent_reader.Close();
  std::string lock_error;
  FileLock writer(db_dir.string());
  EXPECT_FALSE(writer.lock(lock_error, DBMode::READ_WRITE));

  ::close(release_pipe[1]);
  const auto wait_for_child = [](pid_t child_pid) {
    int status = 0;
    for (int attempt = 0; attempt < 50; ++attempt) {
      const auto result = ::waitpid(child_pid, &status, WNOHANG);
      if (result == child_pid || result == -1) {
        return status;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    (void) ::kill(child_pid, SIGKILL);
    (void) ::waitpid(child_pid, &status, 0);
    return status;
  };
  const int first_status = wait_for_child(first);
  const int second_status = wait_for_child(second);
  EXPECT_TRUE(WIFEXITED(first_status));
  EXPECT_EQ(WEXITSTATUS(first_status), 0);
  EXPECT_TRUE(WIFEXITED(second_status));
  EXPECT_EQ(WEXITSTATUS(second_status), 0);
  for (const auto& path : first_runtime_files) {
    EXPECT_FALSE(std::filesystem::exists(path));
  }
  EXPECT_TRUE(std::filesystem::exists(allocator_marker));
  ASSERT_TRUE(writer.lock(lock_error, DBMode::READ_WRITE)) << lock_error;
  writer.unlock();

  ::close(ready_pipe[0]);
  std::filesystem::remove_all(db_dir);
}

// weakly_canonical normalizes the data directory before locking, so opening
// the same database through a symlink resolves to the same lock-table entry.
TEST(ConnectionReadOnlyTest, SymlinkedDirectoryResolvesToSameLockEntry) {
  const auto db_dir =
      std::filesystem::temp_directory_path() /
      ("neug_symlink_lock_db_test_" + std::to_string(::getpid()));
  const auto link_dir =
      std::filesystem::temp_directory_path() /
      ("neug_symlink_lock_link_test_" + std::to_string(::getpid()));
  std::filesystem::remove_all(db_dir);
  std::filesystem::remove_all(link_dir);

  NeugDBConfig write_config(db_dir.string(), 1);
  write_config.checkpoint_on_close = true;
  write_config.memory_level = MemoryLevel::kInMemory;
  {
    NeugDB db;
    ASSERT_TRUE(db.Open(write_config));
    auto connection = db.Connect();
    ASSERT_TRUE(connection->Query(
        "CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));", "schema"));
    connection->Close();
    db.Close();
  }

  std::filesystem::create_directory_symlink(db_dir, link_dir);

  // A writer opened through the real path conflicts with a read-only open
  // through the symlink.
  {
    NeugDB writer;
    ASSERT_TRUE(writer.Open(write_config));

    NeugDBConfig symlink_read_config(link_dir.string(), 1);
    symlink_read_config.mode = DBMode::READ_ONLY;
    symlink_read_config.memory_level = MemoryLevel::kSyncToFile;
    NeugDB symlink_reader;
    EXPECT_THROW(symlink_reader.Open(symlink_read_config),
                 neug::exception::DatabaseLockedException);

    writer.Close();
  }

  // Read-only opens through the real path and the symlink share the lock
  // entry and coexist.
  {
    NeugDBConfig read_config(db_dir.string(), 1);
    read_config.mode = DBMode::READ_ONLY;
    read_config.memory_level = MemoryLevel::kSyncToFile;
    NeugDB first_reader;
    ASSERT_TRUE(first_reader.Open(read_config));

    NeugDBConfig symlink_read_config(link_dir.string(), 1);
    symlink_read_config.mode = DBMode::READ_ONLY;
    symlink_read_config.memory_level = MemoryLevel::kSyncToFile;
    NeugDB second_reader;
    ASSERT_TRUE(second_reader.Open(symlink_read_config));

    auto connection = second_reader.Connect();
    ASSERT_NE(connection, nullptr);
    EXPECT_TRUE(connection->Query("MATCH (n:person) RETURN count(n);", "read"));
    connection->Close();
    second_reader.Close();
    first_reader.Close();
  }

  std::filesystem::remove_all(link_dir);
  std::filesystem::remove_all(db_dir);
}

// Two processes opening the same database read-only at the same time must
// both succeed: they race on the fcntl lock and on the O_EXCL runtime-file
// reservation, and the retries must converge instead of failing.
TEST(ConnectionReadOnlyTest, ConcurrentProcessesOpenDatabaseSimultaneously) {
  const auto db_dir =
      std::filesystem::temp_directory_path() /
      ("neug_concurrent_open_test_" + std::to_string(::getpid()));
  std::filesystem::remove_all(db_dir);

  NeugDBConfig write_config(db_dir.string(), 1);
  write_config.checkpoint_on_close = true;
  write_config.memory_level = MemoryLevel::kInMemory;
  {
    NeugDB db;
    ASSERT_TRUE(db.Open(write_config));
    auto connection = db.Connect();
    ASSERT_TRUE(connection->Query(
        "CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));", "schema"));
    connection->Close();
    db.Close();
  }

  int start_pipe[2];
  ASSERT_EQ(::pipe(start_pipe), 0);

  pid_t children[2];
  for (auto& child_pid : children) {
    const pid_t pid = ::fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
      ::close(start_pipe[1]);
      char token = 0;
      // Block until the parent closes the write end, releasing every child
      // at the same moment to start the open race.
      while (::read(start_pipe[0], &token, 1) == -1 && errno == EINTR) {}
      ::close(start_pipe[0]);

      char status = '0';
      try {
        NeugDBConfig read_config(db_dir.string(), 1);
        read_config.mode = DBMode::READ_ONLY;
        read_config.memory_level = MemoryLevel::kSyncToFile;
        NeugDB db;
        const bool opened = db.Open(read_config);
        auto connection = db.Connect();
        const bool queried =
            connection->Query("MATCH (n:person) RETURN count(n);", "read")
                .has_value();
        status = opened && queried ? '1' : '0';
        db.Close();
      } catch (...) {}
      ::_exit(status == '1' ? 0 : 1);
    }
    child_pid = pid;
  }
  ::close(start_pipe[0]);
  // Let both children block on the pipe, then release them simultaneously.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ::close(start_pipe[1]);

  for (const auto child_pid : children) {
    int status = 0;
    ASSERT_EQ(::waitpid(child_pid, &status, 0), child_pid);
    EXPECT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
  }
  std::filesystem::remove_all(db_dir);
}

// Explicit access_mode=read: read-only CALL is allowed, mutating CALL is not.
TEST_F(ConnectionTest, TestExplicitReadAccessModeForCall) {
  NeugDB db;
  NeugDBConfig config;
  config.data_dir = DB_DIR;
  config.mode = DBMode::READ_WRITE;
  db.Open(config);

  auto conn = db.Connect();
  ASSERT_NE(conn, nullptr);

  auto show_res = conn->Query("CALL SHOW_LOADED_EXTENSIONS();", "read");
  ASSERT_TRUE(show_res) << show_res.error().ToString();

  auto project_res = conn->Query(
      "CALL project_graph('g', ['person'], {'[person, knows, person]': ''});",
      "read");
  ASSERT_FALSE(project_res);
  EXPECT_NE(project_res.error().ToString().find(
                "Write queries are not supported in read-only mode"),
            std::string::npos)
      << project_res.error().ToString();
}

// Regression test for the P2 review (Major-1): the embedded (AP) path
// intentionally retains legacy compatibility — an explicit
// access_mode="insert" query whose plan also reads (MATCH) must still
// execute. The insert-only restriction applies only to the TP path
// (ExecuteTransactionalRequest); see
// NeugDBServiceTest.InsertModeRejectsMixedPlanWithoutSideEffects.
TEST_F(ConnectionTest, ExplicitInsertAccessModeAllowsMixedPlan) {
  NeugDB db;
  NeugDBConfig config;
  config.data_dir = DB_DIR;
  config.mode = DBMode::READ_WRITE;
  db.Open(config);

  auto conn = db.Connect();
  ASSERT_NE(conn, nullptr);

  // MATCH on a non-primary-key property forces a graph scan, so the plan is
  // genuinely read + CREATE — not the atomic key-lookup insert that the
  // analyzer classifies as insert-only. A regression re-introducing the
  // insert-only check into the shared prepareQuery would reject this.
  auto res = conn->Query(
      "MATCH (a:person {name: 'vadas'}), (b:person {name: 'josh'}) "
      "CREATE (a)-[:knows {weight: 7.5}]->(b);",
      "insert");
  ASSERT_TRUE(res) << res.error().ToString();

  auto check = conn->Query(
      "MATCH (a:person {id: 2})-[e:knows]->(b:person {id: 4}) "
      "RETURN e.weight;",
      "read");
  ASSERT_TRUE(check) << check.error().ToString();
  EXPECT_EQ(check.value().response().row_count(), 1);
}

TEST(ConnectionStandaloneTest, PrepareForServingPreservesLoadedExtensions) {
  constexpr const char* db_dir = "/tmp/prepare_for_serving_test";
  std::filesystem::remove_all(db_dir);

  NeugDB db;
  NeugDBConfig config;
  config.data_dir = db_dir;
  config.mode = DBMode::READ_WRITE;
  config.checkpoint_on_close = false;
  db.Open(config);

  auto planner = db.GetPlanner();

  extension::ExtensionAPI::registerFunction<PrepareForServingTestFunctionSet>(
      catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);
  ASSERT_TRUE(HasPrepareForServingTestFunction());

  db.PrepareForServing();
  EXPECT_EQ(planner, db.GetPlanner());
  EXPECT_TRUE(HasPrepareForServingTestFunction());

  db.Close();
  std::filesystem::remove_all(db_dir);
}

// Test Parameterized Query
TEST_F(ConnectionTest, TestParameterizedQuery) {
  NeugDB db;
  NeugDBConfig config;
  config.data_dir = DB_DIR;
  config.mode = DBMode::READ_WRITE;

  db.Open(config);

  auto conn = db.Connect();
  EXPECT_NE(conn, nullptr);

  InitParameterizedQueryData(conn);

  rapidjson::Document parameters(rapidjson::kObjectType);
  parameters.AddMember("person_id", 1, parameters.GetAllocator());
  parameters.AddMember("increment", 5, parameters.GetAllocator());
  auto res = conn->Query(
      "MATCH (n:PERSON2 {id: $person_id}) SET n.id2 = n.id2 + "
      "$increment;",
      "update", parameters);
  ASSERT_TRUE(res) << res.error().ToString();
  LOG(INFO) << res.value().ToString();

  ASSERT_TRUE(
      conn->Query("MATCH (a:PERSON2 {id: 1}), (b:PERSON2 {id: 2}) "
                  "CREATE (a)-[:atomic_knows {since: 1}]->(b);",
                  "insert"));
  rapidjson::Document edge_parameters(rapidjson::kObjectType);
  edge_parameters.AddMember("increment", 2, edge_parameters.GetAllocator());
  res = conn->Query(
      "MATCH (:PERSON2)-[e:atomic_knows]->(:PERSON2) "
      "SET e.since = e.since + $increment;",
      "update", edge_parameters);
  ASSERT_TRUE(res) << res.error().ToString();

  rapidjson::Document invalid_parameters(rapidjson::kArrayType);
  res =
      conn->Query("MATCH (n:PERSON2) RETURN n.id;", "read", invalid_parameters);
  ASSERT_FALSE(res);
  EXPECT_EQ(res.error().error_code(), StatusCode::ERR_INVALID_ARGUMENT);
}

TEST_F(ConnectionTest, TestConnectionQueryResult) {
  NeugDB db;
  NeugDBConfig config;
  config.data_dir = DB_DIR;
  config.mode = DBMode::READ_ONLY;
  db.Open(config);

  auto conn = db.Connect();
  EXPECT_NE(conn, nullptr);

  auto res = conn->Query("MATCH (n:person) RETURN n.id ORDER BY n.id;");
  EXPECT_TRUE(res);
  const auto& res_value = res.value();
  std::vector<int64_t> ids;
  auto table = res_value.response();
  auto id_column = table.arrays(0).int64_array();
  for (int64_t i = 0; i < id_column.values_size(); ++i) {
    ids.push_back(id_column.values(i));
  }
  EXPECT_EQ(ids.size(), 4);
  std::vector<int64_t> expected_ids = {1, 2, 4, 6};
  EXPECT_EQ(ids, expected_ids);
}

TEST_F(ConnectionTest, ApMutationCheckpointRoundTripUsesBaselineTimestamp) {
  NeugDBConfig config;
  config.data_dir = DB_DIR;
  config.mode = DBMode::READ_WRITE;
  config.checkpoint_on_close = false;

  {
    NeugDB db;
    db.Open(config);
    auto connection = db.Connect();
    ASSERT_TRUE(connection->Query(
        "CREATE (:person {id: 10001, name: 'ap-timestamp', age: 1});"));
    ASSERT_TRUE(
        connection->Query("MATCH (a:person {id: 10001}), (b:person {id: 1}) "
                          "CREATE (a)-[:knows {weight: 9.0}]->(b);"));
    ASSERT_TRUE(connection->Query("CHECKPOINT;"));
    db.Close();
  }

  {
    NeugDB reopened;
    reopened.Open(config);
    auto connection = reopened.Connect();
    auto result = connection->Query(
        "MATCH (a:person {id: 10001})-[e:knows]->(b:person {id: 1}) "
        "RETURN e.weight;",
        "read");
    ASSERT_TRUE(result) << result.error().ToString();
    EXPECT_EQ(result.value().response().row_count(), 1);

    // Explicit AP CHECKPOINT dumps/reopens without advancing a durable WAL
    // timeline. Both vertex and edge mutations must therefore remain visible
    // from the baseline timestamp restored on process restart.
    SnapshotGuard snapshot(reopened.graph_snapshot_store());
    StorageReadInterface storage(snapshot.get().view(), 0);
    const auto person_label = storage.schema().get_vertex_label_id("person");
    vid_t vertex_id = 0;
    EXPECT_TRUE(
        storage.GetVertexIndex(person_label, Value::INT64(10001), vertex_id));
  }
}

}  // namespace test

}  // namespace neug
