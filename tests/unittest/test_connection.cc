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
#include <filesystem>

#include "neug/main/connection.h"
#include "neug/main/neug_db.h"
#include "unittest/utils.h"

namespace neug {

namespace test {
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
    config.compact_on_close = true;
    config.compact_csr = true;
    config.enable_auto_compaction = false;  // TODO(zhanglei): very slow
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

  void atomicityInit(std::shared_ptr<Connection> conn) {
    EXPECT_TRUE(conn->Query(
        "CREATE NODE TABLE PERSON (id INT64, id2 INT64, name STRING, "
        "emails STRING, PRIMARY KEY(id));"));
    EXPECT_TRUE(conn->Query(
        "CREATE REL TABLE KNOWS(FROM PERSON TO PERSON, since INT64);"));

    EXPECT_TRUE(
        conn->Query("CREATE (u: PERSON { id: 1, id2: 1, name: 'Alice', emails: "
                    "'alice@example.com' });"));
    EXPECT_TRUE(
        conn->Query("CREATE (u: PERSON { id: 2, id2: 1, name: 'Bob', emails: "
                    "'bob@example.com;bobby@hotmail.com' });"));
  }

  std::pair<int64_t, int64_t> atomicityCheck(std::shared_ptr<Connection> conn) {
    std::vector<std::string> emails;
    auto res = conn->Query("MATCH (n:PERSON) RETURN n.id2 ORDER BY n.id2;");
    EXPECT_TRUE(res);
    size_t person_count = 0;
    int64_t id2_sum = 0;
    const auto& res_value = res.value().response();
    person_count = res_value.row_count();
    const auto& id2_column = res_value.arrays(0).int64_array();
    for (int64_t i = 0; i < id2_column.values_size(); ++i) {
      id2_sum += id2_column.values(i);
    }
    return {person_count, id2_sum};
  }

  int32_t parallel_execute(std::shared_ptr<Connection> conn,
                           const std::vector<std::string>& queries,
                           int thread_num) {
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};
    for (int i = 0; i < thread_num; ++i) {
      threads.emplace_back([conn, queries, &success_count, i]() {
        bool all_success = true;
        for (const auto& query : queries) {
          // Replace $TXN_ID with thread id
          std::string query_with_id = query;
          size_t pos = query_with_id.find("$TXN_ID");

          while (pos != std::string::npos) {
            query_with_id.replace(pos, 7, std::to_string(i));
            pos = query_with_id.find("$TXN_ID", pos + 1);
          }
          LOG(INFO) << "Executing query: " << query_with_id;
          auto res = conn->Query(query_with_id);
          if (!res) {
            all_success = false;
            LOG(ERROR) << "Query failed: " << res.error().ToString();
            break;
          }
        }
        EXPECT_TRUE(all_success);
        if (all_success) {
          success_count.fetch_add(1);
        }
      });
    }
    for (auto& t : threads) {
      t.join();
    }
    return success_count.load();
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
  auto res3 =
      connections[0]->Query("MATCH(n) where n.id = 1 SET n.name = 'Alice';");
  EXPECT_FALSE(res3);
}

// Test Parallel Execution
TEST_F(ConnectionTest, TestParallelExecutionAtomicity) {
  NeugDB db;
  NeugDBConfig config;
  config.data_dir = DB_DIR;
  config.mode = DBMode::READ_WRITE;
  db.Open(config);

  auto conn = db.Connect();
  EXPECT_NE(conn, nullptr);

  atomicityInit(conn);
  auto committed = atomicityCheck(conn);
  std::vector<std::string> queries;
  queries.push_back("MATCH (n:PERSON {id: 1}) set n.id2 = n.id2 + 1;");
  queries.push_back(
      "CREATE (n1:PERSON {id: $TXN_ID + 3, id2: 1, name: "
      "'NewPerson$TXN_ID', emails: 'newperson$TXN_ID@example.com'});");
  int num_thread = 100;
  int success_count = parallel_execute(conn, queries, num_thread);
  auto finalStatus = atomicityCheck(conn);
  committed.first += success_count;
  committed.second += success_count * 2;
  EXPECT_EQ(success_count, num_thread);
  EXPECT_EQ(committed, finalStatus);
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

  atomicityInit(conn);

  auto res = conn->Query(
      "MATCH (n:PERSON {id: $person_id}) SET n.id2 = n.id2 + $increment;",
      "update",
      {{"person_id", execution::Value::INT64(1)},
       {"increment", execution::Value::INT64(5)}});
  EXPECT_TRUE(res);
  LOG(INFO) << res.value().ToString();
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
}  // namespace test

}  // namespace test

}  // namespace neug
