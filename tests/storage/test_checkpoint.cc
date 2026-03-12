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
#include <array>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include "column_assertions.h"
#include "neug/main/connection.h"
#include "neug/main/neug_db.h"
#include "neug/server/neug_db_service.h"
#include "neug/storages/file_names.h"
#include "neug/storages/graph/schema.h"
#include "unittest/utils.h"

namespace {

constexpr std::array<int64_t, 4> kPersonIds = {1, 2, 4, 6};
const std::vector<int64_t> kPersonIdValues(kPersonIds.begin(),
                                           kPersonIds.end());
const std::vector<std::string> kPersonNames = {"marko", "vadas", "josh",
                                               "peter"};
constexpr std::array<int64_t, 4> kPersonAges = {29, 27, 32, 35};
const std::vector<int64_t> kPersonAgeValues(kPersonAges.begin(),
                                            kPersonAges.end());

void AssertPersonVertexBasic(const neug::QueryResponse& table) {
  ASSERT_EQ(table.row_count(), 4);
  ASSERT_EQ(table.arrays_size(), 3);
  neug::test::AssertInt64Column(table, 0, kPersonIdValues);
  neug::test::AssertStringColumn(table, 1, kPersonNames);
  neug::test::AssertInt64Column(table, 2, kPersonAgeValues);
}

void AssertPersonVertexWithCreated(
    const neug::QueryResponse& table,
    const std::vector<std::string>& created_values) {
  ASSERT_EQ(table.row_count(), 4);
  ASSERT_EQ(table.arrays_size(), 4);
  neug::test::AssertInt64Column(table, 0, kPersonIdValues);
  neug::test::AssertStringColumn(table, 1, kPersonNames);
  neug::test::AssertInt64Column(table, 2, kPersonAgeValues);
  neug::test::AssertStringColumn(table, 3, created_values);
}

void AssertPersonVertexWithoutAge(const neug::QueryResponse& table) {
  ASSERT_EQ(table.row_count(), 4);
  ASSERT_EQ(table.arrays_size(), 2);
  neug::test::AssertInt64Column(table, 0, kPersonIdValues);
  neug::test::AssertStringColumn(table, 1, kPersonNames);
}

void AssertPersonVertexAfterDelete(const neug::QueryResponse& table) {
  ASSERT_EQ(table.row_count(), 3);
  ASSERT_EQ(table.arrays_size(), 3);
  neug::test::AssertInt64Column(table, 0, {2, 4, 6});
  neug::test::AssertStringColumn(table, 1, {"vadas", "josh", "peter"});
  neug::test::AssertInt64Column(table, 2, {27, 32, 35});
}

void AssertKnowsWeight(const neug::QueryResponse& table,
                       const std::vector<double>& weights) {
  ASSERT_EQ(table.arrays_size(), 1);
  neug::test::AssertDoubleColumn(table, 0, weights);
}

void AssertKnowsWeightAndRegistration(
    const neug::QueryResponse& table, const std::vector<double>& weights,
    const std::vector<int64_t>& registrations) {
  ASSERT_EQ(table.arrays_size(), 2);
  neug::test::AssertDoubleColumn(table, 0, weights);
  neug::test::AssertDate32Column(table, 1, registrations);
}

void AssertKnowsWeightAndDescription(
    const neug::QueryResponse& table, const std::vector<double>& weights,
    const std::vector<std::string>& descriptions) {
  ASSERT_EQ(table.arrays_size(), 2);
  neug::test::AssertDoubleColumn(table, 0, weights);
  neug::test::AssertStringColumn(table, 1, descriptions);
}

void AssertKnowsFullSchema(const neug::QueryResponse& table,
                           const std::vector<double>& weights,
                           const std::vector<std::string>& descriptions,
                           const std::vector<int64_t>& dates) {
  ASSERT_EQ(table.arrays_size(), 3);
  neug::test::AssertDoubleColumn(table, 0, weights);
  neug::test::AssertStringColumn(table, 1, descriptions);
  neug::test::AssertDate32Column(table, 2, dates);
}

void AssertMapColumn(const neug::QueryResponse& table, int64_t expected_rows) {
  ASSERT_EQ(table.arrays_size(), 1);
  ASSERT_EQ(table.row_count(), expected_rows);
  auto array = table.arrays(0);
  ASSERT_TRUE(array.has_vertex_array() || array.has_edge_array() ||
              array.has_path_array());
}

void AssertSingleInt64Result(const neug::QueryResponse& table,
                             int64_t expected) {
  ASSERT_EQ(table.arrays_size(), 1);
  ASSERT_EQ(table.row_count(), 1);
  neug::test::AssertInt64Column(table, 0, {expected});
}

void AssertCreatedEdgesSnapshotResult(
    const neug::QueryResponse& table, const std::vector<int64_t>& ids,
    const std::vector<int64_t>& since,
    const std::vector<int64_t>& software_ids) {
  ASSERT_EQ(table.arrays_size(), 3);
  ASSERT_EQ(table.row_count(), ids.size());
  neug::test::AssertInt64Column(table, 0, ids);
  neug::test::AssertInt64Column(table, 1, since);
  neug::test::AssertInt64Column(table, 2, software_ids);
}
}  // namespace

namespace neug {

namespace test {

class CheckpointTest : public ::testing::Test {
 protected:
  static constexpr const char* DB_DIR = "/tmp/checkpoint_test";

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
    LOG(INFO) << "[CheckPointTest]: Finished loading modern graph";
    conn->Close();
    db_->Close();
    db_.reset();
  }

  void TearDown() override {}
};

TEST_F(CheckpointTest, test_basic) {
  std::unique_ptr<neug::NeugDB> db_ = std::make_unique<neug::NeugDB>();
  db_->Open(DB_DIR);
  auto conn = db_->Connect();

  db_->Close();
  db_->Open(DB_DIR);

  conn = db_->Connect();
  {
    auto res = conn->Query("MATCH (v:person) RETURN v.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    const auto& table = res.value().response();
    AssertPersonVertexBasic(table);
  }
  {
    auto res = conn->Query("MATCH (v:person)-[e:knows]->(:person) RETURN e.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    const auto& table = res.value().response();
    AssertKnowsWeight(table, {0.5, 1.0});
  }
}

TEST_F(CheckpointTest, test_after_add_vertex_property) {
  neug::NeugDB db;
  db.Open(DB_DIR);
  auto conn = db.Connect();

  {
    auto res = conn->Query("ALTER TABLE person ADD created STRING;");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  db.Close();
  db.Open(DB_DIR);

  conn = db.Connect();
  {
    auto res = conn->Query("MATCH (v:person) RETURN v.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    const auto& table = res.value().response();
    AssertPersonVertexWithCreated(table, {"", "", "", ""});
  }
  {
    auto res = conn->Query("MATCH (v:person)-[e:knows]->(:person) RETURN e.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    const auto& table = res.value().response();
    AssertKnowsWeight(table, {0.5, 1.0});
  }
}

TEST_F(CheckpointTest, test_after_delete_vertex_property) {
  neug::NeugDB db;
  db.Open(DB_DIR);
  auto conn = db.Connect();

  {
    auto res = conn->Query("ALTER TABLE person DROP age;");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  db.Close();
  db.Open(DB_DIR);
  conn = db.Connect();

  {
    auto res = conn->Query("MATCH (v:person) RETURN v.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    const auto& table = res.value().response();
    AssertPersonVertexWithoutAge(table);
  }
  {
    auto res = conn->Query("MATCH (v:person)-[e:knows]->(:person) RETURN e.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    const auto& table = res.value().response();
    AssertKnowsWeight(table, {0.5, 1.0});
  }
}

TEST_F(CheckpointTest, test_after_delete_vertex) {
  neug::NeugDB db;
  db.Open(DB_DIR);
  auto conn = db.Connect();

  {
    auto res = conn->Query("MATCH (v:person) WHERE v.id = 1 DELETE v;");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  db.Close();
  db.Open(DB_DIR);
  conn = db.Connect();

  {
    auto res = conn->Query("MATCH (v:person) RETURN v.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    const auto& table = res.value().response();
    AssertPersonVertexAfterDelete(table);
  }

  {
    auto res = conn->Query("MATCH (v:person)-[e:knows]->(:person) RETURN e.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    const auto& table = res.value().response();
    AssertKnowsWeight(table, {});
  }
}

TEST_F(CheckpointTest, test_after_add_edge_property1) {
  neug::NeugDB db;
  db.Open(DB_DIR);
  auto conn = db.Connect();

  {
    auto res = conn->Query("ALTER TABLE knows ADD registration DATE;");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  {
    auto res = conn->Query("MATCH (v:person)-[e:knows]->(:person) RETURN e.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    const auto& table = res.value().response();
    AssertKnowsWeightAndRegistration(table, {0.5, 1.0}, {0, 0});
  }

  db.Close();
  db.Open(DB_DIR);

  conn = db.Connect();

  {
    auto res = conn->Query("MATCH (v:person) RETURN v.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    const auto& table = res.value().response();
    AssertPersonVertexBasic(table);
  }

  {
    auto res = conn->Query("MATCH (v:person)-[e:knows]->(:person) RETURN e.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    const auto& table = res.value().response();
    AssertKnowsWeightAndRegistration(table, {0.5, 1.0}, {0, 0});
  }
}

// Add a test test_after_add_edge_property2 which add property to an edge
// table which already have 2 properties
TEST_F(CheckpointTest, test_after_add_edge_property2) {
  neug::NeugDB db;
  db.Open(DB_DIR);
  auto conn = db.Connect();

  {
    auto res = conn->Query("MATCH (v:person) RETURN v.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    const auto& table = res.value().response();
    AssertPersonVertexBasic(table);
  }
  {
    auto res = conn->Query("ALTER TABLE knows ADD description STRING;");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  db.Close();
  db.Open(DB_DIR);

  conn = db.Connect();
  {
    auto res = conn->Query("MATCH (v:person)-[e:knows]->(:person) RETURN e.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    const auto& table = res.value().response();
    AssertKnowsWeightAndDescription(table, {0.5, 1.0}, {"", ""});
  }

  {
    // Add another edge to check if the new property is added correctly
    auto res = conn->Query("ALTER TABLE knows ADD date DATE;");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  db.Close();
  db.Open(DB_DIR);

  conn = db.Connect();

  {
    auto res = conn->Query("MATCH (v:person) RETURN v.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    const auto& table = res.value().response();
    AssertPersonVertexBasic(table);
  }

  {
    auto res = conn->Query("MATCH (v:person)-[e:knows]->(:person) RETURN e.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    const auto& table = res.value().response();
    AssertKnowsFullSchema(table, {0.5, 1.0}, {"", ""}, {0, 0});
  }
}

TEST_F(CheckpointTest, test_after_delete_edge_property) {
  neug::NeugDB db;
  db.Open(DB_DIR);

  {
    auto conn = db.Connect();
    auto res = conn->Query("ALTER TABLE knows DROP weight");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  db.Close();
  db.Open(DB_DIR);
  auto conn = db.Connect();

  {
    auto res = conn->Query("MATCH (v:person) RETURN v.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    const auto& table = res.value().response();
    AssertPersonVertexBasic(table);
  }

  {
    auto res = conn->Query("MATCH (v:person)-[e:knows]->(:person) RETURN e;");
    EXPECT_TRUE(res) << res.error().ToString();
    const auto& table = res.value().response();
    AssertMapColumn(table, 2);
  }
}

TEST_F(CheckpointTest, test_after_delete_edge) {
  neug::NeugDB db;
  db.Open(DB_DIR);

  {
    auto conn = db.Connect();
    auto res = conn->Query(
        "MATCH (v:person)-[e:knows]->(:person) WHERE v.id = 1 DELETE e;");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  db.Close();
  db.Open(DB_DIR);
  auto conn = db.Connect();

  {
    auto res = conn->Query("MATCH (v:person) RETURN v.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    const auto& table = res.value().response();
    AssertPersonVertexBasic(table);
  }
  {
    auto res = conn->Query("MATCH (:person)-[e:knows]->(v:person) RETURN e.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    const auto& table = res.value().response();
    AssertKnowsWeight(table, {});
  }
}

TEST_F(CheckpointTest, test_compact) {
  std::string db_path = "/tmp/test_compact_db";
  if (std::filesystem::exists(db_path)) {
    std::filesystem::remove_all(db_path);
  }
  std::filesystem::create_directories(db_path);
  {
    neug::NeugDB db;
    db.Open(db_path);
    auto conn = db.Connect();
    load_modern_graph(conn);
    conn->Close();
    auto svc = std::make_shared<neug::NeugDBService>(db);

    auto sess = svc->AcquireSession();
    auto compact_txn = sess->GetCompactTransaction();
    compact_txn.Commit();

    db.Close();
  }

  neug::NeugDB db2;
  db2.Open(db_path);
  auto svc = std::make_shared<neug::NeugDBService>(db2);
  auto conn2 = db2.Connect();
  {
    auto res = conn2->Query("MATCH (v:person) RETURN COUNT(v);");
    EXPECT_TRUE(res) << res.error().ToString();
    AssertSingleInt64Result(res.value().response(), 4);
  }

  {
    // Delete half vertices
    auto res = conn2->Query("MATCH (v:person) WHERE v.id <= 2 DELETE v;");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  auto sess2 = svc->AcquireSession();
  auto compact_txn2 = sess2->GetCompactTransaction();
  compact_txn2.Commit();

  {
    auto res = conn2->Query("MATCH (v:person) RETURN COUNT(v);");
    EXPECT_TRUE(res) << res.error().ToString();
    AssertSingleInt64Result(res.value().response(), 2);
  }

  {
    auto res =
        conn2->Query("MATCH (v:person)-[e:knows]->(:person) RETURN count(e);");
    EXPECT_TRUE(res) << res.error().ToString();
    AssertSingleInt64Result(res.value().response(), 0);
  }
  conn2->Close();
  db2.Close();
}

TEST_F(CheckpointTest, test_recover_from_checkpoint) {
  neug::NeugDB db;
  db.Open(DB_DIR);
  auto conn = db.Connect();

  {
    auto res = conn->Query("MATCH (v:person) RETURN COUNT(v);");
    EXPECT_TRUE(res) << res.error().ToString();
    AssertSingleInt64Result(res.value().response(), 4);
  }
  {
    auto res = conn->Query("MATCH (v)-[e]->(a) RETURN COUNT(e);");
    EXPECT_TRUE(res) << res.error().ToString();
    AssertSingleInt64Result(res.value().response(), 6);
  }

  {
    auto res = conn->Query(
        "MATCH (v:person)-[e:created]->(f:software) return v.id, e.since, "
        "f.id;");
    EXPECT_TRUE(res) << res.error().ToString();
    AssertCreatedEdgesSnapshotResult(res.value().response(), {1, 4, 4, 6},
                                     {2020, 2022, 2021, 2023}, {3, 3, 5, 3});
  }

  EXPECT_TRUE(conn->Query("MATCH (v:person) WHERE v.id = 1 DELETE v;"));
  EXPECT_TRUE(
      conn->Query("MATCH (v:person)-[e:created]->(f:software) WHERE "
                  "v.id > 4 DELETE e;"));
  auto res = conn->Query(
      "MATCH (v:person)-[e:created]->(f:software) return v.id, e.since, "
      "f.id;");
  EXPECT_TRUE(res) << res.error().ToString();
  conn->Close();
  db.Close();

  // Reopen the db, should recover from checkpoint
  neug::NeugDB db2;
  db2.Open(DB_DIR);
  auto conn2 = db2.Connect();
  {
    auto res = conn2->Query("MATCH (v:person) RETURN COUNT(v);");
    EXPECT_TRUE(res) << res.error().ToString();
    AssertSingleInt64Result(res.value().response(), 3);
  }
  {
    auto res = conn2->Query("MATCH (v)-[e]->(a) RETURN COUNT(e);");
    EXPECT_TRUE(res) << res.error().ToString();
    AssertSingleInt64Result(res.value().response(), 2);
  }
}

TEST(CheckpointTestStringProp, test_checkpoint_with_string_edge_prop) {
  std::string db_path = "/tmp/test_checkpoint_string_edge_prop_db";
  if (std::filesystem::exists(db_path)) {
    std::filesystem::remove_all(db_path);
  }
  std::filesystem::create_directories(db_path);
  {
    neug::NeugDB db;
    db.Open(db_path);
    auto conn = db.Connect();
    EXPECT_TRUE(
        conn->Query("CREATE NODE TABLE A (id STRING, PRIMARY KEY(id));"));
    EXPECT_TRUE(
        conn->Query("CREATE NODE TABLE B (id STRING, PRIMARY KEY(id));"));
    EXPECT_TRUE(conn->Query("CREATE REL TABLE R (FROM A TO B, prop STRING);"));
    EXPECT_TRUE(conn->Query("CREATE (a:A {id: 'a1'})"));
    EXPECT_TRUE(conn->Query("CREATE (b:B {id: 'b1'})"));
    EXPECT_TRUE(conn->Query("CHECKPOINT;"));

    auto ret = conn->Query(
        "MATCH (a:A {id: 'a1'}), (b:B {id: 'b1'}) CREATE (a)-[:R {prop: "
        "'hello'}]->(b)");
    EXPECT_TRUE(ret) << ret.error().ToString();

    conn->Close();
    db.Close();
  }
}

}  // namespace test

}  // namespace neug
