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
#include <type_traits>
#include <vector>
#include "column_assertions.h"
#include "neug/config.h"
#include "neug/main/connection.h"
#include "neug/main/neug_db.h"
#include "neug/server/neug_db_service.h"
#include "neug/storages/file_names.h"
#include "neug/storages/graph/schema.h"
#include "unittest/utils.h"

namespace {

// ---------------------------------------------------------------------------
// Memory level traits for typed tests.
// ---------------------------------------------------------------------------
template <neug::MemoryLevel kLevel>
struct MemoryLevelTag {
  static constexpr neug::MemoryLevel value = kLevel;
};

using InMemoryLevel = MemoryLevelTag<neug::MemoryLevel::kInMemory>;
using SyncToFileLevel = MemoryLevelTag<neug::MemoryLevel::kSyncToFile>;

using AllMemoryLevels = ::testing::Types<InMemoryLevel, SyncToFileLevel>;

// ---------------------------------------------------------------------------
// Test data constants
// ---------------------------------------------------------------------------
constexpr std::array<int64_t, 4> kPersonIds = {1, 2, 4, 6};
const std::vector<int64_t> kPersonIdValues(kPersonIds.begin(),
                                           kPersonIds.end());
const std::vector<std::string> kPersonNames = {"marko", "vadas", "josh",
                                               "peter"};
constexpr std::array<int64_t, 4> kPersonAges = {29, 27, 32, 35};
const std::vector<int64_t> kPersonAgeValues(kPersonAges.begin(),
                                            kPersonAges.end());

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------
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

// ===========================================================================
// Base fixture — shared DB lifecycle helpers for all checkpoint suites.
// Parameterized by MemoryLevelTag<kLevel> via CRTP so typed tests inherit it.
// ===========================================================================
template <typename MemoryLevelT>
class CheckpointTestBase : public ::testing::Test {
 protected:
  static constexpr neug::MemoryLevel kMemoryLevel = MemoryLevelT::value;

  // -- Config / open helpers (single source of truth) ----------------------

  static neug::NeugDBConfig MakeConfig(const std::string& data_dir) {
    neug::NeugDBConfig config;
    config.data_dir = data_dir;
    config.memory_level = kMemoryLevel;
    config.checkpoint_on_close = true;
    config.compact_on_close = true;
    config.compact_csr = true;
    config.enable_auto_compaction = false;
    return config;
  }

  static void OpenDB(neug::NeugDB& db, const std::string& data_dir) {
    db.Open(MakeConfig(data_dir));
  }

  // -- Directory lifecycle -------------------------------------------------

  static void CleanDir(const std::string& dir) {
    if (std::filesystem::exists(dir)) {
      std::filesystem::remove_all(dir);
    }
  }

  static void EnsureCleanDir(const std::string& dir) {
    CleanDir(dir);
    std::filesystem::create_directories(dir);
  }

  // -- Query helpers (avoid repetitive boilerplate) ------------------------

  static void ExpectQuery(neug::Connection& conn, const std::string& cypher) {
    auto res = conn.Query(cypher);
    EXPECT_TRUE(res) << res.error().ToString();
  }

  static neug::QueryResponse RunQuery(neug::Connection& conn,
                                      const std::string& cypher) {
    auto res = conn.Query(cypher);
    EXPECT_TRUE(res) << res.error().ToString();
    return res.value().response();
  }

  // -- Common assertion combos used across many tests ----------------------

  static void AssertBasicPersonAndKnows(neug::Connection& conn,
                                        const std::vector<double>& weights) {
    AssertPersonVertexBasic(RunQuery(conn, "MATCH (v:person) RETURN v.*;"));
    AssertKnowsWeight(
        RunQuery(conn, "MATCH (v:person)-[e:knows]->(:person) RETURN e.*;"),
        weights);
  }
};

// ===========================================================================
// CheckpointTest — modern-graph based checkpoint scenarios
// ===========================================================================
template <typename T>
class CheckpointTest : public CheckpointTestBase<T> {
 protected:
  static constexpr const char* DB_DIR = "/tmp/checkpoint_test";

  void SetUp() override {
    this->EnsureCleanDir(DB_DIR);
    neug::NeugDB db;
    this->OpenDB(db, DB_DIR);
    auto conn = db.Connect();
    load_modern_graph(conn);
    LOG(INFO) << "[CheckPointTest]: Finished loading modern graph";
    conn->Close();
    db.Close();
  }

  void TearDown() override { this->CleanDir(DB_DIR); }
};

TYPED_TEST_SUITE(CheckpointTest, AllMemoryLevels);

TYPED_TEST(CheckpointTest, basic) {
  neug::NeugDB db;
  this->OpenDB(db, this->DB_DIR);
  db.Close();
  this->OpenDB(db, this->DB_DIR);
  auto conn = db.Connect();
  this->AssertBasicPersonAndKnows(*conn, {0.5, 1.0});
}

TYPED_TEST(CheckpointTest, after_add_vertex_property) {
  neug::NeugDB db;
  this->OpenDB(db, this->DB_DIR);
  auto conn = db.Connect();
  this->ExpectQuery(*conn, "ALTER TABLE person ADD created STRING;");

  db.Close();
  this->OpenDB(db, this->DB_DIR);
  conn = db.Connect();

  AssertPersonVertexWithCreated(
      this->RunQuery(*conn, "MATCH (v:person) RETURN v.*;"), {"", "", "", ""});
  AssertKnowsWeight(
      this->RunQuery(*conn,
                     "MATCH (v:person)-[e:knows]->(:person) RETURN e.*;"),
      {0.5, 1.0});
}

TYPED_TEST(CheckpointTest, after_delete_vertex_property) {
  neug::NeugDB db;
  this->OpenDB(db, this->DB_DIR);
  auto conn = db.Connect();
  this->ExpectQuery(*conn, "ALTER TABLE person DROP age;");

  db.Close();
  this->OpenDB(db, this->DB_DIR);
  conn = db.Connect();

  AssertPersonVertexWithoutAge(
      this->RunQuery(*conn, "MATCH (v:person) RETURN v.*;"));
  AssertKnowsWeight(
      this->RunQuery(*conn,
                     "MATCH (v:person)-[e:knows]->(:person) RETURN e.*;"),
      {0.5, 1.0});
}

TYPED_TEST(CheckpointTest, after_delete_vertex) {
  neug::NeugDB db;
  this->OpenDB(db, this->DB_DIR);
  auto conn = db.Connect();
  this->ExpectQuery(*conn, "MATCH (v:person) WHERE v.id = 1 DELETE v;");

  db.Close();
  this->OpenDB(db, this->DB_DIR);
  conn = db.Connect();

  AssertPersonVertexAfterDelete(
      this->RunQuery(*conn, "MATCH (v:person) RETURN v.*;"));
  AssertKnowsWeight(
      this->RunQuery(*conn,
                     "MATCH (v:person)-[e:knows]->(:person) RETURN e.*;"),
      {});
}

TYPED_TEST(CheckpointTest, after_add_edge_property1) {
  neug::NeugDB db;
  this->OpenDB(db, this->DB_DIR);
  auto conn = db.Connect();
  this->ExpectQuery(*conn, "ALTER TABLE knows ADD registration DATE;");

  AssertKnowsWeightAndRegistration(
      this->RunQuery(*conn,
                     "MATCH (v:person)-[e:knows]->(:person) RETURN e.*;"),
      {0.5, 1.0}, {0, 0});

  db.Close();
  this->OpenDB(db, this->DB_DIR);
  conn = db.Connect();

  AssertPersonVertexBasic(
      this->RunQuery(*conn, "MATCH (v:person) RETURN v.*;"));
  AssertKnowsWeightAndRegistration(
      this->RunQuery(*conn,
                     "MATCH (v:person)-[e:knows]->(:person) RETURN e.*;"),
      {0.5, 1.0}, {0, 0});
}

TYPED_TEST(CheckpointTest, after_add_edge_property2) {
  neug::NeugDB db;
  this->OpenDB(db, this->DB_DIR);
  auto conn = db.Connect();

  AssertPersonVertexBasic(
      this->RunQuery(*conn, "MATCH (v:person) RETURN v.*;"));
  this->ExpectQuery(*conn, "ALTER TABLE knows ADD description STRING;");

  db.Close();
  this->OpenDB(db, this->DB_DIR);
  conn = db.Connect();

  AssertKnowsWeightAndDescription(
      this->RunQuery(*conn,
                     "MATCH (v:person)-[e:knows]->(:person) RETURN e.*;"),
      {0.5, 1.0}, {"", ""});

  this->ExpectQuery(*conn, "ALTER TABLE knows ADD date DATE;");

  db.Close();
  this->OpenDB(db, this->DB_DIR);
  conn = db.Connect();

  AssertPersonVertexBasic(
      this->RunQuery(*conn, "MATCH (v:person) RETURN v.*;"));
  AssertKnowsFullSchema(
      this->RunQuery(*conn,
                     "MATCH (v:person)-[e:knows]->(:person) RETURN e.*;"),
      {0.5, 1.0}, {"", ""}, {0, 0});
}

TYPED_TEST(CheckpointTest, after_delete_edge_property) {
  neug::NeugDB db;
  this->OpenDB(db, this->DB_DIR);
  {
    auto conn = db.Connect();
    this->ExpectQuery(*conn, "ALTER TABLE knows DROP weight");
  }

  db.Close();
  this->OpenDB(db, this->DB_DIR);
  auto conn = db.Connect();

  AssertPersonVertexBasic(
      this->RunQuery(*conn, "MATCH (v:person) RETURN v.*;"));
  AssertMapColumn(
      this->RunQuery(*conn, "MATCH (v:person)-[e:knows]->(:person) RETURN e;"),
      2);
}

TYPED_TEST(CheckpointTest, after_delete_edge) {
  neug::NeugDB db;
  this->OpenDB(db, this->DB_DIR);
  {
    auto conn = db.Connect();
    this->ExpectQuery(
        *conn,
        "MATCH (v:person)-[e:knows]->(:person) WHERE v.id = 1 DELETE e;");
  }

  db.Close();
  this->OpenDB(db, this->DB_DIR);
  auto conn = db.Connect();

  AssertPersonVertexBasic(
      this->RunQuery(*conn, "MATCH (v:person) RETURN v.*;"));
  AssertKnowsWeight(
      this->RunQuery(*conn,
                     "MATCH (:person)-[e:knows]->(v:person) RETURN e.*;"),
      {});
}

TYPED_TEST(CheckpointTest, compact) {
  std::string db_path = "/tmp/test_compact_db";
  this->EnsureCleanDir(db_path);

  {
    neug::NeugDB db;
    this->OpenDB(db, db_path);
    auto conn = db.Connect();
    load_modern_graph(conn);
    conn->Close();
    auto svc = std::make_shared<neug::NeugDBService>(db);
    auto sess = svc->AcquireSession();
    sess->GetCompactTransaction().Commit();
    db.Close();
  }

  neug::NeugDB db2;
  this->OpenDB(db2, db_path);
  auto svc = std::make_shared<neug::NeugDBService>(db2);
  auto conn2 = db2.Connect();

  AssertSingleInt64Result(
      this->RunQuery(*conn2, "MATCH (v:person) RETURN COUNT(v);"), 4);
  this->ExpectQuery(*conn2, "MATCH (v:person) WHERE v.id <= 2 DELETE v;");

  svc->AcquireSession()->GetCompactTransaction().Commit();

  AssertSingleInt64Result(
      this->RunQuery(*conn2, "MATCH (v:person) RETURN COUNT(v);"), 2);
  AssertSingleInt64Result(
      this->RunQuery(*conn2,
                     "MATCH (v:person)-[e:knows]->(:person) RETURN count(e);"),
      0);

  conn2->Close();
  db2.Close();
  this->CleanDir(db_path);
}

TYPED_TEST(CheckpointTest, recover_from_checkpoint) {
  neug::NeugDB db;
  this->OpenDB(db, this->DB_DIR);
  auto conn = db.Connect();

  AssertSingleInt64Result(
      this->RunQuery(*conn, "MATCH (v:person) RETURN COUNT(v);"), 4);
  AssertSingleInt64Result(
      this->RunQuery(*conn, "MATCH (v)-[e]->(a) RETURN COUNT(e);"), 6);
  AssertCreatedEdgesSnapshotResult(
      this->RunQuery(*conn,
                     "MATCH (v:person)-[e:created]->(f:software) return v.id, "
                     "e.since, f.id;"),
      {1, 4, 4, 6}, {2020, 2022, 2021, 2023}, {3, 3, 5, 3});

  this->ExpectQuery(*conn, "MATCH (v:person) WHERE v.id = 1 DELETE v;");
  this->ExpectQuery(
      *conn,
      "MATCH (v:person)-[e:created]->(f:software) WHERE v.id > 4 DELETE e;");
  conn->Close();
  db.Close();

  neug::NeugDB db2;
  this->OpenDB(db2, this->DB_DIR);
  auto conn2 = db2.Connect();
  AssertSingleInt64Result(
      this->RunQuery(*conn2, "MATCH (v:person) RETURN COUNT(v);"), 3);
  AssertSingleInt64Result(
      this->RunQuery(*conn2, "MATCH (v)-[e]->(a) RETURN COUNT(e);"), 2);
}

TYPED_TEST(CheckpointTest, checkpoint_with_string_edge_prop) {
  std::string db_path = "/tmp/test_checkpoint_string_edge_prop_db";
  this->EnsureCleanDir(db_path);
  {
    neug::NeugDB db;
    this->OpenDB(db, db_path);
    auto conn = db.Connect();
    this->ExpectQuery(*conn,
                      "CREATE NODE TABLE A (id STRING, PRIMARY KEY(id));");
    this->ExpectQuery(*conn,
                      "CREATE NODE TABLE B (id STRING, PRIMARY KEY(id));");
    this->ExpectQuery(*conn, "CREATE REL TABLE R (FROM A TO B, prop STRING);");
    this->ExpectQuery(*conn, "CREATE (a:A {id: 'a1'})");
    this->ExpectQuery(*conn, "CREATE (b:B {id: 'b1'})");
    this->ExpectQuery(*conn, "CHECKPOINT;");
    this->ExpectQuery(
        *conn,
        "MATCH (a:A {id: 'a1'}), (b:B {id: 'b1'}) CREATE (a)-[:R {prop: "
        "'hello'}]->(b)");
    conn->Close();
    db.Close();
  }
  this->CleanDir(db_path);
}

// ===========================================================================
// DropTableCheckpointTest — DROP TABLE checkpoint correctness
// ===========================================================================
template <typename T>
class DropTableCheckpointTest : public CheckpointTestBase<T> {
 protected:
  static constexpr const char* DB_DIR = "/tmp/drop_table_checkpoint_test";

  void SetUp() override { this->EnsureCleanDir(DB_DIR); }
  void TearDown() override { this->CleanDir(DB_DIR); }

  void CreateAndCheckpointPerson() {
    neug::NeugDB db;
    this->OpenDB(db, DB_DIR);
    auto conn = db.Connect();
    this->ExpectQuery(*conn,
                      "CREATE NODE TABLE IF NOT EXISTS Person"
                      "(id STRING, PRIMARY KEY(id));");
    this->ExpectQuery(*conn, "CREATE (p:Person {id: 'alice'});");
    this->ExpectQuery(*conn, "CHECKPOINT;");
    auto table = this->RunQuery(*conn, "MATCH (p:Person) RETURN p.id;");
    ASSERT_EQ(table.row_count(), 1);
    conn->Close();
    db.Close();
  }

  // Creates Person + Software vertex tables and a Created edge table,
  // inserts sample data, and checkpoints.
  void CreateGraphWithEdgesAndCheckpoint() {
    neug::NeugDB db;
    this->OpenDB(db, DB_DIR);
    auto conn = db.Connect();
    this->ExpectQuery(*conn,
                      "CREATE NODE TABLE IF NOT EXISTS Person"
                      "(id STRING, PRIMARY KEY(id));");
    this->ExpectQuery(*conn,
                      "CREATE NODE TABLE IF NOT EXISTS Software"
                      "(id STRING, PRIMARY KEY(id));");
    this->ExpectQuery(*conn,
                      "CREATE REL TABLE IF NOT EXISTS Created"
                      "(FROM Person TO Software, weight DOUBLE);");
    this->ExpectQuery(*conn, "CREATE (p:Person {id: 'alice'});");
    this->ExpectQuery(*conn, "CREATE (s:Software {id: 'neug'});");
    this->ExpectQuery(
        *conn,
        "MATCH (p:Person {id: 'alice'}), (s:Software {id: 'neug'}) "
        "CREATE (p)-[:Created {weight: 1.0}]->(s);");
    this->ExpectQuery(*conn, "CHECKPOINT;");

    AssertSingleInt64Result(
        this->RunQuery(
            *conn,
            "MATCH (p:Person)-[e:Created]->(s:Software) RETURN count(e);"),
        1);
    conn->Close();
    db.Close();
  }

  void ReopenAndVerifyPersonEmpty() {
    neug::NeugDB db;
    this->OpenDB(db, DB_DIR);
    auto conn = db.Connect();
    this->ExpectQuery(*conn,
                      "CREATE NODE TABLE IF NOT EXISTS Person"
                      "(id STRING, PRIMARY KEY(id));");
    auto table = this->RunQuery(*conn, "MATCH (p:Person) RETURN p.id;");
    EXPECT_EQ(table.row_count(), 0);
    conn->Close();
    db.Close();
  }
};

TYPED_TEST_SUITE(DropTableCheckpointTest, AllMemoryLevels);

TYPED_TEST(DropTableCheckpointTest, drop_and_recreate_clears_stale_data) {
  this->CreateAndCheckpointPerson();

  neug::NeugDB db;
  this->OpenDB(db, this->DB_DIR);
  auto conn = db.Connect();

  this->ExpectQuery(*conn, "DROP TABLE IF EXISTS Person;");
  this->ExpectQuery(
      *conn,
      "CREATE NODE TABLE IF NOT EXISTS Person(id STRING, PRIMARY KEY(id));");

  auto table = this->RunQuery(*conn, "MATCH (p:Person) RETURN p.id;");
  EXPECT_EQ(table.row_count(), 0)
      << "Stale data visible after DROP TABLE + re-CREATE";

  this->ExpectQuery(*conn, "CREATE (p:Person {id: 'bob'});");
  auto table2 = this->RunQuery(*conn, "MATCH (p:Person) RETURN p.id;");
  EXPECT_EQ(table2.row_count(), 1)
      << "Expected only 'bob', but stale data may be present";
  neug::test::AssertStringColumn(table2, 0, {"bob"});

  conn->Close();
  db.Close();
}

TYPED_TEST(DropTableCheckpointTest, checkpoint_after_drop_succeeds) {
  this->CreateAndCheckpointPerson();

  {
    neug::NeugDB db;
    this->OpenDB(db, this->DB_DIR);
    auto conn = db.Connect();
    this->ExpectQuery(*conn, "DROP TABLE IF EXISTS Person;");
    this->ExpectQuery(*conn, "CHECKPOINT;");
    conn->Close();
    db.Close();
  }

  this->ReopenAndVerifyPersonEmpty();
}

TYPED_TEST(DropTableCheckpointTest,
           drop_checkpoint_reopen_recreate_has_no_stale_data) {
  this->CreateAndCheckpointPerson();

  {
    neug::NeugDB db;
    this->OpenDB(db, this->DB_DIR);
    auto conn = db.Connect();
    this->ExpectQuery(*conn, "DROP TABLE IF EXISTS Person;");
    this->ExpectQuery(*conn, "CHECKPOINT;");
    conn->Close();
    db.Close();
  }

  this->ReopenAndVerifyPersonEmpty();
}

TYPED_TEST(DropTableCheckpointTest, drop_edge_table_and_checkpoint) {
  this->CreateGraphWithEdgesAndCheckpoint();

  {
    neug::NeugDB db;
    this->OpenDB(db, this->DB_DIR);
    auto conn = db.Connect();

    // Drop the edge table while keeping vertex tables intact
    this->ExpectQuery(*conn, "DROP TABLE IF EXISTS Created;");
    this->ExpectQuery(*conn, "CHECKPOINT;");

    conn->Close();
    db.Close();
  }

  // Reopen: vertex tables should survive, edge table should be gone
  {
    neug::NeugDB db;
    this->OpenDB(db, this->DB_DIR);
    auto conn = db.Connect();

    // Vertices are still present
    auto person_table =
        this->RunQuery(*conn, "MATCH (p:Person) RETURN p.id;");
    EXPECT_EQ(person_table.row_count(), 1);

    auto software_table =
        this->RunQuery(*conn, "MATCH (s:Software) RETURN s.id;");
    EXPECT_EQ(software_table.row_count(), 1);

    // Re-create the edge table — it should be empty
    this->ExpectQuery(*conn,
                      "CREATE REL TABLE IF NOT EXISTS Created"
                      "(FROM Person TO Software, weight DOUBLE);");
    AssertSingleInt64Result(
        this->RunQuery(
            *conn,
            "MATCH (p:Person)-[e:Created]->(s:Software) RETURN count(e);"),
        0);

    conn->Close();
    db.Close();
  }
}

TYPED_TEST(DropTableCheckpointTest,
           drop_edge_table_and_recreate_clears_stale_data) {
  this->CreateGraphWithEdgesAndCheckpoint();

  neug::NeugDB db;
  this->OpenDB(db, this->DB_DIR);
  auto conn = db.Connect();

  // Drop + re-create in the same session
  this->ExpectQuery(*conn, "DROP TABLE IF EXISTS Created;");
  this->ExpectQuery(*conn,
                    "CREATE REL TABLE IF NOT EXISTS Created"
                    "(FROM Person TO Software, weight DOUBLE);");

  // Old edge data must not be visible
  AssertSingleInt64Result(
      this->RunQuery(
          *conn,
          "MATCH (p:Person)-[e:Created]->(s:Software) RETURN count(e);"),
      0);

  // Insert fresh edge — only this one should appear
  this->ExpectQuery(
      *conn,
      "MATCH (p:Person {id: 'alice'}), (s:Software {id: 'neug'}) "
      "CREATE (p)-[:Created {weight: 2.0}]->(s);");
  AssertSingleInt64Result(
      this->RunQuery(
          *conn,
          "MATCH (p:Person)-[e:Created]->(s:Software) RETURN count(e);"),
      1);

  conn->Close();
  db.Close();
}

TYPED_TEST(DropTableCheckpointTest,
           drop_edge_table_checkpoint_reopen_recreate_has_no_stale_data) {
  this->CreateGraphWithEdgesAndCheckpoint();

  // Drop edge table + checkpoint
  {
    neug::NeugDB db;
    this->OpenDB(db, this->DB_DIR);
    auto conn = db.Connect();
    this->ExpectQuery(*conn, "DROP TABLE IF EXISTS Created;");
    this->ExpectQuery(*conn, "CHECKPOINT;");
    conn->Close();
    db.Close();
  }

  // Reopen, re-create edge table, verify empty
  {
    neug::NeugDB db;
    this->OpenDB(db, this->DB_DIR);
    auto conn = db.Connect();
    this->ExpectQuery(*conn,
                      "CREATE REL TABLE IF NOT EXISTS Created"
                      "(FROM Person TO Software, weight DOUBLE);");
    AssertSingleInt64Result(
        this->RunQuery(
            *conn,
            "MATCH (p:Person)-[e:Created]->(s:Software) RETURN count(e);"),
        0);
    conn->Close();
    db.Close();
  }
}

}  // namespace test
}  // namespace neug
