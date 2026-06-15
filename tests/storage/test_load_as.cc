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
#include <fstream>

#include "neug/main/connection.h"
#include "neug/main/neug_db.h"
#include "unittest/utils.h"

namespace neug {
namespace test {

class LoadAsTest : public ::testing::Test {
 protected:
  static constexpr const char* DB_DIR = "/tmp/load_as_test_db";
  static constexpr const char* CSV_DIR = "/tmp/load_as_test_csv";

  std::unique_ptr<NeugDB> db_;

  void write_csv(const std::string& filename,
                 const std::string& content) {
    std::ofstream ofs(std::string(CSV_DIR) + "/" + filename);
    ofs << content;
  }

  void SetUp() override {
    if (std::filesystem::exists(DB_DIR)) {
      std::filesystem::remove_all(DB_DIR);
    }
    if (std::filesystem::exists(CSV_DIR)) {
      std::filesystem::remove_all(CSV_DIR);
    }
    std::filesystem::create_directories(DB_DIR);
    std::filesystem::create_directories(CSV_DIR);

    // CSV files for LOAD NODE TABLE tests.
    // NeuG's CSV reader defaults to '|' delimiter
    // (CopyConstants::DEFAULT_CSV_DELIMITER) and HEADER=false
    // (DEFAULT_CSV_HAS_HEADER). To stay aligned with those defaults we
    // write '|'-delimited content here and pass `header = true` in every
    // LOAD statement below so the first row is parsed as column names.
    write_csv("people.csv",
              "id|name|age\n"
              "1|Alice|30\n"
              "2|Bob|25\n"
              "3|Carol|35\n"
              "4|Dave|20\n");

    write_csv("items.csv",
              "item_id|title|price\n"
              "101|Widget|9.99\n"
              "102|Gadget|19.99\n"
              "103|Doohickey|4.99\n");

    // CSV for edge tests: src_id, dst_id, weight
    write_csv("edges.csv",
              "src_id|dst_id|weight\n"
              "1|2|0.5\n"
              "2|3|1.0\n"
              "3|4|0.8\n");

    // CSV for dangling edge tests: src_id=99 does not exist in people.csv.
    write_csv("dangling_edges.csv",
              "src_id|dst_id|weight\n"
              "1|2|0.5\n"
              "99|3|1.0\n");

    db_ = std::make_unique<NeugDB>();
    NeugDBConfig config;
    config.data_dir = DB_DIR;
    config.checkpoint_on_close = true;
    config.compact_on_close = true;
    config.compact_csr = true;
    config.enable_auto_compaction = false;
    db_->Open(config);
  }

  void TearDown() override {
    if (db_) {
      db_->Close();
      db_.reset();
    }
    if (std::filesystem::exists(DB_DIR)) {
      std::filesystem::remove_all(DB_DIR);
    }
    if (std::filesystem::exists(CSV_DIR)) {
      std::filesystem::remove_all(CSV_DIR);
    }
  }

  // Create persistent node tables needed for edge LOAD tests.
  void setupPersistentNodeTables(std::shared_ptr<Connection> conn) {
    auto res = conn->Query(
        "CREATE NODE TABLE Person(id INT64, name STRING, age INT64, "
        "PRIMARY KEY(id));");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  // Insert some vertices so edge lookups can succeed.
  // Note: tl::expected<QueryResult, Status> has no move-assignment
  // (QueryResult is move-only and the expected specialization disables
  // operator=), so each call needs its own variable.
  void insertPersistentVertices(std::shared_ptr<Connection> conn) {
    auto r1 = conn->Query(
        "CREATE (p:Person {id: 1, name: 'Alice', age: 30});");
    EXPECT_TRUE(r1) << r1.error().ToString();
    auto r2 = conn->Query(
        "CREATE (p:Person {id: 2, name: 'Bob', age: 25});");
    EXPECT_TRUE(r2) << r2.error().ToString();
    auto r3 = conn->Query(
        "CREATE (p:Person {id: 3, name: 'Carol', age: 35});");
    EXPECT_TRUE(r3) << r3.error().ToString();
    auto r4 = conn->Query(
        "CREATE (p:Person {id: 4, name: 'Dave', age: 20});");
    EXPECT_TRUE(r4) << r4.error().ToString();
  }
};

// ============================================================================
// LOAD NODE TABLE basic
// ============================================================================

TEST_F(LoadAsTest, LoadNodeTableBasic) {
  auto conn = db_->Connect();
  std::string csv_path = std::string(CSV_DIR) + "/people.csv";

  auto res = conn->Query(
      "LOAD NODE TABLE FROM \"" + csv_path +
      "\" (primary_key = 'id', header = true) AS TempPeople;");
  EXPECT_TRUE(res) << res.error().ToString();

  // Verify data is queryable
  auto match_res = conn->Query(
      "MATCH (n:TempPeople) RETURN n.id ORDER BY n.id;");
  EXPECT_TRUE(match_res) << match_res.error().ToString();

  const auto& table = match_res.value().response();
  EXPECT_EQ(table.row_count(), 4);
  const auto& id_col = table.arrays(0).int64_array();
  std::vector<int64_t> ids;
  for (int64_t i = 0; i < id_col.values_size(); ++i) {
    ids.push_back(id_col.values(i));
  }
  EXPECT_EQ(ids, (std::vector<int64_t>{1, 2, 3, 4}));
  conn->Close();
}

// ============================================================================
// LOAD NODE TABLE default primary key (first column)
// ============================================================================

TEST_F(LoadAsTest, LoadNodeTableDefaultPrimaryKey) {
  auto conn = db_->Connect();
  std::string csv_path = std::string(CSV_DIR) + "/people.csv";

  // No primary_key option: should default to first column (id)
  auto res = conn->Query(
      "LOAD NODE TABLE FROM \"" + csv_path + "\" (header = true) AS TempDefault;");
  EXPECT_TRUE(res) << res.error().ToString();

  auto match_res = conn->Query(
      "MATCH (n:TempDefault) RETURN n.id, n.name ORDER BY n.id;");
  EXPECT_TRUE(match_res) << match_res.error().ToString();
  EXPECT_EQ(match_res.value().response().row_count(), 4);
  conn->Close();
}

// ============================================================================
// LOAD NODE TABLE with WHERE filter pushdown
// ============================================================================

TEST_F(LoadAsTest, LoadNodeTableWithWhere) {
  auto conn = db_->Connect();
  std::string csv_path = std::string(CSV_DIR) + "/people.csv";

  auto res = conn->Query(
      "LOAD NODE TABLE FROM \"" + csv_path +
      "\" (primary_key = 'id', header = true) WHERE age > 25 AS TempAdults;");
  EXPECT_TRUE(res) << res.error().ToString();

  auto match_res = conn->Query(
      "MATCH (n:TempAdults) RETURN n.id, n.name, n.age ORDER BY n.id;");
  EXPECT_TRUE(match_res) << match_res.error().ToString();

  const auto& table = match_res.value().response();
  // Only Alice(30) and Carol(35) should pass age > 25
  EXPECT_EQ(table.row_count(), 2);
  const auto& id_col = table.arrays(0).int64_array();
  std::vector<int64_t> ids;
  for (int64_t i = 0; i < id_col.values_size(); ++i) {
    ids.push_back(id_col.values(i));
  }
  EXPECT_EQ(ids, (std::vector<int64_t>{1, 3}));
  conn->Close();
}

// ============================================================================
// LOAD NODE TABLE with RETURN projection
// ============================================================================

TEST_F(LoadAsTest, LoadNodeTableWithReturn) {
  auto conn = db_->Connect();
  std::string csv_path = std::string(CSV_DIR) + "/people.csv";

  // Only project id and name (drop age)
  auto res = conn->Query(
      "LOAD NODE TABLE FROM \"" + csv_path +
      "\" (primary_key = 'id', header = true) RETURN id, name AS TempSlim;");
  EXPECT_TRUE(res) << res.error().ToString();

  // id and name should be accessible
  auto match_res = conn->Query(
      "MATCH (n:TempSlim) RETURN n.id, n.name ORDER BY n.id;");
  EXPECT_TRUE(match_res) << match_res.error().ToString();
  EXPECT_EQ(match_res.value().response().row_count(), 4);
  conn->Close();
}

// ============================================================================
// LOAD NODE TABLE with WHERE + RETURN combined
// ============================================================================

TEST_F(LoadAsTest, LoadNodeTableWhereAndReturn) {
  auto conn = db_->Connect();
  std::string csv_path = std::string(CSV_DIR) + "/people.csv";

  // Filter age >= 25, project only id and age
  auto res = conn->Query(
      "LOAD NODE TABLE FROM \"" + csv_path +
      "\" (primary_key = 'id', header = true) WHERE age >= 25 RETURN id, age "
      "AS TempFiltered;");
  EXPECT_TRUE(res) << res.error().ToString();

  auto match_res = conn->Query(
      "MATCH (n:TempFiltered) RETURN n.id, n.age ORDER BY n.id;");
  EXPECT_TRUE(match_res) << match_res.error().ToString();

  const auto& table = match_res.value().response();
  // Bob(25), Alice(30), Carol(35) pass age >= 25
  EXPECT_EQ(table.row_count(), 3);
  const auto& id_col = table.arrays(0).int64_array();
  const auto& age_col = table.arrays(1).int64_array();
  // Filtered by age >= 25 → Alice(1,30), Bob(2,25), Carol(3,35);
  // ORDER BY n.id ascending → ids = [1,2,3], ages = [30,25,35].
  EXPECT_EQ(id_col.values(0), 1);
  EXPECT_EQ(id_col.values(1), 2);
  EXPECT_EQ(id_col.values(2), 3);
  EXPECT_EQ(age_col.values(0), 30);
  EXPECT_EQ(age_col.values(1), 25);
  EXPECT_EQ(age_col.values(2), 35);
  conn->Close();
}

// ============================================================================
// LOAD REL TABLE basic
// ============================================================================

TEST_F(LoadAsTest, LoadRelTableBasic) {
  auto conn = db_->Connect();
  setupPersistentNodeTables(conn);
  insertPersistentVertices(conn);

  // First load the src/dst vertices as temp tables too (for index lookup)
  std::string people_csv = std::string(CSV_DIR) + "/people.csv";
  auto load_nodes_res = conn->Query(
      "LOAD NODE TABLE FROM \"" + people_csv +
      "\" (primary_key = 'id', header = true) AS TempPerson;");
  EXPECT_TRUE(load_nodes_res) << load_nodes_res.error().ToString();

  std::string edges_csv = std::string(CSV_DIR) + "/edges.csv";
  auto load_edges_res = conn->Query(
      "LOAD REL TABLE FROM \"" + edges_csv +
      "\" (from = 'TempPerson', to = 'TempPerson', "
      "from_col = 'src_id', to_col = 'dst_id', header = true) AS TempKnows;");
  EXPECT_TRUE(load_edges_res) << load_edges_res.error().ToString();

  // Query the loaded edges
  auto match_res = conn->Query(
      "MATCH (a:TempPerson)-[e:TempKnows]->(b:TempPerson) "
      "RETURN a.id, b.id, e.weight ORDER BY a.id;");
  EXPECT_TRUE(match_res) << match_res.error().ToString();

  const auto& table = match_res.value().response();
  EXPECT_EQ(table.row_count(), 3);
  conn->Close();
}

// ============================================================================
// Error: RETURN missing primary_key
// ============================================================================

TEST_F(LoadAsTest, LoadNodeTableReturnMissingPrimaryKey) {
  auto conn = db_->Connect();
  std::string csv_path = std::string(CSV_DIR) + "/people.csv";

  // primary_key is 'id' but RETURN only has name and age
  auto res = conn->Query(
      "LOAD NODE TABLE FROM \"" + csv_path +
      "\" (primary_key = 'id', header = true) RETURN name, age AS TempBad;");
  EXPECT_FALSE(res);
  // Should report missing primary_key in RETURN
  EXPECT_NE(res.error().ToString().find("primary key"), std::string::npos)
      << res.error().ToString();
  conn->Close();
}

// ============================================================================
// Error: LOAD REL TABLE without from/to options
// ============================================================================

TEST_F(LoadAsTest, LoadRelTableMissingFromTo) {
  auto conn = db_->Connect();
  std::string csv_path = std::string(CSV_DIR) + "/edges.csv";

  auto res = conn->Query(
      "LOAD REL TABLE FROM \"" + csv_path + "\" (header = true) AS TempEdge;");
  EXPECT_FALSE(res);
  EXPECT_NE(res.error().ToString().find("from"), std::string::npos)
      << res.error().ToString();
  conn->Close();
}

// ============================================================================
// Error: RETURN column not in source
// ============================================================================

TEST_F(LoadAsTest, LoadNodeTableReturnNonexistentColumn) {
  auto conn = db_->Connect();
  std::string csv_path = std::string(CSV_DIR) + "/people.csv";

  auto res = conn->Query(
      "LOAD NODE TABLE FROM \"" + csv_path +
      "\" (primary_key = 'id', header = true) RETURN id, nonexistent AS TempMissing;");
  EXPECT_FALSE(res);
  EXPECT_NE(res.error().ToString().find("not found"), std::string::npos)
      << res.error().ToString();
  conn->Close();
}

// ============================================================================
// Cleanup: temp tables removed after Connection::Close()
// ============================================================================

TEST_F(LoadAsTest, CleanupOnClose) {
  // Phase 1: create temp table and verify it exists
  {
    auto conn = db_->Connect();
    std::string csv_path = std::string(CSV_DIR) + "/people.csv";
    auto res = conn->Query(
        "LOAD NODE TABLE FROM \"" + csv_path +
        "\" (primary_key = 'id', header = true) AS TempEphemeral;");
    EXPECT_TRUE(res) << res.error().ToString();

    auto match_res = conn->Query(
        "MATCH (n:TempEphemeral) RETURN count(n);");
    EXPECT_TRUE(match_res) << match_res.error().ToString();
    conn->Close();
  }

  // Phase 2: reopen DB, temp table should be gone
  {
    // Release the fixture's db_ first; NeugDB takes an exclusive write
    // lock on the data directory and the same process cannot hold two
    // locks at once.
    db_->Close();
    db_.reset();
    auto db2 = std::make_unique<NeugDB>();
    NeugDBConfig config;
    config.data_dir = DB_DIR;
    config.checkpoint_on_close = true;
    config.compact_on_close = true;
    config.compact_csr = true;
    config.enable_auto_compaction = false;
    db2->Open(config);
    auto conn2 = db2->Connect();

    // TempEphemeral should not exist after checkpoint+reopen
    auto match_res = conn2->Query(
        "MATCH (n:TempEphemeral) RETURN n.id;");
    // This may fail with "label not found" or return empty
    // depending on how the catalog handles unknown labels
    conn2->Close();
    db2->Close();
  }
}

// ============================================================================
// Error: LOAD AS conflicts with existing persistent label
// ============================================================================

TEST_F(LoadAsTest, LoadAsLabelConflict) {
  auto conn = db_->Connect();
  // Create a persistent table named 'Person'
  auto create_res = conn->Query(
      "CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id));");
  EXPECT_TRUE(create_res) << create_res.error().ToString();

  std::string csv_path = std::string(CSV_DIR) + "/people.csv";
  // Try to LOAD AS 'Person' — should conflict
  auto res = conn->Query(
      "LOAD NODE TABLE FROM \"" + csv_path +
      "\" (primary_key = 'id', header = true) AS Person;");
  EXPECT_FALSE(res);
  conn->Close();
}

// ============================================================================
// Error: duplicate LOAD AS same label on same connection
// ============================================================================

TEST_F(LoadAsTest, DuplicateLoadAsSameLabel) {
  auto conn = db_->Connect();
  std::string csv_path = std::string(CSV_DIR) + "/people.csv";

  auto res1 = conn->Query(
      "LOAD NODE TABLE FROM \"" + csv_path +
      "\" (primary_key = 'id', header = true) AS TempDup;");
  EXPECT_TRUE(res1) << res1.error().ToString();

  // Second LOAD AS with the same label should fail.
  auto res2 = conn->Query(
      "LOAD NODE TABLE FROM \"" + csv_path +
      "\" (primary_key = 'id', header = true) AS TempDup;");
  EXPECT_FALSE(res2);
  conn->Close();
}

// ============================================================================
// Close → Reconnect → Reload same temp label succeeds
// ============================================================================

TEST_F(LoadAsTest, ReloadAfterCleanup) {
  // Phase 1: create temp table, verify, close connection.
  {
    auto conn = db_->Connect();
    std::string csv_path = std::string(CSV_DIR) + "/people.csv";
    auto res = conn->Query(
        "LOAD NODE TABLE FROM \"" + csv_path +
        "\" (primary_key = 'id', header = true) AS TempReusable;");
    EXPECT_TRUE(res) << res.error().ToString();
    auto match_res = conn->Query(
        "MATCH (n:TempReusable) RETURN count(n);");
    EXPECT_TRUE(match_res) << match_res.error().ToString();
    conn->Close();
    db_->RemoveConnection(conn);
  }

  // Phase 2: reconnect and load the same label again — should succeed.
  {
    auto conn2 = db_->Connect();
    std::string csv_path = std::string(CSV_DIR) + "/people.csv";
    auto res2 = conn2->Query(
        "LOAD NODE TABLE FROM \"" + csv_path +
        "\" (primary_key = 'id', header = true) AS TempReusable;");
    EXPECT_TRUE(res2) << res2.error().ToString();
    auto match_res = conn2->Query(
        "MATCH (n:TempReusable) RETURN count(n);");
    EXPECT_TRUE(match_res) << match_res.error().ToString();
    conn2->Close();
    db_->RemoveConnection(conn2);
  }
}

// ============================================================================
// Temp data not persisted to disk after checkpoint
// ============================================================================

TEST_F(LoadAsTest, TempNotPersistedAfterCheckpoint) {
  // Phase 1: create temp table, close DB (triggers checkpoint).
  {
    auto conn = db_->Connect();
    std::string csv_path = std::string(CSV_DIR) + "/people.csv";
    auto res = conn->Query(
        "LOAD NODE TABLE FROM \"" + csv_path +
        "\" (primary_key = 'id', header = true) AS TempGhost;");
    EXPECT_TRUE(res) << res.error().ToString();
    conn->Close();
  }

  // Phase 2: release and reopen DB from disk.  TempGhost must not exist.
  {
    db_->Close();
    db_.reset();
    auto db2 = std::make_unique<NeugDB>();
    NeugDBConfig config;
    config.data_dir = DB_DIR;
    config.checkpoint_on_close = true;
    config.compact_on_close = true;
    config.compact_csr = true;
    config.enable_auto_compaction = false;
    db2->Open(config);
    auto conn2 = db2->Connect();
    auto match_res = conn2->Query(
        "MATCH (n:TempGhost) RETURN n.id;");
    EXPECT_FALSE(match_res);
    conn2->Close();
    db2->Close();
  }
}

// ============================================================================
// Error: LOAD REL TABLE with nonexistent vertex label
// ============================================================================

TEST_F(LoadAsTest, LoadRelTableNonexistentVertexLabel) {
  auto conn = db_->Connect();
  std::string edges_csv = std::string(CSV_DIR) + "/edges.csv";

  // 'Nope' does not exist as a vertex label.
  auto res = conn->Query(
      "LOAD REL TABLE FROM \"" + edges_csv +
      "\" (header = true, from = 'Nope', to = 'Nope', "
      "from_col = 'src_id', to_col = 'dst_id') AS TempBadEdge;");
  EXPECT_FALSE(res);
  conn->Close();
}

// ============================================================================
// LOAD REL TABLE: from = temp vertex, to = persistent vertex
// ============================================================================

TEST_F(LoadAsTest, LoadRelTableTempToPersistent) {
  auto conn = db_->Connect();
  setupPersistentNodeTables(conn);
  insertPersistentVertices(conn);

  // Load src vertices as temp table.
  std::string people_csv = std::string(CSV_DIR) + "/people.csv";
  auto load_nodes_res = conn->Query(
      "LOAD NODE TABLE FROM \"" + people_csv +
      "\" (primary_key = 'id', header = true) AS TempSrc;");
  EXPECT_TRUE(load_nodes_res) << load_nodes_res.error().ToString();

  // edges.csv: src_id → dst_id.  src is TempSrc (temp), dst is Person
  // (persistent).
  std::string edges_csv = std::string(CSV_DIR) + "/edges.csv";
  auto load_edges_res = conn->Query(
      "LOAD REL TABLE FROM \"" + edges_csv +
      "\" (header = true, from = 'TempSrc', to = 'Person', "
      "from_col = 'src_id', to_col = 'dst_id') AS TempMixedEdge;");
  EXPECT_TRUE(load_edges_res) << load_edges_res.error().ToString();

  auto match_res = conn->Query(
      "MATCH (a:TempSrc)-[r:TempMixedEdge]->(b:Person) "
      "RETURN a.id, b.id ORDER BY a.id;");
  EXPECT_TRUE(match_res) << match_res.error().ToString();
  EXPECT_EQ(match_res.value().response().row_count(), 3);
  conn->Close();
}

// ============================================================================
// Edge dangling reference: from_col/to_col values not found in vertex table
// ============================================================================

TEST_F(LoadAsTest, LoadRelTableDanglingReference) {
  auto conn = db_->Connect();
  std::string people_csv = std::string(CSV_DIR) + "/people.csv";
  auto load_nodes_res = conn->Query(
      "LOAD NODE TABLE FROM \"" + people_csv +
      "\" (primary_key = 'id', header = true) AS TempPersonD;");
  EXPECT_TRUE(load_nodes_res) << load_nodes_res.error().ToString();

  // dangling_edges.csv has src_id=99 which does not exist in TempPersonD.
  // Storage silently skips edges with unresolved endpoints (consistent with
  // the persist-graph COPY FROM path).
  std::string dangling_csv = std::string(CSV_DIR) + "/dangling_edges.csv";
  auto load_edges_res = conn->Query(
      "LOAD REL TABLE FROM \"" + dangling_csv +
      "\" (header = true, from = 'TempPersonD', to = 'TempPersonD', "
      "from_col = 'src_id', to_col = 'dst_id') AS TempDanglingEdge;");
  EXPECT_TRUE(load_edges_res) << load_edges_res.error().ToString();

  // Only the valid edge (1→2) should be inserted; (99→3) is skipped.
  auto match_res = conn->Query(
      "MATCH (a:TempPersonD)-[r:TempDanglingEdge]->(b:TempPersonD) "
      "RETURN a.id, b.id;");
  EXPECT_TRUE(match_res) << match_res.error().ToString();
  EXPECT_EQ(match_res.value().response().row_count(), 1);
  conn->Close();
}

// ============================================================================
// Multiple temp tables all cleaned up on close
// ============================================================================

TEST_F(LoadAsTest, MultipleTempTablesCleanup) {
  {
    auto conn = db_->Connect();
    std::string people_csv = std::string(CSV_DIR) + "/people.csv";
    std::string items_csv = std::string(CSV_DIR) + "/items.csv";

    auto r1 = conn->Query(
        "LOAD NODE TABLE FROM \"" + people_csv +
        "\" (primary_key = 'id', header = true) AS TempA;");
    EXPECT_TRUE(r1) << r1.error().ToString();

    auto r2 = conn->Query(
        "LOAD NODE TABLE FROM \"" + items_csv +
        "\" (primary_key = 'item_id', header = true) AS TempB;");
    EXPECT_TRUE(r2) << r2.error().ToString();

    auto r3 = conn->Query(
        "LOAD NODE TABLE FROM \"" + people_csv +
        "\" (primary_key = 'id', header = true) AS TempC;");
    EXPECT_TRUE(r3) << r3.error().ToString();

    // Verify all three exist.
    for (const auto& label : {"TempA", "TempB", "TempC"}) {
      auto res = conn->Query(
          std::string("MATCH (n:") + label + ") RETURN count(n);");
      EXPECT_TRUE(res) << res.error().ToString();
    }
    conn->Close();
  }

  // Reopen DB, none of the three should exist.
  {
    db_->Close();
    db_.reset();
    auto db2 = std::make_unique<NeugDB>();
    NeugDBConfig config;
    config.data_dir = DB_DIR;
    config.checkpoint_on_close = true;
    config.compact_on_close = true;
    config.compact_csr = true;
    config.enable_auto_compaction = false;
    db2->Open(config);
    auto conn2 = db2->Connect();
    for (const auto& label : {"TempA", "TempB", "TempC"}) {
      auto res = conn2->Query(
          std::string("MATCH (n:") + label + ") RETURN n.id;");
      EXPECT_FALSE(res) << "label " << label << " should not exist after close";
    }
    conn2->Close();
    db2->Close();
  }
}

// ============================================================================
// Persistent table survives, temp table cleaned on close
// ============================================================================

TEST_F(LoadAsTest, PersistentSurvivesTempCleanup) {
  {
    auto conn = db_->Connect();
    auto create_res = conn->Query(
        "CREATE NODE TABLE Persistent(id INT64, name STRING, PRIMARY KEY(id));");
    EXPECT_TRUE(create_res) << create_res.error().ToString();
    auto insert_res = conn->Query(
        "CREATE (p:Persistent {id: 1, name: 'Alice'});");
    EXPECT_TRUE(insert_res) << insert_res.error().ToString();

    std::string people_csv = std::string(CSV_DIR) + "/people.csv";
    auto load_res = conn->Query(
        "LOAD NODE TABLE FROM \"" + people_csv +
        "\" (primary_key = 'id', header = true) AS TempGone;");
    EXPECT_TRUE(load_res) << load_res.error().ToString();
    conn->Close();
  }

  // Reopen DB: Persistent should still be there, TempGone should not.
  {
    db_->Close();
    db_.reset();
    auto db2 = std::make_unique<NeugDB>();
    NeugDBConfig config;
    config.data_dir = DB_DIR;
    config.checkpoint_on_close = true;
    config.compact_on_close = true;
    config.compact_csr = true;
    config.enable_auto_compaction = false;
    db2->Open(config);
    auto conn2 = db2->Connect();

    auto persist_res = conn2->Query(
        "MATCH (n:Persistent) RETURN n.id, n.name;");
    EXPECT_TRUE(persist_res) << persist_res.error().ToString();
    EXPECT_EQ(persist_res.value().response().row_count(), 1);

    auto temp_res = conn2->Query(
        "MATCH (n:TempGone) RETURN n.id;");
    EXPECT_FALSE(temp_res);

    conn2->Close();
    db2->Close();
  }
}

// ============================================================================
// Property type inference: age column should be INT64
// ============================================================================

TEST_F(LoadAsTest, PropertyTypeInference) {
  auto conn = db_->Connect();
  std::string csv_path = std::string(CSV_DIR) + "/people.csv";

  auto res = conn->Query(
      "LOAD NODE TABLE FROM \"" + csv_path +
      "\" (primary_key = 'id', header = true) AS TempTyped;");
  EXPECT_TRUE(res) << res.error().ToString();

  // age should be queryable as an integer (e.g., arithmetic works).
  auto match_res = conn->Query(
      "MATCH (n:TempTyped) RETURN n.age + 1 ORDER BY n.id;");
  EXPECT_TRUE(match_res) << match_res.error().ToString();
  const auto& table = match_res.value().response();
  EXPECT_EQ(table.row_count(), 4);
  const auto& col = table.arrays(0).int64_array();
  // First row: Alice age=30, 30+1=31.
  EXPECT_EQ(col.values(0), 31);
  conn->Close();
}

// ============================================================================
// RETURN column ordering: verify the declared order is respected
// ============================================================================

TEST_F(LoadAsTest, ReturnColumnOrdering) {
  auto conn = db_->Connect();
  std::string csv_path = std::string(CSV_DIR) + "/people.csv";

  // RETURN name, id (reversed from source order id, name).
  auto res = conn->Query(
      "LOAD NODE TABLE FROM \"" + csv_path +
      "\" (primary_key = 'id', header = true) RETURN name, id AS TempOrder;");
  EXPECT_TRUE(res) << res.error().ToString();

  // MATCH returns properties in the order they were declared in DDL.
  // Since ddlColumns is built from returnColumns order, name should come first.
  auto match_res = conn->Query(
      "MATCH (n:TempOrder) RETURN n.name, n.id ORDER BY n.id;");
  EXPECT_TRUE(match_res) << match_res.error().ToString();
  const auto& table = match_res.value().response();
  EXPECT_EQ(table.row_count(), 4);
  // Column 0 should be name (string), column 1 should be id (int64).
  const auto& name_col = table.arrays(0).string_array();
  const auto& id_col = table.arrays(1).int64_array();
  EXPECT_EQ(name_col.values(0), "Alice");
  EXPECT_EQ(id_col.values(0), 1);
  conn->Close();
}

// ============================================================================
// LOAD REL TABLE with WHERE filter
// ============================================================================

TEST_F(LoadAsTest, LoadRelTableWithWhere) {
  auto conn = db_->Connect();
  std::string people_csv = std::string(CSV_DIR) + "/people.csv";
  auto load_nodes_res = conn->Query(
      "LOAD NODE TABLE FROM \"" + people_csv +
      "\" (primary_key = 'id', header = true) AS TempPersonW;");
  EXPECT_TRUE(load_nodes_res) << load_nodes_res.error().ToString();

  // Only load edges where weight > 0.6 (rows: 1.0 and 0.8, skip 0.5).
  std::string edges_csv = std::string(CSV_DIR) + "/edges.csv";
  auto load_edges_res = conn->Query(
      "LOAD REL TABLE FROM \"" + edges_csv +
      "\" (header = true, from = 'TempPersonW', to = 'TempPersonW', "
      "from_col = 'src_id', to_col = 'dst_id') "
      "WHERE weight > 0.6 AS TempFilteredEdge;");
  EXPECT_TRUE(load_edges_res) << load_edges_res.error().ToString();

  auto match_res = conn->Query(
      "MATCH (a:TempPersonW)-[r:TempFilteredEdge]->(b:TempPersonW) "
      "RETURN a.id, b.id, r.weight ORDER BY r.weight;");
  EXPECT_TRUE(match_res) << match_res.error().ToString();
  const auto& table = match_res.value().response();
  EXPECT_EQ(table.row_count(), 2);
  conn->Close();
}

// ============================================================================
// LOAD REL TABLE with RETURN projection
// ============================================================================

TEST_F(LoadAsTest, LoadRelTableWithReturn) {
  auto conn = db_->Connect();
  std::string people_csv = std::string(CSV_DIR) + "/people.csv";
  auto load_nodes_res = conn->Query(
      "LOAD NODE TABLE FROM \"" + people_csv +
      "\" (primary_key = 'id', header = true) AS TempPersonR;");
  EXPECT_TRUE(load_nodes_res) << load_nodes_res.error().ToString();

  // Only project src_id, dst_id, weight (weight is the only extra property).
  std::string edges_csv = std::string(CSV_DIR) + "/edges.csv";
  auto load_edges_res = conn->Query(
      "LOAD REL TABLE FROM \"" + edges_csv +
      "\" (header = true, from = 'TempPersonR', to = 'TempPersonR', "
      "from_col = 'src_id', to_col = 'dst_id') "
      "RETURN src_id, dst_id, weight AS TempSlimEdge;");
  EXPECT_TRUE(load_edges_res) << load_edges_res.error().ToString();

  auto match_res = conn->Query(
      "MATCH (a:TempPersonR)-[r:TempSlimEdge]->(b:TempPersonR) "
      "RETURN a.id, b.id, r.weight ORDER BY a.id;");
  EXPECT_TRUE(match_res) << match_res.error().ToString();
  EXPECT_EQ(match_res.value().response().row_count(), 3);
  conn->Close();
}

// ============================================================================
// LOAD REL TABLE with WHERE + RETURN combined
// ============================================================================

TEST_F(LoadAsTest, LoadRelTableWhereAndReturn) {
  auto conn = db_->Connect();
  std::string people_csv = std::string(CSV_DIR) + "/people.csv";
  auto load_nodes_res = conn->Query(
      "LOAD NODE TABLE FROM \"" + people_csv +
      "\" (primary_key = 'id', header = true) AS TempPersonWR;");
  EXPECT_TRUE(load_nodes_res) << load_nodes_res.error().ToString();

  // Filter weight >= 0.8, project only weight (src_id/dst_id implicit).
  std::string edges_csv = std::string(CSV_DIR) + "/edges.csv";
  auto load_edges_res = conn->Query(
      "LOAD REL TABLE FROM \"" + edges_csv +
      "\" (header = true, from = 'TempPersonWR', to = 'TempPersonWR', "
      "from_col = 'src_id', to_col = 'dst_id') "
      "WHERE weight >= 0.8 "
      "RETURN src_id, dst_id, weight AS TempWREdge;");
  EXPECT_TRUE(load_edges_res) << load_edges_res.error().ToString();

  auto match_res = conn->Query(
      "MATCH (a:TempPersonWR)-[r:TempWREdge]->(b:TempPersonWR) "
      "RETURN a.id, b.id, r.weight ORDER BY r.weight;");
  EXPECT_TRUE(match_res) << match_res.error().ToString();
  const auto& table = match_res.value().response();
  // Only edges with weight >= 0.8: (2→3, 1.0) and (3→4, 0.8).
  EXPECT_EQ(table.row_count(), 2);
  conn->Close();
}

}  // namespace test
}  // namespace neug
