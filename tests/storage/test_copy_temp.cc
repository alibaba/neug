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

class CopyTempTest : public ::testing::Test {
 protected:
  static constexpr const char* DB_DIR = "/tmp/copy_temp_test_db";
  static constexpr const char* CSV_DIR = "/tmp/copy_temp_test_csv";

  std::unique_ptr<NeugDB> db_;

  void write_csv(const std::string& filename, const std::string& content) {
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

    write_csv("people.csv",
              "id|name|age\n"
              "1|Alice|30\n"
              "2|Bob|25\n"
              "3|Carol|35\n"
              "4|Dave|20\n");

    write_csv("edges.csv",
              "src_id|dst_id|weight\n"
              "1|2|0.5\n"
              "2|3|1.0\n"
              "3|4|0.8\n");

    write_csv("edges_shuffled.csv",
              "weight|src_id|dst_id\n"
              "0.5|1|2\n"
              "1.0|2|3\n"
              "0.8|3|4\n");

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

  void setupPersistentPersonTable(std::shared_ptr<Connection> conn) {
    auto r = conn->Query(
        "CREATE NODE TABLE Person(id INT64, name STRING, age INT64, "
        "PRIMARY KEY(id));");
    EXPECT_TRUE(r) << r.error().ToString();
    auto r1 = conn->Query("CREATE (p:Person {id: 1, name: 'Alice', age: 30});");
    EXPECT_TRUE(r1) << r1.error().ToString();
    auto r2 = conn->Query("CREATE (p:Person {id: 2, name: 'Bob', age: 25});");
    EXPECT_TRUE(r2) << r2.error().ToString();
    auto r3 = conn->Query("CREATE (p:Person {id: 3, name: 'Carol', age: 35});");
    EXPECT_TRUE(r3) << r3.error().ToString();
    auto r4 = conn->Query("CREATE (p:Person {id: 4, name: 'Dave', age: 20});");
    EXPECT_TRUE(r4) << r4.error().ToString();
  }
};

// ============================================================================
// Basic NODE tests
// ============================================================================

TEST_F(CopyTempTest, NodeBasic) {
  auto conn = db_->Connect();
  std::string csv = std::string(CSV_DIR) + "/people.csv";
  auto res =
      conn->Query("COPY TEMP TempPeople FROM \"" + csv + "\" (header = true)");
  EXPECT_TRUE(res) << res.error().ToString();
  auto q = conn->Query("MATCH (n:TempPeople) RETURN n.id ORDER BY n.id;");
  EXPECT_TRUE(q) << q.error().ToString();
  EXPECT_EQ(q.value().response().row_count(), 4);
  conn->Close();
}

TEST_F(CopyTempTest, NodeDefaultPrimaryKey) {
  auto conn = db_->Connect();
  std::string csv = std::string(CSV_DIR) + "/people.csv";
  auto res =
      conn->Query("COPY TEMP TempDefault FROM \"" + csv + "\" (header = true)");
  EXPECT_TRUE(res) << res.error().ToString();
  auto q = conn->Query("MATCH (n:TempDefault) RETURN n.id ORDER BY n.id;");
  EXPECT_TRUE(q) << q.error().ToString();
  EXPECT_EQ(q.value().response().row_count(), 4);
  conn->Close();
}

TEST_F(CopyTempTest, NodeWithWhere) {
  auto conn = db_->Connect();
  std::string csv = std::string(CSV_DIR) + "/people.csv";
  auto res = conn->Query("COPY TEMP TempFiltered FROM (LOAD FROM \"" + csv +
                         "\" (header = true) WHERE age > 25 RETURN *)");
  EXPECT_TRUE(res) << res.error().ToString();
  auto q = conn->Query("MATCH (n:TempFiltered) RETURN n.id ORDER BY n.id;");
  EXPECT_TRUE(q) << q.error().ToString();
  EXPECT_EQ(q.value().response().row_count(), 2);
  conn->Close();
}

TEST_F(CopyTempTest, NodeWithReturn) {
  auto conn = db_->Connect();
  std::string csv = std::string(CSV_DIR) + "/people.csv";
  auto res = conn->Query("COPY TEMP TempSlim FROM (LOAD FROM \"" + csv +
                         "\" (header = true) RETURN id, name)");
  EXPECT_TRUE(res) << res.error().ToString();
  auto q = conn->Query("MATCH (n:TempSlim) RETURN n.id, n.name ORDER BY n.id;");
  EXPECT_TRUE(q) << q.error().ToString();
  EXPECT_EQ(q.value().response().row_count(), 4);
  conn->Close();
}

TEST_F(CopyTempTest, NodeWhereAndReturn) {
  auto conn = db_->Connect();
  std::string csv = std::string(CSV_DIR) + "/people.csv";
  auto res = conn->Query("COPY TEMP TempWR FROM (LOAD FROM \"" + csv +
                         "\" (header = true) WHERE age >= 25 RETURN id, name)");
  EXPECT_TRUE(res) << res.error().ToString();
  auto q = conn->Query("MATCH (n:TempWR) RETURN n.id ORDER BY n.id;");
  EXPECT_TRUE(q) << q.error().ToString();
  EXPECT_EQ(q.value().response().row_count(), 3);
  conn->Close();
}

// ============================================================================
// Basic REL tests
// ============================================================================

TEST_F(CopyTempTest, RelBasic) {
  auto conn = db_->Connect();
  std::string people = std::string(CSV_DIR) + "/people.csv";
  std::string edges = std::string(CSV_DIR) + "/edges.csv";
  auto r1 = conn->Query("COPY TEMP TempPerson FROM \"" + people +
                        "\" (header = true)");
  EXPECT_TRUE(r1) << r1.error().ToString();
  auto r2 =
      conn->Query("COPY TEMP TempKnows FROM \"" + edges +
                  "\" (header = true, from = 'TempPerson', to = 'TempPerson')");
  EXPECT_TRUE(r2) << r2.error().ToString();
  auto q = conn->Query(
      "MATCH (a:TempPerson)-[r:TempKnows]->(b:TempPerson) "
      "RETURN a.id, b.id ORDER BY a.id;");
  EXPECT_TRUE(q) << q.error().ToString();
  EXPECT_EQ(q.value().response().row_count(), 3);
  conn->Close();
}

TEST_F(CopyTempTest, RelWithPersistentVertices) {
  auto conn = db_->Connect();
  setupPersistentPersonTable(conn);
  std::string edges = std::string(CSV_DIR) + "/edges.csv";
  auto r = conn->Query("COPY TEMP TempKnowsP FROM \"" + edges +
                       "\" (header = true, from = 'Person', to = 'Person')");
  EXPECT_TRUE(r) << r.error().ToString();
  auto q = conn->Query(
      "MATCH (a:Person)-[r:TempKnowsP]->(b:Person) "
      "RETURN a.id, b.id ORDER BY a.id;");
  EXPECT_TRUE(q) << q.error().ToString();
  EXPECT_EQ(q.value().response().row_count(), 3);
  conn->Close();
}

TEST_F(CopyTempTest, RelShuffledColumnsNeedsReturn) {
  auto conn = db_->Connect();
  setupPersistentPersonTable(conn);
  std::string csv = std::string(CSV_DIR) + "/edges_shuffled.csv";
  auto r = conn->Query("COPY TEMP TempShuf FROM (LOAD FROM \"" + csv +
                       "\" (header = true) RETURN src_id, dst_id, weight) "
                       "(from = 'Person', to = 'Person')");
  EXPECT_TRUE(r) << r.error().ToString();
  auto q = conn->Query(
      "MATCH (a:Person)-[r:TempShuf]->(b:Person) "
      "RETURN a.id, b.id ORDER BY a.id;");
  EXPECT_TRUE(q) << q.error().ToString();
  EXPECT_EQ(q.value().response().row_count(), 3);
  conn->Close();
}

TEST_F(CopyTempTest, RelWithWhereSubquery) {
  auto conn = db_->Connect();
  std::string people = std::string(CSV_DIR) + "/people.csv";
  std::string edges = std::string(CSV_DIR) + "/edges.csv";
  auto r1 =
      conn->Query("COPY TEMP TempPW FROM \"" + people + "\" (header = true)");
  EXPECT_TRUE(r1) << r1.error().ToString();
  auto r2 = conn->Query("COPY TEMP TempFE FROM (LOAD FROM \"" + edges +
                        "\" (header = true) WHERE weight > 0.6 RETURN *) "
                        "(from = 'TempPW', to = 'TempPW')");
  EXPECT_TRUE(r2) << r2.error().ToString();
  auto q = conn->Query(
      "MATCH (a:TempPW)-[r:TempFE]->(b:TempPW) "
      "RETURN a.id, b.id ORDER BY a.id;");
  EXPECT_TRUE(q) << q.error().ToString();
  EXPECT_EQ(q.value().response().row_count(), 2);
  conn->Close();
}

// ============================================================================
// Mixed persistent + temporary queries
// ============================================================================

TEST_F(CopyTempTest, MixedPersistentAndTemp) {
  auto conn = db_->Connect();
  setupPersistentPersonTable(conn);
  std::string edges = std::string(CSV_DIR) + "/edges.csv";
  auto r = conn->Query("COPY TEMP TempLink FROM \"" + edges +
                       "\" (header = true, from = 'Person', to = 'Person')");
  EXPECT_TRUE(r) << r.error().ToString();
  auto q = conn->Query(
      "MATCH (a:Person)-[r:TempLink]->(b:Person) "
      "RETURN a.name, b.name ORDER BY a.id;");
  EXPECT_TRUE(q) << q.error().ToString();
  EXPECT_EQ(q.value().response().row_count(), 3);
  const auto& src_col = q.value().response().arrays(0).string_array();
  EXPECT_EQ(src_col.values(0), "Alice");
  conn->Close();
}

TEST_F(CopyTempTest, TempSrcPersistentDst) {
  auto conn = db_->Connect();
  setupPersistentPersonTable(conn);
  std::string people = std::string(CSV_DIR) + "/people.csv";
  std::string edges = std::string(CSV_DIR) + "/edges.csv";
  auto r1 =
      conn->Query("COPY TEMP TempSrc FROM \"" + people + "\" (header = true)");
  EXPECT_TRUE(r1) << r1.error().ToString();
  auto r2 = conn->Query("COPY TEMP TempMixed FROM \"" + edges +
                        "\" (header = true, from = 'TempSrc', to = 'Person')");
  EXPECT_TRUE(r2) << r2.error().ToString();
  auto q = conn->Query(
      "MATCH (a:TempSrc)-[r:TempMixed]->(b:Person) "
      "RETURN a.id, b.id ORDER BY a.id;");
  EXPECT_TRUE(q) << q.error().ToString();
  EXPECT_EQ(q.value().response().row_count(), 3);
  conn->Close();
}

// ============================================================================
// Cleanup & lifecycle
// ============================================================================

TEST_F(CopyTempTest, CleanupOnClose) {
  {
    auto conn = db_->Connect();
    std::string csv = std::string(CSV_DIR) + "/people.csv";
    auto r = conn->Query("COPY TEMP TempEphemeral FROM \"" + csv +
                         "\" (header = true)");
    EXPECT_TRUE(r) << r.error().ToString();
    auto q = conn->Query("MATCH (n:TempEphemeral) RETURN count(n);");
    EXPECT_TRUE(q) << q.error().ToString();
    conn->Close();
  }
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
    auto q = conn2->Query("MATCH (n:TempEphemeral) RETURN n.id;");
    EXPECT_FALSE(q);
    conn2->Close();
    db2->Close();
  }
}

TEST_F(CopyTempTest, DuplicateLabelFails) {
  auto conn = db_->Connect();
  std::string csv = std::string(CSV_DIR) + "/people.csv";
  auto r1 =
      conn->Query("COPY TEMP TempDup FROM \"" + csv + "\" (header = true)");
  EXPECT_TRUE(r1) << r1.error().ToString();
  auto r2 =
      conn->Query("COPY TEMP TempDup FROM \"" + csv + "\" (header = true)");
  EXPECT_FALSE(r2);
  conn->Close();
}

TEST_F(CopyTempTest, ReloadAfterClose) {
  {
    auto conn = db_->Connect();
    std::string csv = std::string(CSV_DIR) + "/people.csv";
    auto r =
        conn->Query("COPY TEMP TempReuse FROM \"" + csv + "\" (header = true)");
    EXPECT_TRUE(r) << r.error().ToString();
    conn->Close();
    db_->RemoveConnection(conn);
  }
  {
    auto conn2 = db_->Connect();
    std::string csv = std::string(CSV_DIR) + "/people.csv";
    auto r = conn2->Query("COPY TEMP TempReuse FROM \"" + csv +
                          "\" (header = true)");
    EXPECT_TRUE(r) << r.error().ToString();
    auto q = conn2->Query("MATCH (n:TempReuse) RETURN count(n);");
    EXPECT_TRUE(q) << q.error().ToString();
    conn2->Close();
    db_->RemoveConnection(conn2);
  }
}

TEST_F(CopyTempTest, PersistentSurvivesTempCleanup) {
  {
    auto conn = db_->Connect();
    auto cr =
        conn->Query("CREATE NODE TABLE Persistent(id INT64, PRIMARY KEY(id));");
    EXPECT_TRUE(cr) << cr.error().ToString();
    auto ins = conn->Query("CREATE (p:Persistent {id: 1});");
    EXPECT_TRUE(ins) << ins.error().ToString();
    std::string csv = std::string(CSV_DIR) + "/people.csv";
    auto r =
        conn->Query("COPY TEMP TempGone FROM \"" + csv + "\" (header = true)");
    EXPECT_TRUE(r) << r.error().ToString();
    conn->Close();
  }
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
    auto q1 = conn2->Query("MATCH (n:Persistent) RETURN count(n);");
    EXPECT_TRUE(q1) << q1.error().ToString();
    EXPECT_EQ(q1.value().response().row_count(), 1);
    auto q2 = conn2->Query("MATCH (n:TempGone) RETURN n.id;");
    EXPECT_FALSE(q2);
    conn2->Close();
    db2->Close();
  }
}

// ============================================================================
// Error cases
// ============================================================================

TEST_F(CopyTempTest, LabelConflictWithPersistent) {
  auto conn = db_->Connect();
  auto cr =
      conn->Query("CREATE NODE TABLE Conflict(id INT64, PRIMARY KEY(id));");
  EXPECT_TRUE(cr) << cr.error().ToString();
  std::string csv = std::string(CSV_DIR) + "/people.csv";
  auto r =
      conn->Query("COPY TEMP Conflict FROM \"" + csv + "\" (header = true)");
  EXPECT_FALSE(r);
  conn->Close();
}

TEST_F(CopyTempTest, NonexistentVertexLabel) {
  auto conn = db_->Connect();
  std::string edges = std::string(CSV_DIR) + "/edges.csv";
  auto r = conn->Query("COPY TEMP TempBadEdge FROM \"" + edges +
                       "\" (header = true, from = 'Nope', to = 'Nope')");
  EXPECT_FALSE(r);
  conn->Close();
}

TEST_F(CopyTempTest, DanglingReferenceSilentSkip) {
  auto conn = db_->Connect();
  std::string people = std::string(CSV_DIR) + "/people.csv";
  std::string dangling = std::string(CSV_DIR) + "/dangling_edges.csv";
  auto r1 =
      conn->Query("COPY TEMP TempPD FROM \"" + people + "\" (header = true)");
  EXPECT_TRUE(r1) << r1.error().ToString();
  auto r2 = conn->Query("COPY TEMP TempDangling FROM \"" + dangling +
                        "\" (header = true, from = 'TempPD', to = 'TempPD')");
  EXPECT_TRUE(r2) << r2.error().ToString();
  auto q = conn->Query(
      "MATCH (a:TempPD)-[r:TempDangling]->(b:TempPD) RETURN a.id, b.id;");
  EXPECT_TRUE(q) << q.error().ToString();
  EXPECT_EQ(q.value().response().row_count(), 1);
  conn->Close();
}

// ============================================================================
// Corner cases
// ============================================================================

TEST_F(CopyTempTest, EmptyCsv) {
  write_csv("empty.csv", "id|name|age\n");
  auto conn = db_->Connect();
  std::string csv = std::string(CSV_DIR) + "/empty.csv";
  auto r =
      conn->Query("COPY TEMP TempEmpty FROM \"" + csv + "\" (header = true)");
  EXPECT_TRUE(r) << r.error().ToString();
  auto q = conn->Query("MATCH (n:TempEmpty) RETURN count(n);");
  EXPECT_TRUE(q) << q.error().ToString();
  const auto& col = q.value().response().arrays(0).int64_array();
  EXPECT_EQ(col.values(0), 0);
  conn->Close();
}

TEST_F(CopyTempTest, WhereFiltersAllRows) {
  auto conn = db_->Connect();
  std::string csv = std::string(CSV_DIR) + "/people.csv";
  auto r = conn->Query("COPY TEMP TempNone FROM (LOAD FROM \"" + csv +
                       "\" (header = true) WHERE age > 1000 RETURN *)");
  EXPECT_TRUE(r) << r.error().ToString();
  auto q = conn->Query("MATCH (n:TempNone) RETURN count(n);");
  EXPECT_TRUE(q) << q.error().ToString();
  const auto& col = q.value().response().arrays(0).int64_array();
  EXPECT_EQ(col.values(0), 0);
  conn->Close();
}

TEST_F(CopyTempTest, PropertyTypeInference) {
  auto conn = db_->Connect();
  std::string csv = std::string(CSV_DIR) + "/people.csv";
  auto r =
      conn->Query("COPY TEMP TempTyped FROM \"" + csv + "\" (header = true)");
  EXPECT_TRUE(r) << r.error().ToString();
  auto q = conn->Query("MATCH (n:TempTyped) RETURN n.age + 1 ORDER BY n.id;");
  EXPECT_TRUE(q) << q.error().ToString();
  const auto& col = q.value().response().arrays(0).int64_array();
  EXPECT_EQ(col.values(0), 31);
  conn->Close();
}

TEST_F(CopyTempTest, MultipleQueriesOnSameTemp) {
  auto conn = db_->Connect();
  std::string csv = std::string(CSV_DIR) + "/people.csv";
  auto r =
      conn->Query("COPY TEMP TempStable FROM \"" + csv + "\" (header = true)");
  EXPECT_TRUE(r) << r.error().ToString();
  auto q1 = conn->Query("MATCH (n:TempStable) RETURN count(n);");
  EXPECT_TRUE(q1) << q1.error().ToString();
  auto q2 = conn->Query("MATCH (n:TempStable) WHERE n.age > 25 RETURN n.id;");
  EXPECT_TRUE(q2) << q2.error().ToString();
  EXPECT_EQ(q2.value().response().row_count(), 2);
  auto q3 = conn->Query("MATCH (n:TempStable) RETURN sum(n.age);");
  EXPECT_TRUE(q3) << q3.error().ToString();
  conn->Close();
}

// ============================================================================
// DROP temporary table
// ============================================================================

TEST_F(CopyTempTest, DropTempNode) {
  auto conn = db_->Connect();
  std::string csv = std::string(CSV_DIR) + "/people.csv";
  auto r =
      conn->Query("COPY TEMP TempDrop FROM \"" + csv + "\" (header = true)");
  EXPECT_TRUE(r) << r.error().ToString();
  auto q1 = conn->Query("MATCH (n:TempDrop) RETURN count(n);");
  EXPECT_TRUE(q1) << q1.error().ToString();
  auto drop = conn->Query("DROP TABLE TempDrop;");
  EXPECT_TRUE(drop) << drop.error().ToString();
  auto q2 = conn->Query("MATCH (n:TempDrop) RETURN n.id;");
  EXPECT_FALSE(q2);
  conn->Close();
}

TEST_F(CopyTempTest, DropTempEdge) {
  auto conn = db_->Connect();
  std::string people = std::string(CSV_DIR) + "/people.csv";
  std::string edges = std::string(CSV_DIR) + "/edges.csv";
  auto r1 = conn->Query("COPY TEMP TempNodeK FROM \"" + people +
                        "\" (header = true)");
  EXPECT_TRUE(r1) << r1.error().ToString();
  auto r2 =
      conn->Query("COPY TEMP TempEdgeK FROM \"" + edges +
                  "\" (header = true, from = 'TempNodeK', to = 'TempNodeK')");
  EXPECT_TRUE(r2) << r2.error().ToString();
  auto drop = conn->Query("DROP TABLE TempEdgeK;");
  EXPECT_TRUE(drop) << drop.error().ToString();
  auto q1 = conn->Query("MATCH ()-[r:TempEdgeK]->() RETURN r;");
  EXPECT_FALSE(q1);
  auto q2 = conn->Query("MATCH (n:TempNodeK) RETURN count(n);");
  EXPECT_TRUE(q2) << q2.error().ToString();
  EXPECT_EQ(q2.value().response().row_count(), 1);
  conn->Close();
}

TEST_F(CopyTempTest, DropThenRecreate) {
  auto conn = db_->Connect();
  std::string csv = std::string(CSV_DIR) + "/people.csv";
  auto r1 =
      conn->Query("COPY TEMP TempRecycle FROM \"" + csv + "\" (header = true)");
  EXPECT_TRUE(r1) << r1.error().ToString();
  auto drop = conn->Query("DROP TABLE TempRecycle;");
  EXPECT_TRUE(drop) << drop.error().ToString();
  auto r2 =
      conn->Query("COPY TEMP TempRecycle FROM \"" + csv + "\" (header = true)");
  EXPECT_TRUE(r2) << r2.error().ToString();
  auto q = conn->Query("MATCH (n:TempRecycle) RETURN count(n);");
  EXPECT_TRUE(q) << q.error().ToString();
  conn->Close();
}

}  // namespace test
}  // namespace neug
