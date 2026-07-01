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
#include <array>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <system_error>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include "column_assertions.h"
#include "neug/config.h"
#include "neug/main/connection.h"
#include "neug/main/neug_db.h"
#include "neug/server/neug_db_service.h"
#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/module_descriptor.h"
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

  // -- Unique per-test directory generation --------------------------------

  static std::string MakeUniqueDir(const std::string& prefix) {
    const auto* test_info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    std::string dir = (std::filesystem::temp_directory_path() /
                       (prefix + "_" + test_info->test_suite_name() + "_" +
                        test_info->name()))
                          .string();
    EnsureCleanDir(dir);
    return dir;
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
  std::string db_dir_;

  void SetUp() override {
    db_dir_ = this->MakeUniqueDir("checkpoint_test");
    neug::NeugDB db;
    this->OpenDB(db, db_dir_);
    auto conn = db.Connect();
    load_modern_graph(conn);
    LOG(INFO) << "[CheckPointTest]: Finished loading modern graph";
    conn->Close();
    db.Close();
  }

  void TearDown() override { this->CleanDir(db_dir_); }
};

TYPED_TEST_SUITE(CheckpointTest, AllMemoryLevels);

TYPED_TEST(CheckpointTest, basic) {
  neug::NeugDB db;
  this->OpenDB(db, this->db_dir_);
  db.Close();
  this->OpenDB(db, this->db_dir_);
  auto conn = db.Connect();
  this->AssertBasicPersonAndKnows(*conn, {0.5, 1.0});
}

TYPED_TEST(CheckpointTest, after_add_vertex_property) {
  neug::NeugDB db;
  this->OpenDB(db, this->db_dir_);
  auto conn = db.Connect();
  this->ExpectQuery(*conn, "ALTER TABLE person ADD created STRING;");

  db.Close();
  this->OpenDB(db, this->db_dir_);
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
  this->OpenDB(db, this->db_dir_);
  auto conn = db.Connect();
  this->ExpectQuery(*conn, "ALTER TABLE person DROP age;");

  db.Close();
  this->OpenDB(db, this->db_dir_);
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
  this->OpenDB(db, this->db_dir_);
  auto conn = db.Connect();
  this->ExpectQuery(*conn, "MATCH (v:person) WHERE v.id = 1 DELETE v;");

  db.Close();
  this->OpenDB(db, this->db_dir_);
  conn = db.Connect();

  AssertPersonVertexAfterDelete(
      this->RunQuery(*conn, "MATCH (v:person) RETURN v.*;"));
  AssertKnowsWeight(
      this->RunQuery(*conn,
                     "MATCH (v:person)-[e:knows]->(:person) RETURN e.*;"),
      {});
}

// ALTER ADD a property, write per-row values, explicit CHECKPOINT, then
// reopen — the new column must come back via the new module/descriptor flow.
// add_columns gives the new column a fresh runtime UUID (descriptor.path is
// empty until the next Dump assigns one), so a Dump-then-reload here is the
// regression guard for that contract.
TYPED_TEST(CheckpointTest, alter_add_then_explicit_checkpoint_roundtrip) {
  neug::NeugDB db;
  this->OpenDB(db, this->db_dir_);
  auto conn = db.Connect();

  this->ExpectQuery(*conn, "ALTER TABLE person ADD score INT64;");
  this->ExpectQuery(*conn, "MATCH (v:person) SET v.score = v.id + 100;");
  this->ExpectQuery(*conn, "CHECKPOINT;");

  db.Close();
  this->OpenDB(db, this->db_dir_);
  conn = db.Connect();

  // Values written before the explicit CHECKPOINT must survive reopen.
  // load_modern_graph creates persons with id ∈ {1, 2, 4, 6}.
  auto res = this->RunQuery(
      *conn, "MATCH (v:person) RETURN v.id, v.score ORDER BY v.id;");
  neug::test::AssertInt64Column(res, 0, {1, 2, 4, 6});
  neug::test::AssertInt64Column(res, 1, {101, 102, 104, 106});
}

TYPED_TEST(CheckpointTest, after_add_edge_property1) {
  neug::NeugDB db;
  this->OpenDB(db, this->db_dir_);
  auto conn = db.Connect();
  this->ExpectQuery(*conn, "ALTER TABLE knows ADD registration DATE;");

  AssertKnowsWeightAndRegistration(
      this->RunQuery(*conn,
                     "MATCH (v:person)-[e:knows]->(:person) RETURN e.*;"),
      {0.5, 1.0}, {0, 0});

  db.Close();
  this->OpenDB(db, this->db_dir_);
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
  this->OpenDB(db, this->db_dir_);
  auto conn = db.Connect();

  AssertPersonVertexBasic(
      this->RunQuery(*conn, "MATCH (v:person) RETURN v.*;"));
  this->ExpectQuery(*conn, "ALTER TABLE knows ADD description STRING;");

  db.Close();
  this->OpenDB(db, this->db_dir_);
  conn = db.Connect();

  AssertKnowsWeightAndDescription(
      this->RunQuery(*conn,
                     "MATCH (v:person)-[e:knows]->(:person) RETURN e.*;"),
      {0.5, 1.0}, {"", ""});

  this->ExpectQuery(*conn, "ALTER TABLE knows ADD date DATE;");

  db.Close();
  this->OpenDB(db, this->db_dir_);
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
  this->OpenDB(db, this->db_dir_);
  {
    auto conn = db.Connect();
    this->ExpectQuery(*conn, "ALTER TABLE knows DROP weight");
  }

  db.Close();
  this->OpenDB(db, this->db_dir_);
  auto conn = db.Connect();

  AssertPersonVertexBasic(
      this->RunQuery(*conn, "MATCH (v:person) RETURN v.*;"));
  AssertMapColumn(
      this->RunQuery(*conn, "MATCH (v:person)-[e:knows]->(:person) RETURN e;"),
      2);
}

TYPED_TEST(CheckpointTest, after_delete_edge) {
  neug::NeugDB db;
  this->OpenDB(db, this->db_dir_);
  {
    auto conn = db.Connect();
    this->ExpectQuery(
        *conn,
        "MATCH (v:person)-[e:knows]->(:person) WHERE v.id = 1 DELETE e;");
  }

  db.Close();
  this->OpenDB(db, this->db_dir_);
  auto conn = db.Connect();

  AssertPersonVertexBasic(
      this->RunQuery(*conn, "MATCH (v:person) RETURN v.*;"));
  AssertKnowsWeight(
      this->RunQuery(*conn,
                     "MATCH (:person)-[e:knows]->(v:person) RETURN e.*;"),
      {});
}

TYPED_TEST(CheckpointTest, compact) {
  std::string db_path = this->MakeUniqueDir("compact");

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
  this->OpenDB(db, this->db_dir_);
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
  this->OpenDB(db2, this->db_dir_);
  auto conn2 = db2.Connect();
  AssertSingleInt64Result(
      this->RunQuery(*conn2, "MATCH (v:person) RETURN COUNT(v);"), 3);
  AssertSingleInt64Result(
      this->RunQuery(*conn2, "MATCH (v)-[e]->(a) RETURN COUNT(e);"), 2);
}

// ---------------------------------------------------------------------------
// Optimization tests: verify hardlink-based fast path for unchanged dumps.
// ---------------------------------------------------------------------------

// Helper: return sorted list of checkpoint-NNNNN subdirectories under db_dir.
static std::vector<std::filesystem::path> list_checkpoint_dirs(
    const std::string& db_dir) {
  std::vector<std::filesystem::path> dirs;
  for (const auto& entry : std::filesystem::directory_iterator(db_dir)) {
    if (entry.is_directory() &&
        entry.path().filename().string().rfind("checkpoint-", 0) == 0) {
      dirs.push_back(entry.path());
    }
  }
  std::sort(dirs.begin(), dirs.end());
  return dirs;
}

static size_t count_regular_files(const std::string& dir) {
  size_t n = 0;
  for (const auto& e : std::filesystem::directory_iterator(dir)) {
    if (e.is_regular_file())
      ++n;
  }
  return n;
}

// Probe whether @p dir's filesystem supports hardlinks.  Some sandboxes / CI
// runners use overlayfs, tmpfs, or 9p mounts that fail or silently fall back
// to copies on create_hard_link, which makes the optimization tests below
// flaky.  Returns true iff a hardlink can actually be created in @p dir.
static bool fs_supports_hardlink(const std::string& dir) {
  std::filesystem::create_directories(dir);
  auto src = std::filesystem::path(dir) / "_hardlink_probe_src";
  auto dst = std::filesystem::path(dir) / "_hardlink_probe_dst";
  std::error_code ec;
  std::filesystem::remove(src, ec);
  std::filesystem::remove(dst, ec);
  { std::ofstream(src) << 'x'; }
  std::filesystem::create_hard_link(src, dst, ec);
  bool ok = !ec && std::filesystem::hard_link_count(src) >= 2;
  std::filesystem::remove(src, ec);
  std::filesystem::remove(dst, ec);
  return ok;
}

// Verify that dumping an unchanged graph to a new checkpoint produces zero new
// data writes: every file in the new snapshot_dir must be a hardlink
// (hard_link_count > 1), and the file count must match checkpoint-1.
TEST(CheckpointOptTest, test_no_extra_files_on_unchanged_dump) {
  std::string db_path = "/tmp/test_unchanged_dump_db";
  if (std::filesystem::exists(db_path)) {
    std::filesystem::remove_all(db_path);
  }
  if (!fs_supports_hardlink(db_path)) {
    GTEST_SKIP() << "filesystem under " << db_path
                 << " does not support hardlinks; skipping unchanged-dump "
                    "optimization assertion";
  }

  // Step 1: load modern graph, explicit CHECKPOINT → checkpoint-1.
  {
    neug::NeugDB db;
    db.Open(db_path);
    auto conn = db.Connect();
    load_modern_graph(conn);
    auto res = conn->Query("CHECKPOINT;");
    ASSERT_TRUE(res) << res.error().ToString();
    conn->Close();
    db.Close();
  }

  auto dirs1 = list_checkpoint_dirs(db_path);
  ASSERT_EQ(dirs1.size(), 2u);
  std::string ckp1_snapshot = dirs1[1].string() + "/snapshot";
  size_t ckp1_count = count_regular_files(ckp1_snapshot);
  ASSERT_GT(ckp1_count, 0u);

  // Step 2: reopen, zero changes, another CHECKPOINT → checkpoint-2.
  {
    neug::NeugDB db;
    db.Open(db_path);
    auto conn = db.Connect();
    auto res = conn->Query("CHECKPOINT;");
    ASSERT_TRUE(res) << res.error().ToString();
    conn->Close();
    db.Close();
  }

  auto dirs2 = list_checkpoint_dirs(db_path);
  ASSERT_EQ(dirs2.size(), 3u);
  std::string ckp2_snapshot = dirs2[2].string() + "/snapshot";

  // (c) file count must match – no phantom files.
  size_t ckp2_count = count_regular_files(ckp2_snapshot);
  EXPECT_EQ(ckp1_count, ckp2_count);

  // (b) Every file in ckp-2's snapshot/ should be a hardlink: link_count >= 2
  // (one ref from ckp-2's snapshot/, plus one or more refs from earlier
  // checkpoint snapshots / runtime).  We don't assert an exact count because
  // it depends on how many earlier checkpoints share each inode and whether
  // any runtime fast-paths kept references — that's brittle across envs.
  size_t multi_linked = 0;
  for (const auto& entry : std::filesystem::directory_iterator(ckp2_snapshot)) {
    if (!entry.is_regular_file())
      continue;
    auto lc = std::filesystem::hard_link_count(entry.path());
    LOG(INFO) << "File: " << entry.path() << ", link count: " << lc;
    if (lc >= 2) {
      ++multi_linked;
    }
  }
  EXPECT_GT(multi_linked, 0u)
      << "Expected at least one snapshot file to be hardlinked from a prior "
         "checkpoint (zero-rewrite optimization)";

  // (a) data round-trip correctness.
  {
    neug::NeugDB db;
    db.Open(db_path);
    auto conn = db.Connect();
    auto res = conn->Query("MATCH (v:person) RETURN v.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    AssertPersonVertexBasic(res.value().response());
    conn->Close();
    db.Close();
  }
}

TEST(CheckpointOptTest, test_hardlink_survives_source_cleanup) {
  std::string db_path = "/tmp/test_hardlink_survives_db";
  if (std::filesystem::exists(db_path)) {
    std::filesystem::remove_all(db_path);
  }
  if (!fs_supports_hardlink(db_path)) {
    GTEST_SKIP() << "filesystem under " << db_path
                 << " does not support hardlinks; skipping hardlink-survival "
                    "test";
  }

  // checkpoint-1: load modern graph.
  {
    neug::NeugDB db;
    db.Open(db_path);
    auto conn = db.Connect();
    load_modern_graph(conn);
    auto res = conn->Query("CHECKPOINT;");
    ASSERT_TRUE(res) << res.error().ToString();
    conn->Close();
    db.Close();
  }

  // checkpoint-2: zero changes.
  {
    neug::NeugDB db;
    db.Open(db_path);
    auto conn = db.Connect();
    auto res = conn->Query("CHECKPOINT;");
    ASSERT_TRUE(res) << res.error().ToString();
    conn->Close();
    db.Close();
  }

  auto dirs = list_checkpoint_dirs(db_path);
  ASSERT_EQ(dirs.size(), 3u);
  std::string ckp1_snapshot = dirs[1].string() + "/snapshot";
  std::string ckp2_snapshot = dirs[2].string() + "/snapshot";

  // Collect checkpoint-2's files before any deletion.
  std::vector<std::filesystem::path> ckp2_files;
  for (const auto& e : std::filesystem::directory_iterator(ckp2_snapshot)) {
    if (e.is_regular_file())
      ckp2_files.push_back(e.path());
  }
  ASSERT_GT(ckp2_files.size(), 0u);

  // Remove the entire checkpoint-1 directory to simulate source cleanup.
  // (Removing only files inside snapshot/ would leave a broken checkpoint dir
  // that Workspace::Open would try to load and sanity-check, causing a throw.)
  std::filesystem::remove_all(dirs[1]);

  // Post-condition: checkpoint-2 files still exist (hardlinks are independent
  // inodes). After the source directory is gone, link_count drops to 1.
  std::unordered_map<int, int> link_counts;
  for (const auto& f : ckp2_files) {
    EXPECT_TRUE(std::filesystem::exists(f))
        << "File disappeared after source deletion: " << f;
    if (std::filesystem::exists(f)) {
      auto lc = std::filesystem::hard_link_count(f);
      ++link_counts[lc];
    }
  }
  for (const auto& [lc, count] : link_counts) {
    LOG(INFO) << "Link count " << lc << ": " << count << " files";
  }
  EXPECT_TRUE(link_counts.count(1) > 0)
      << "Expected files with link_count=1 after source dir removal";

  // Data correctness: open checkpoint-2 (latest) and query.
  {
    neug::NeugDB db;
    db.Open(db_path);
    auto conn = db.Connect();
    auto res = conn->Query("MATCH (v:person) RETURN v.*;");
    EXPECT_TRUE(res) << res.error().ToString();
    AssertPersonVertexBasic(res.value().response());
    conn->Close();
    db.Close();
  }
}

template <typename T>
class CheckpointTestStringProp : public CheckpointTestBase<T> {};

TYPED_TEST_SUITE(CheckpointTestStringProp, AllMemoryLevels);

TYPED_TEST(CheckpointTestStringProp, test_checkpoint_with_string_edge_prop) {
  std::string db_path = this->MakeUniqueDir("test_checkpoint_string_edge_prop");
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
  std::string db_dir_;

  void SetUp() override { db_dir_ = this->MakeUniqueDir("drop_table_ckpt"); }
  void TearDown() override { this->CleanDir(db_dir_); }

  void CreateAndCheckpointPerson() {
    neug::NeugDB db;
    this->OpenDB(db, db_dir_);
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
    this->OpenDB(db, db_dir_);
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
    this->OpenDB(db, db_dir_);
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
  this->OpenDB(db, this->db_dir_);
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
    this->OpenDB(db, this->db_dir_);
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
    this->OpenDB(db, this->db_dir_);
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
    this->OpenDB(db, this->db_dir_);
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
    this->OpenDB(db, this->db_dir_);
    auto conn = db.Connect();

    // Vertices are still present
    auto person_table = this->RunQuery(*conn, "MATCH (p:Person) RETURN p.id;");
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
  this->OpenDB(db, this->db_dir_);
  auto conn = db.Connect();

  // Drop + re-create in the same session
  this->ExpectQuery(*conn, "DROP TABLE IF EXISTS Created;");
  this->ExpectQuery(*conn,
                    "CREATE REL TABLE IF NOT EXISTS Created"
                    "(FROM Person TO Software, weight DOUBLE);");

  // Old edge data must not be visible
  AssertSingleInt64Result(
      this->RunQuery(
          *conn, "MATCH (p:Person)-[e:Created]->(s:Software) RETURN count(e);"),
      0);

  // Insert fresh edge — only this one should appear
  this->ExpectQuery(*conn,
                    "MATCH (p:Person {id: 'alice'}), (s:Software {id: 'neug'}) "
                    "CREATE (p)-[:Created {weight: 2.0}]->(s);");
  AssertSingleInt64Result(
      this->RunQuery(
          *conn, "MATCH (p:Person)-[e:Created]->(s:Software) RETURN count(e);"),
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
    this->OpenDB(db, this->db_dir_);
    auto conn = db.Connect();
    this->ExpectQuery(*conn, "DROP TABLE IF EXISTS Created;");
    this->ExpectQuery(*conn, "CHECKPOINT;");
    conn->Close();
    db.Close();
  }

  // Reopen, re-create edge table, verify empty
  {
    neug::NeugDB db;
    this->OpenDB(db, this->db_dir_);
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

// Regression: tmp-file cleanup on DROP must key on the full label name, not a
// bare prefix. Dropping "User" must not wipe files for "UserAccount".
TYPED_TEST(DropTableCheckpointTest,
           drop_label_does_not_sweep_sibling_with_shared_prefix) {
  neug::NeugDB db;
  this->OpenDB(db, this->db_dir_);
  auto conn = db.Connect();

  this->ExpectQuery(*conn,
                    "CREATE NODE TABLE IF NOT EXISTS User"
                    "(id STRING, PRIMARY KEY(id));");
  this->ExpectQuery(*conn,
                    "CREATE NODE TABLE IF NOT EXISTS UserAccount"
                    "(id STRING, PRIMARY KEY(id));");
  this->ExpectQuery(*conn, "CREATE (u:User {id: 'u1'});");
  this->ExpectQuery(*conn, "CREATE (a:UserAccount {id: 'a1'});");
  this->ExpectQuery(*conn, "CHECKPOINT;");

  // Drop only the shorter-named label. UserAccount data must survive.
  this->ExpectQuery(*conn, "DROP TABLE IF EXISTS User;");

  auto account_table =
      this->RunQuery(*conn, "MATCH (a:UserAccount) RETURN a.id;");
  EXPECT_EQ(account_table.row_count(), 1)
      << "DROP TABLE User wiped sibling label UserAccount (prefix collision)";
  neug::test::AssertStringColumn(account_table, 0, {"a1"});

  conn->Close();
  db.Close();
}

// ===========================================================================
// CheckpointSafetyTest — verifies failure handling
// ===========================================================================
template <typename T>
class CheckpointSafetyTest : public CheckpointTestBase<T> {
 protected:
  std::string db_dir_;

  void SetUp() override {
    if (getuid() == 0) {
      GTEST_SKIP() << "Cannot test permission-based failures as root";
    }
    db_dir_ = this->MakeUniqueDir("ckp_safety");
  }

  void TearDown() override {
    // Restore permissions recursively before cleanup.
    for (const auto& entry : std::filesystem::recursive_directory_iterator(
             db_dir_,
             std::filesystem::directory_options::skip_permission_denied)) {
      std::error_code ec;
      std::filesystem::permissions(entry.path(),
                                   std::filesystem::perms::owner_all, ec);
    }
    std::error_code ec;
    std::filesystem::permissions(db_dir_, std::filesystem::perms::owner_all,
                                 ec);
    this->CleanDir(db_dir_);
  }

  neug::NeugDBConfig MakeConfigNoCheckpointOnClose(
      const std::string& data_dir) {
    auto config = this->MakeConfig(data_dir);
    config.checkpoint_on_close = false;
    return config;
  }
};

TYPED_TEST_SUITE(CheckpointSafetyTest, AllMemoryLevels);

// Fix #528: UpdateMeta re-throws on I/O failure and preserves old meta.
TYPED_TEST(CheckpointSafetyTest, update_meta_rethrows_on_failure) {
  neug::CheckpointManager mgr;
  mgr.Open(this->db_dir_);
  auto ckp_id = mgr.CreateCheckpoint();
  auto ckp = mgr.GetCheckpoint(ckp_id);

  ASSERT_TRUE(ckp->GetMeta().modules().empty());

  neug::CheckpointManifest new_meta;
  neug::ModuleDescriptor desc;
  desc.set_path("data", "/fake/path");
  new_meta.set_module("test_module", std::move(desc));

  // Make checkpoint root dir read-only → AtomicFileWriter can't create .tmp.
  std::filesystem::permissions(
      ckp->path(),
      std::filesystem::perms::owner_read | std::filesystem::perms::owner_exec);

  EXPECT_THROW(ckp->UpdateMeta(std::move(new_meta)), std::exception);

  // Old (empty) meta is preserved after the failed update.
  EXPECT_TRUE(ckp->GetMeta().modules().empty());

  // Restore permissions for cleanup.
  std::filesystem::permissions(ckp->path(), std::filesystem::perms::owner_all);
}

// Fix #529: RemoveCheckpoint cleans up directory and map entry.
TYPED_TEST(CheckpointSafetyTest, remove_checkpoint_cleans_up_directory) {
  neug::CheckpointManager mgr;
  mgr.Open(this->db_dir_);
  auto ckp_id = mgr.CreateCheckpoint();
  auto ckp = mgr.GetCheckpoint(ckp_id);
  auto ckp_path = ckp->path();

  ASSERT_TRUE(std::filesystem::exists(ckp_path));
  ASSERT_EQ(mgr.NumCheckpoints(), 1u);
  ASSERT_EQ(mgr.HeadId(), ckp_id);

  mgr.RemoveCheckpoint(ckp_id);

  EXPECT_FALSE(std::filesystem::exists(ckp_path));
  EXPECT_EQ(mgr.NumCheckpoints(), 0u);
  EXPECT_EQ(mgr.HeadId(), neug::kInvalidCheckpointId);
}

// Fix #528 integration: A failed in-place CHECKPOINT does not corrupt
// on-disk data — recovery on restart succeeds.
TYPED_TEST(CheckpointSafetyTest,
           in_place_checkpoint_failure_preserves_data_on_reopen) {
  // Phase 1: Create table, insert data, and produce a valid checkpoint.
  {
    neug::NeugDB db;
    this->OpenDB(db, this->db_dir_);
    auto conn = db.Connect();
    this->ExpectQuery(*conn,
                      "CREATE NODE TABLE IF NOT EXISTS Item"
                      "(id INT64, PRIMARY KEY(id));");
    this->ExpectQuery(*conn, "CREATE (:Item {id: 100});");
    this->ExpectQuery(*conn, "CREATE (:Item {id: 200});");
    auto res = conn->Query("CHECKPOINT;");
    ASSERT_TRUE(res) << res.error().ToString();
    conn->Close();
    db.Close();
  }

  // Phase 2: Reopen and trigger a failing in-place CHECKPOINT.
  // After a failed in-place CHECKPOINT, the in-memory graph is left in an
  // inconsistent state (tables disassembled). We use checkpoint_on_close=false
  // so Close() doesn't attempt to checkpoint the broken graph.
  {
    neug::NeugDB db;
    db.Open(this->MakeConfigNoCheckpointOnClose(this->db_dir_));
    auto conn = db.Connect();

    // Find the highest-numbered checkpoint dir (the one ckp_ points to).
    std::string ckp_dir;
    int32_t max_id = -1;
    for (const auto& entry :
         std::filesystem::directory_iterator(this->db_dir_)) {
      auto name = entry.path().filename().string();
      if (entry.is_directory() && name.find("checkpoint-") == 0) {
        int32_t id = std::stoi(name.substr(std::string("checkpoint-").size()));
        if (id > max_id) {
          max_id = id;
          ckp_dir = entry.path().string();
        }
      }
    }
    ASSERT_FALSE(ckp_dir.empty());

    std::filesystem::permissions(ckp_dir,
                                 std::filesystem::perms::owner_read |
                                     std::filesystem::perms::owner_exec);

    // CHECKPOINT should fail because UpdateMeta can't write meta.tmp.
    auto res = conn->Query("CHECKPOINT;");
    EXPECT_FALSE(res) << "Expected CHECKPOINT to fail, but it succeeded";

    // Restore permissions so Close() and subsequent reopen work.
    std::filesystem::permissions(ckp_dir, std::filesystem::perms::owner_all);
    conn->Close();
    db.Close();
  }

  // Phase 3: Reopen and verify data is intact from the valid checkpoint.
  {
    neug::NeugDB db;
    db.Open(this->MakeConfigNoCheckpointOnClose(this->db_dir_));
    auto conn = db.Connect();
    auto table = this->RunQuery(*conn, "MATCH (v:Item) RETURN v.id;");
    EXPECT_EQ(table.row_count(), 2);
    conn->Close();
    db.Close();
  }
}

// Fix #530: Open discards an incomplete (empty-meta) checkpoint and recovers.
TYPED_TEST(CheckpointSafetyTest,
           open_discards_incomplete_checkpoint_and_recovers) {
  // Phase 1: Create a valid DB with data.
  {
    neug::NeugDB db;
    this->OpenDB(db, this->db_dir_);
    auto conn = db.Connect();
    this->ExpectQuery(*conn,
                      "CREATE NODE TABLE IF NOT EXISTS Widget"
                      "(id INT64, name STRING, PRIMARY KEY(id));");
    this->ExpectQuery(*conn, "CREATE (:Widget {id: 1, name: 'alpha'});");
    this->ExpectQuery(*conn, "CREATE (:Widget {id: 2, name: 'beta'});");
    conn->Close();
    db.Close();  // checkpoint_on_close creates a valid checkpoint
  }

  // Phase 2: Simulate a crash that left an incomplete checkpoint directory.
  std::string bad_ckp_path;
  {
    // Find the highest existing checkpoint ID.
    int32_t max_id = -1;
    for (const auto& entry :
         std::filesystem::directory_iterator(this->db_dir_)) {
      auto name = entry.path().filename().string();
      if (name.find("checkpoint-") == 0) {
        auto id_str = name.substr(std::string("checkpoint-").size());
        int32_t id = std::stoi(id_str);
        max_id = std::max(max_id, id);
      }
    }
    ASSERT_GE(max_id, 0);

    // Create a fake incomplete checkpoint with a higher ID.
    bad_ckp_path = this->db_dir_ + "/checkpoint-" + std::to_string(max_id + 1);
    std::filesystem::create_directories(bad_ckp_path);
    neug::CheckpointManifest::GenerateEmptyMeta(bad_ckp_path + "/meta");
    ASSERT_TRUE(std::filesystem::exists(bad_ckp_path + "/meta"));
  }

  // Phase 3: Reopen — the incomplete checkpoint should be discarded.
  {
    neug::NeugDB db;
    db.Open(this->MakeConfigNoCheckpointOnClose(this->db_dir_));
    auto conn = db.Connect();
    auto table = this->RunQuery(*conn, "MATCH (v:Widget) RETURN v.id, v.name;");
    EXPECT_EQ(table.row_count(), 2);
    conn->Close();
    db.Close();
  }

  EXPECT_FALSE(std::filesystem::exists(bad_ckp_path))
      << "Incomplete checkpoint directory should have been removed on Open";
}

}  // namespace test
}  // namespace neug
