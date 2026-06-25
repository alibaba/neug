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
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <vector>
#include "column_assertions.h"
#include "neug/config.h"
#include "neug/main/connection.h"
#include "neug/main/neug_db.h"
#include "neug/server/neug_db_service.h"
#include "neug/storages/allocators.h"
#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_file_manager.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/container/file_header.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/module_descriptor.h"
#include "neug/utils/property/column.h"
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
    auto name = entry.path().filename().string();
    if (entry.is_directory() && name.rfind("checkpoint-", 0) == 0 &&
        name.find(".next") == std::string::npos) {
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

static size_t count_valid_checkpoint_dirs(const std::string& db_dir) {
  size_t n = 0;
  for (const auto& checkpoint_dir : list_checkpoint_dirs(db_dir)) {
    neug::CheckpointManifest meta;
    meta.Load((checkpoint_dir / "meta").string());
    if (meta.has_schema()) {
      ++n;
    }
  }
  return n;
}

static std::string make_checkpoint_gc_test_dir(const std::string& prefix) {
  const auto* test_info =
      ::testing::UnitTest::GetInstance()->current_test_info();
  auto dir =
      std::filesystem::temp_directory_path() /
      (prefix + "_" + test_info->test_suite_name() + "_" + test_info->name());
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
  std::filesystem::create_directories(dir);
  return dir.string();
}

static void write_valid_empty_manifest(std::shared_ptr<neug::Checkpoint> ckp) {
  neug::CheckpointManifest meta;
  neug::Schema schema;
  meta.SetSchema(schema);
  ckp->UpdateMeta(std::move(meta));
}

static void create_valid_checkpoint(neug::CheckpointManager& mgr) {
  auto id = mgr.CreateStagingCheckpoint();
  write_valid_empty_manifest(mgr.GetCheckpoint(id));
  mgr.PublishStagingCheckpoint(id);
}

static std::string read_checkpoint_pointer(const std::string& db_path) {
  std::ifstream input(std::filesystem::path(db_path) / "CHECKPOINT");
  std::string value;
  std::getline(input, value);
  return value;
}

static bool has_staging_checkpoint_dirs(const std::string& db_path) {
  for (const auto& entry : std::filesystem::directory_iterator(db_path)) {
    auto name = entry.path().filename().string();
    if (entry.is_directory() && name.rfind("checkpoint-", 0) == 0 &&
        name.ends_with(".next")) {
      return true;
    }
  }
  return false;
}

static void AssertSingleCurrentCheckpoint(const std::string& db_path) {
  auto dirs = list_checkpoint_dirs(db_path);
  ASSERT_EQ(dirs.size(), 1u);
  auto pointer = read_checkpoint_pointer(db_path);
  ASSERT_FALSE(pointer.empty());
  auto pointer_path = std::filesystem::path(db_path) / pointer;
  EXPECT_TRUE(std::filesystem::exists(pointer_path));
  EXPECT_EQ(pointer_path.filename(), dirs[0].filename());
  EXPECT_FALSE(has_staging_checkpoint_dirs(db_path));
  EXPECT_FALSE(std::filesystem::exists(std::filesystem::path(db_path) /
                                       "CHECKPOINT.tmp"));
}

static std::string read_raw_file(const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  EXPECT_TRUE(input.is_open()) << path;
  return std::string(std::istreambuf_iterator<char>(input),
                     std::istreambuf_iterator<char>());
}

static std::string read_container_payload_prefix(const std::string& path,
                                                 size_t size) {
  std::ifstream input(path, std::ios::binary);
  EXPECT_TRUE(input.is_open()) << path;
  input.seekg(sizeof(neug::FileHeader), std::ios::beg);
  std::string payload(size, '\0');
  input.read(payload.data(), static_cast<std::streamsize>(size));
  EXPECT_EQ(input.gcount(), static_cast<std::streamsize>(size));
  return payload;
}

static void write_raw_file(const std::string& path, std::string_view payload) {
  std::ofstream output(path, std::ios::binary);
  ASSERT_TRUE(output.is_open()) << path;
  output.write(payload.data(), static_cast<std::streamsize>(payload.size()));
  ASSERT_TRUE(output.good()) << path;
}

static void write_container_payload(neug::IDataContainer& container,
                                    std::string_view payload) {
  ASSERT_GE(container.GetDataSize(), payload.size());
  std::memcpy(container.GetData(), payload.data(), payload.size());
}

static std::vector<std::filesystem::path> list_regular_files_in_dir(
    const std::filesystem::path& dir) {
  std::vector<std::filesystem::path> files;
  for (const auto& entry : std::filesystem::directory_iterator(dir)) {
    if (entry.is_regular_file()) {
      files.push_back(entry.path());
    }
  }
  std::sort(files.begin(), files.end());
  return files;
}

static bool is_equivalent_to_any(
    const std::filesystem::path& path,
    const std::vector<std::filesystem::path>& candidates) {
  for (const auto& candidate : candidates) {
    std::error_code ec;
    if (std::filesystem::equivalent(path, candidate, ec) && !ec) {
      return true;
    }
  }
  return false;
}

static size_t count_mutable_csr_edges(const neug::MutableCsr<int32_t>& csr,
                                      size_t vertex_count) {
  size_t edge_count = 0;
  auto view = csr.get_generic_view(0);
  for (neug::vid_t src = 0; src < static_cast<neug::vid_t>(vertex_count);
       ++src) {
    auto edges = view.get_edges(src);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      ++edge_count;
    }
  }
  return edge_count;
}

static void open_db_for_checkpoint_test(neug::NeugDB& db,
                                        const std::string& db_path) {
  neug::NeugDBConfig config(db_path);
  config.checkpoint_on_close = true;
  config.enable_auto_compaction = false;
  db.Open(config);
}

static void seed_checkpoint_opt_graph(std::shared_ptr<neug::Connection> conn) {
  auto res = conn->Query("CREATE NODE TABLE Item(id INT64, PRIMARY KEY(id));");
  ASSERT_TRUE(res) << res.error().ToString();
  res = conn->Query("CREATE (:Item {id: 1});");
  ASSERT_TRUE(res) << res.error().ToString();
  res = conn->Query("CREATE (:Item {id: 2});");
  ASSERT_TRUE(res) << res.error().ToString();
}

static void assert_checkpoint_opt_graph(neug::NeugDB& db,
                                        const std::string& db_path) {
  open_db_for_checkpoint_test(db, db_path);
  auto conn = db.Connect();
  auto res = conn->Query("MATCH (v:Item) RETURN COUNT(v);");
  ASSERT_TRUE(res) << res.error().ToString();
  AssertSingleInt64Result(res.value().response(), 2);
  conn->Close();
  db.Close();
}

TEST(CheckpointGCTest, publish_staging_checkpoint_updates_root_pointer) {
  auto db_path = make_checkpoint_gc_test_dir("checkpoint_gc");
  neug::CheckpointManager mgr;
  mgr.Open(db_path);

  create_valid_checkpoint(mgr);
  ASSERT_EQ(mgr.CurrentCheckpointId(), 0);
  EXPECT_EQ(read_checkpoint_pointer(db_path), "checkpoint-0");

  auto id = mgr.CreateStagingCheckpoint();
  write_valid_empty_manifest(mgr.GetCheckpoint(id));
  mgr.PublishStagingCheckpoint(id);

  EXPECT_TRUE(mgr.HasCurrentCheckpoint());
  EXPECT_EQ(mgr.CurrentCheckpointId(), 1);
  EXPECT_EQ(read_checkpoint_pointer(db_path), "checkpoint-1");
  EXPECT_FALSE(std::filesystem::exists(db_path + "/checkpoint-0"));
  EXPECT_TRUE(std::filesystem::exists(db_path + "/checkpoint-1"));
}

TEST(CheckpointGCTest, create_staging_checkpoint_rejects_active_staging) {
  auto db_path = make_checkpoint_gc_test_dir("checkpoint_gc");
  neug::CheckpointManager mgr;
  mgr.Open(db_path);

  auto id = mgr.CreateStagingCheckpoint();
  EXPECT_THROW(mgr.CreateStagingCheckpoint(), std::exception);

  mgr.DiscardStagingCheckpoint(id);
}

TEST(CheckpointGCTest, discard_staging_checkpoint_allows_new_staging) {
  auto db_path = make_checkpoint_gc_test_dir("checkpoint_gc");
  neug::CheckpointManager mgr;
  mgr.Open(db_path);

  auto id = mgr.CreateStagingCheckpoint();
  auto path = mgr.GetCheckpoint(id)->path();
  mgr.DiscardStagingCheckpoint(id);

  EXPECT_FALSE(std::filesystem::exists(path));
  auto next_id = mgr.CreateStagingCheckpoint();
  EXPECT_EQ(next_id, id);
  mgr.DiscardStagingCheckpoint(next_id);
}

TEST(CheckpointGCTest, publish_staging_checkpoint_rejects_missing_schema) {
  auto db_path = make_checkpoint_gc_test_dir("checkpoint_gc");
  neug::CheckpointManager mgr;
  mgr.Open(db_path);

  auto id = mgr.CreateStagingCheckpoint();
  EXPECT_THROW(mgr.PublishStagingCheckpoint(id), std::exception);

  mgr.DiscardStagingCheckpoint(id);
}

TEST(CheckpointGCTest, publish_staging_checkpoint_rejects_non_staging_id) {
  auto db_path = make_checkpoint_gc_test_dir("checkpoint_gc");
  neug::CheckpointManager mgr;
  mgr.Open(db_path);

  create_valid_checkpoint(mgr);

  EXPECT_THROW(mgr.PublishStagingCheckpoint(0), std::exception);
  EXPECT_EQ(mgr.CurrentCheckpointId(), 0);
  EXPECT_EQ(read_checkpoint_pointer(db_path), "checkpoint-0");
}

TEST(CheckpointGCTest, publish_staging_checkpoint_rejects_extra_staging_dir) {
  auto db_path = make_checkpoint_gc_test_dir("checkpoint_gc");
  neug::CheckpointManager mgr;
  mgr.Open(db_path);

  auto id = mgr.CreateStagingCheckpoint();
  write_valid_empty_manifest(mgr.GetCheckpoint(id));
  auto extra_staging = std::filesystem::path(db_path) / "checkpoint-999.next";
  std::filesystem::create_directories(extra_staging);

  EXPECT_THROW(mgr.PublishStagingCheckpoint(id), std::exception);
  EXPECT_TRUE(std::filesystem::exists(extra_staging));

  mgr.DiscardStagingCheckpoint(id);
  std::filesystem::remove_all(extra_staging);
}

TEST(CheckpointGCTest, open_migrates_legacy_checkpoints_to_root_pointer) {
  auto db_path = make_checkpoint_gc_test_dir("checkpoint_gc");
  for (int id = 0; id < 3; ++id) {
    auto path =
        std::filesystem::path(db_path) / ("checkpoint-" + std::to_string(id));
    auto ckp = neug::Checkpoint::Open(path.string(), id);
    write_valid_empty_manifest(ckp);
  }

  neug::CheckpointManager mgr;
  mgr.Open(db_path);

  EXPECT_TRUE(mgr.HasCurrentCheckpoint());
  EXPECT_EQ(mgr.CurrentCheckpointId(), 2);
  EXPECT_EQ(read_checkpoint_pointer(db_path), "checkpoint-2");
  EXPECT_FALSE(std::filesystem::exists(db_path + "/checkpoint-0"));
  EXPECT_FALSE(std::filesystem::exists(db_path + "/checkpoint-1"));
  EXPECT_TRUE(std::filesystem::exists(db_path + "/checkpoint-2"));
}

TEST(CheckpointGCTest, open_cleans_staging_and_checkpoint_gc_trash) {
  auto db_path = make_checkpoint_gc_test_dir("checkpoint_gc");
  {
    neug::CheckpointManager mgr;
    mgr.Open(db_path);
    create_valid_checkpoint(mgr);
    mgr.Close();
  }

  auto trash = std::filesystem::path(db_path) / ".checkpoint-gc" / "leftover";
  std::filesystem::create_directories(trash);
  { std::ofstream(trash / "leftover") << "stale"; }
  auto staging = std::filesystem::path(db_path) / "checkpoint-999.next";
  std::filesystem::create_directories(staging);
  { std::ofstream(std::filesystem::path(db_path) / "CHECKPOINT.tmp") << "x"; }
  ASSERT_TRUE(std::filesystem::exists(trash));
  ASSERT_TRUE(std::filesystem::exists(staging));

  neug::CheckpointManager mgr;
  mgr.Open(db_path);

  EXPECT_EQ(mgr.CurrentCheckpointId(), 0);
  EXPECT_FALSE(std::filesystem::exists(trash));
  EXPECT_FALSE(std::filesystem::exists(staging));
  EXPECT_FALSE(std::filesystem::exists(std::filesystem::path(db_path) /
                                       "CHECKPOINT.tmp"));
  EXPECT_TRUE(std::filesystem::exists(db_path + "/checkpoint-0"));
}

TEST(CheckpointGCTest, publish_removes_old_checkpoint_with_old_refs_alive) {
  auto db_path = make_checkpoint_gc_test_dir("checkpoint_gc");
  neug::CheckpointManager mgr;
  mgr.Open(db_path);
  create_valid_checkpoint(mgr);

  auto old_ckp = mgr.GetCheckpoint(0);
  auto old_path = old_ckp->path();
  auto old_container =
      old_ckp->CreateRuntimeContainer(32, neug::MemoryLevel::kSyncToFile);
  auto old_runtime_path = old_container->GetPath();
  ASSERT_TRUE(std::filesystem::exists(old_path));
  ASSERT_TRUE(std::filesystem::exists(old_runtime_path));

  auto id = mgr.CreateStagingCheckpoint();
  write_valid_empty_manifest(mgr.GetCheckpoint(id));
  auto new_ckp = mgr.PublishStagingCheckpoint(id);

  EXPECT_EQ(mgr.CurrentCheckpointId(), id);
  EXPECT_EQ(read_checkpoint_pointer(db_path),
            "checkpoint-" + std::to_string(id));
  EXPECT_FALSE(std::filesystem::exists(old_path));
  EXPECT_FALSE(std::filesystem::exists(old_runtime_path));
  EXPECT_TRUE(std::filesystem::exists(new_ckp->path()));

  old_container.reset();
  old_ckp.reset();
  EXPECT_TRUE(std::filesystem::exists(new_ckp->path()));
}

TEST(CheckpointGCTest, neugdb_keeps_single_current_checkpoint) {
  auto db_path = make_checkpoint_gc_test_dir("checkpoint_gc");
  constexpr size_t kCheckpointRounds = 4;
  for (size_t i = 0; i < kCheckpointRounds; ++i) {
    neug::NeugDB db;
    neug::NeugDBConfig config(db_path);
    config.checkpoint_on_close = true;
    db.Open(config);
    auto conn = db.Connect();
    auto res = conn->Query(
        "CREATE NODE TABLE IF NOT EXISTS Item"
        "(id INT64, PRIMARY KEY(id));");
    ASSERT_TRUE(res) << res.error().ToString();
    res = conn->Query("CREATE (:Item {id: " + std::to_string(i) + "});");
    ASSERT_TRUE(res) << res.error().ToString();
    conn->Close();
    db.Close();
  }

  ASSERT_EQ(count_valid_checkpoint_dirs(db_path), 1u);
  ASSERT_TRUE(std::filesystem::exists(std::filesystem::path(db_path) /
                                      read_checkpoint_pointer(db_path)));

  {
    neug::NeugDB db;
    neug::NeugDBConfig config(db_path);
    config.checkpoint_on_close = false;

    db.Open(config);
    auto conn = db.Connect();
    auto table = conn->Query("MATCH (v:Item) RETURN count(v);");
    ASSERT_TRUE(table) << table.error().ToString();
    AssertSingleInt64Result(table.value().response(), 4);
    conn->Close();
    db.Close();
  }
}

TEST(CheckpointFileManagerTest,
     SyncToFileRuntimeCommitMaterializesIndependentSnapshotAndGcRuntime) {
  auto db_path = make_checkpoint_gc_test_dir("checkpoint_file_manager");
  auto snapshot_dir = std::filesystem::path(db_path) / "snapshot";
  auto runtime_dir = std::filesystem::path(db_path) / "runtime";
  std::filesystem::create_directories(snapshot_dir);
  std::filesystem::create_directories(runtime_dir);
  neug::CheckpointFileManager mgr(snapshot_dir.string(), runtime_dir.string());

  auto container =
      mgr.CreateRuntimeContainer(64, neug::MemoryLevel::kSyncToFile);
  auto runtime_path = container->GetPath();
  ASSERT_TRUE(std::filesystem::exists(runtime_path));

  constexpr std::string_view kSnapshotPayload = "snapshot-payload-v1";
  constexpr std::string_view kRuntimePayload = "runtime-payload-v2!";
  static_assert(kSnapshotPayload.size() == kRuntimePayload.size());
  write_container_payload(*container, kSnapshotPayload);

  auto snapshot_path = mgr.Commit(*container);
  ASSERT_TRUE(std::filesystem::exists(snapshot_path));
  EXPECT_NE(snapshot_path, runtime_path);
  EXPECT_EQ(
      read_container_payload_prefix(snapshot_path, kSnapshotPayload.size()),
      std::string(kSnapshotPayload));

  write_container_payload(*container, kRuntimePayload);
  container->Sync();
  EXPECT_EQ(
      read_container_payload_prefix(snapshot_path, kSnapshotPayload.size()),
      std::string(kSnapshotPayload));
  EXPECT_TRUE(std::filesystem::exists(runtime_path));

  auto alias = container;
  container.reset();
  EXPECT_TRUE(std::filesystem::exists(runtime_path));
  alias.reset();
  EXPECT_FALSE(std::filesystem::exists(runtime_path));
  EXPECT_TRUE(std::filesystem::exists(snapshot_path));
}

TEST(CheckpointFileManagerTest,
     CleanSyncToFileOpenFileCommitMaterializesIndependentSnapshot) {
  auto db_path = make_checkpoint_gc_test_dir("checkpoint_file_manager");
  auto snapshot_dir = std::filesystem::path(db_path) / "snapshot";
  auto runtime_dir = std::filesystem::path(db_path) / "runtime";
  std::filesystem::create_directories(snapshot_dir);
  std::filesystem::create_directories(runtime_dir);
  neug::CheckpointFileManager mgr(snapshot_dir.string(), runtime_dir.string());

  constexpr std::string_view kSnapshotPayload = "clean-snapshot-v1";
  constexpr std::string_view kRuntimePayload = "clean-runtime--v2";
  static_assert(kSnapshotPayload.size() == kRuntimePayload.size());
  auto source = mgr.CreateRuntimeContainer(64, neug::MemoryLevel::kSyncToFile);
  write_container_payload(*source, kSnapshotPayload);
  auto source_snapshot = mgr.Commit(*source);
  source.reset();

  auto container =
      mgr.OpenFile(source_snapshot, neug::MemoryLevel::kSyncToFile);
  auto runtime_path = container->GetPath();
  ASSERT_TRUE(std::filesystem::exists(runtime_path));
  EXPECT_FALSE(container->IsDirty());

  auto committed_snapshot = mgr.Commit(*container);
  ASSERT_TRUE(std::filesystem::exists(committed_snapshot));
  EXPECT_NE(committed_snapshot, runtime_path);
  EXPECT_EQ(read_container_payload_prefix(committed_snapshot,
                                          kSnapshotPayload.size()),
            std::string(kSnapshotPayload));

  write_container_payload(*container, kRuntimePayload);
  container->Sync();
  EXPECT_EQ(read_container_payload_prefix(committed_snapshot,
                                          kSnapshotPayload.size()),
            std::string(kSnapshotPayload));
  EXPECT_EQ(
      read_container_payload_prefix(source_snapshot, kSnapshotPayload.size()),
      std::string(kSnapshotPayload));

  container.reset();
  EXPECT_FALSE(std::filesystem::exists(runtime_path));
  EXPECT_TRUE(std::filesystem::exists(committed_snapshot));
}

TEST(CheckpointFileManagerTest,
     ManualRuntimeFileHandleCommitAndAbandonCleanup) {
  auto db_path = make_checkpoint_gc_test_dir("checkpoint_file_manager");
  auto snapshot_dir = std::filesystem::path(db_path) / "snapshot";
  auto runtime_dir = std::filesystem::path(db_path) / "runtime";
  std::filesystem::create_directories(snapshot_dir);
  std::filesystem::create_directories(runtime_dir);
  neug::CheckpointFileManager mgr(snapshot_dir.string(), runtime_dir.string());

  std::string abandoned_path;
  {
    auto file = mgr.CreateRuntimeFile();
    abandoned_path = file.path();
    write_raw_file(abandoned_path, "abandoned");
    ASSERT_TRUE(std::filesystem::exists(abandoned_path));
  }
  EXPECT_FALSE(std::filesystem::exists(abandoned_path));

  std::string runtime_path;
  std::string snapshot_path;
  {
    auto file = mgr.CreateRuntimeFile();
    runtime_path = file.path();
    write_raw_file(runtime_path, "committed-runtime-file");
    snapshot_path = mgr.CommitRuntimeFile(std::move(file));
  }

  EXPECT_FALSE(std::filesystem::exists(runtime_path));
  ASSERT_TRUE(std::filesystem::exists(snapshot_path));
  EXPECT_EQ(read_raw_file(snapshot_path), "committed-runtime-file");
}

TEST(CheckpointFileManagerTest,
     LinkToSnapshotHardlinksRetiredRuntimeFileByContract) {
  auto db_path = make_checkpoint_gc_test_dir("checkpoint_file_manager");
  auto old_checkpoint_dir = std::filesystem::path(db_path) / "checkpoint-0";
  auto old_runtime_dir = old_checkpoint_dir / "runtime";
  auto new_checkpoint_dir = std::filesystem::path(db_path) / "checkpoint-1";
  auto new_snapshot_dir = new_checkpoint_dir / "snapshot";
  auto new_runtime_dir = new_checkpoint_dir / "runtime";
  std::filesystem::create_directories(old_runtime_dir);
  std::filesystem::create_directories(new_snapshot_dir);
  std::filesystem::create_directories(new_runtime_dir);
  neug::CheckpointFileManager mgr(new_snapshot_dir.string(),
                                  new_runtime_dir.string());

  // The caller contract for LinkToSnapshot() is that this runtime file belongs
  // to a retired checkpoint and will never be written again.
  auto old_runtime_path = old_runtime_dir / "old-runtime-file";
  write_raw_file(old_runtime_path.string(), "previous-runtime-payload");

  auto linked_snapshot = mgr.LinkToSnapshot(old_runtime_path.string());
  ASSERT_TRUE(std::filesystem::exists(linked_snapshot));
  EXPECT_TRUE(std::filesystem::equivalent(old_runtime_path, linked_snapshot));
  EXPECT_EQ(read_raw_file(linked_snapshot), "previous-runtime-payload");

  std::filesystem::remove_all(old_checkpoint_dir);
  ASSERT_TRUE(std::filesystem::exists(linked_snapshot));
  EXPECT_EQ(read_raw_file(linked_snapshot), "previous-runtime-payload");
}

TEST(CheckpointFileManagerTest,
     StringColumnFastPathHardlinksRetiredRuntimeFiles) {
  auto db_path = make_checkpoint_gc_test_dir("checkpoint_file_manager");
  auto old_ckp = neug::Checkpoint::Open(
      (std::filesystem::path(db_path) / "checkpoint-0").string(), 0);
  auto new_ckp = neug::Checkpoint::Open(
      (std::filesystem::path(db_path) / "checkpoint-1").string(), 1);

  neug::StringColumn original;
  original.Open(*old_ckp, neug::ModuleDescriptor{},
                neug::MemoryLevel::kSyncToFile);
  original.resize(3);
  original.set_value(0, "alpha");
  original.set_value(1, "bravo");
  original.set_value(2, "charlie");
  neug::CheckpointManifest manifest;
  original.Dump(*old_ckp, manifest, "test");
  auto old_desc = *manifest.module("test");
  original.Close();

  neug::StringColumn clean;
  clean.Open(*old_ckp, old_desc, neug::MemoryLevel::kSyncToFile);
  ASSERT_TRUE(clean.is_data_unmodified());
  auto retired_runtime_files =
      list_regular_files_in_dir(old_ckp->runtime_dir());
  ASSERT_GE(retired_runtime_files.size(), 2u);

  neug::CheckpointManifest new_manifest;
  clean.Dump(*new_ckp, new_manifest, "test");
  auto new_desc = *new_manifest.module("test");
  auto new_items = new_desc.get_path(neug::ModuleDescriptor::kItemsPath);
  auto new_data = new_desc.get_path(neug::ModuleDescriptor::kDataPath);
  ASSERT_TRUE(new_items.has_value());
  ASSERT_TRUE(new_data.has_value());
  EXPECT_TRUE(is_equivalent_to_any(*new_items, retired_runtime_files));
  EXPECT_TRUE(is_equivalent_to_any(*new_data, retired_runtime_files));

  std::filesystem::remove_all(old_ckp->path());
  ASSERT_TRUE(std::filesystem::exists(*new_items));
  ASSERT_TRUE(std::filesystem::exists(*new_data));

  neug::StringColumn reopened;
  reopened.Open(*new_ckp, new_desc, neug::MemoryLevel::kInMemory);
  ASSERT_EQ(reopened.size(), 3u);
  EXPECT_EQ(reopened.get_view(0), "alpha");
  EXPECT_EQ(reopened.get_view(1), "bravo");
  EXPECT_EQ(reopened.get_view(2), "charlie");
}

TEST(CheckpointFileManagerTest,
     MutableCsrFastPathHardlinksRetiredNbrListRuntimeFile) {
  auto db_path = make_checkpoint_gc_test_dir("checkpoint_file_manager");
  auto old_ckp = neug::Checkpoint::Open(
      (std::filesystem::path(db_path) / "checkpoint-0").string(), 0);
  auto new_ckp = neug::Checkpoint::Open(
      (std::filesystem::path(db_path) / "checkpoint-1").string(), 1);

  neug::MutableCsr<int32_t> original;
  original.Open(*old_ckp, neug::ModuleDescriptor{},
                neug::MemoryLevel::kSyncToFile);
  original.resize(4);
  original.batch_put_edges({0, 1, 1}, {1, 2, 3}, {10, 20, 30}, 0);
  neug::CheckpointManifest manifest;
  original.Dump(*old_ckp, manifest, "test");
  auto old_desc = *manifest.module("test");
  original.Close();

  neug::MutableCsr<int32_t> clean;
  clean.Open(*old_ckp, old_desc, neug::MemoryLevel::kSyncToFile);
  auto retired_runtime_files =
      list_regular_files_in_dir(old_ckp->runtime_dir());
  ASSERT_GE(retired_runtime_files.size(), 3u);

  neug::CheckpointManifest new_manifest;
  clean.Dump(*new_ckp, new_manifest, "test");
  auto new_desc = *new_manifest.module("test");
  auto new_nbr_list = new_desc.get_path(neug::ModuleDescriptor::kNbrListPath);
  ASSERT_TRUE(new_nbr_list.has_value());
  EXPECT_TRUE(is_equivalent_to_any(*new_nbr_list, retired_runtime_files));

  std::filesystem::remove_all(old_ckp->path());
  ASSERT_TRUE(std::filesystem::exists(*new_nbr_list));

  neug::MutableCsr<int32_t> reopened;
  reopened.Open(*new_ckp, new_desc, neug::MemoryLevel::kInMemory);
  EXPECT_EQ(reopened.edge_num(), 3u);
  EXPECT_EQ(count_mutable_csr_edges(reopened, 4), 3u);
}

TEST(CheckpointFileManagerTest,
     RuntimeContainerOutlivesCheckpointUntilLastRef) {
  auto db_path = make_checkpoint_gc_test_dir("checkpoint_file_manager");
  auto checkpoint_path = std::filesystem::path(db_path) / "checkpoint-0";
  std::shared_ptr<neug::IDataContainer> container;
  std::string runtime_path;

  {
    auto checkpoint = neug::Checkpoint::Open(checkpoint_path.string(), 0);
    container =
        checkpoint->CreateRuntimeContainer(64, neug::MemoryLevel::kSyncToFile);
    runtime_path = container->GetPath();
    write_container_payload(*container, "runtime-survives-checkpoint");
    ASSERT_TRUE(std::filesystem::exists(runtime_path));
  }

  EXPECT_TRUE(std::filesystem::exists(runtime_path));
  container.reset();
  EXPECT_FALSE(std::filesystem::exists(runtime_path));
}

// Verify that dumping an unchanged graph to a new checkpoint keeps the current
// snapshot file inventory stable under the single-checkpoint layout.
TEST(CheckpointOptTest, test_no_extra_files_on_unchanged_dump) {
  std::string db_path = "/tmp/test_unchanged_dump_db";
  if (std::filesystem::exists(db_path)) {
    std::filesystem::remove_all(db_path);
  }

  // Step 1: create a small graph and produce the current checkpoint.
  {
    neug::NeugDB db;
    open_db_for_checkpoint_test(db, db_path);
    auto conn = db.Connect();
    seed_checkpoint_opt_graph(conn);
    auto res = conn->Query("CHECKPOINT;");
    ASSERT_TRUE(res) << res.error().ToString();
    conn->Close();
    db.Close();
  }

  auto dirs1 = list_checkpoint_dirs(db_path);
  ASSERT_EQ(dirs1.size(), 1u);
  std::string ckp1_snapshot = dirs1[0].string() + "/snapshot";
  size_t ckp1_count = count_regular_files(ckp1_snapshot);
  ASSERT_GT(ckp1_count, 0u);

  // Step 2: reopen, zero changes, another CHECKPOINT.
  {
    neug::NeugDB db;
    open_db_for_checkpoint_test(db, db_path);
    auto conn = db.Connect();
    auto res = conn->Query("CHECKPOINT;");
    ASSERT_TRUE(res) << res.error().ToString();
    conn->Close();
    db.Close();
  }

  auto dirs2 = list_checkpoint_dirs(db_path);
  ASSERT_EQ(dirs2.size(), 1u);
  std::string ckp2_snapshot = dirs2[0].string() + "/snapshot";

  size_t ckp2_count = count_regular_files(ckp2_snapshot);
  EXPECT_EQ(ckp1_count, ckp2_count);

  // Data round-trip correctness.
  {
    neug::NeugDB db;
    assert_checkpoint_opt_graph(db, db_path);
  }
}

TEST(CheckpointOptTest,
     test_repeated_ap_checkpoint_keeps_single_current_checkpoint) {
  std::string db_path = "/tmp/test_repeated_ap_checkpoint_db";
  if (std::filesystem::exists(db_path)) {
    std::filesystem::remove_all(db_path);
  }

  {
    neug::NeugDB db;
    open_db_for_checkpoint_test(db, db_path);
    auto conn = db.Connect();
    seed_checkpoint_opt_graph(conn);
    auto res = conn->Query("CHECKPOINT;");
    ASSERT_TRUE(res) << res.error().ToString();
    conn->Close();
    db.Close();
  }
  AssertSingleCurrentCheckpoint(db_path);

  {
    neug::NeugDB db;
    open_db_for_checkpoint_test(db, db_path);
    auto conn = db.Connect();
    auto res = conn->Query("CHECKPOINT;");
    ASSERT_TRUE(res) << res.error().ToString();
    conn->Close();
    db.Close();
  }

  AssertSingleCurrentCheckpoint(db_path);
  auto dirs = list_checkpoint_dirs(db_path);
  ASSERT_EQ(dirs.size(), 1u);
  EXPECT_TRUE(std::filesystem::exists(dirs[0] / "snapshot"));

  // Data correctness: open the current checkpoint and query.
  {
    neug::NeugDB db;
    assert_checkpoint_opt_graph(db, db_path);
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
  auto ckp_id = mgr.CreateStagingCheckpoint();
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

// Fix #529: DiscardStagingCheckpoint cleans up directory and map entry.
TYPED_TEST(CheckpointSafetyTest,
           discard_staging_checkpoint_cleans_up_directory) {
  neug::CheckpointManager mgr;
  mgr.Open(this->db_dir_);
  auto ckp_id = mgr.CreateStagingCheckpoint();
  auto ckp = mgr.GetCheckpoint(ckp_id);
  auto ckp_path = ckp->path();

  ASSERT_TRUE(std::filesystem::exists(ckp_path));
  ASSERT_FALSE(mgr.HasCurrentCheckpoint());
  ASSERT_EQ(mgr.CurrentCheckpointId(), neug::kInvalidCheckpointId);

  mgr.DiscardStagingCheckpoint(ckp_id);

  EXPECT_FALSE(std::filesystem::exists(ckp_path));
  EXPECT_FALSE(mgr.HasCurrentCheckpoint());
  EXPECT_EQ(mgr.CurrentCheckpointId(), neug::kInvalidCheckpointId);
}

// Fix #528 integration: A failed CHECKPOINT does not corrupt on-disk data —
// recovery on restart succeeds.
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

  // Phase 2: Reopen and trigger a failing CHECKPOINT.
  // We use checkpoint_on_close=false so Close() doesn't attempt another
  // checkpoint while permissions are being restored.
  {
    neug::NeugDB db;
    db.Open(this->MakeConfigNoCheckpointOnClose(this->db_dir_));
    auto conn = db.Connect();

    // Checkpoints are staged under the DB root before being published. Removing
    // write permission from the root makes staging creation fail while keeping
    // the already-published checkpoint readable.
    std::filesystem::permissions(this->db_dir_,
                                 std::filesystem::perms::owner_read |
                                     std::filesystem::perms::owner_exec);

    // CHECKPOINT should fail before a new staging checkpoint can be published.
    auto res = conn->Query("CHECKPOINT;");
    EXPECT_FALSE(res) << "Expected CHECKPOINT to fail, but it succeeded";

    // Restore permissions so Close() and subsequent reopen work.
    std::filesystem::permissions(this->db_dir_,
                                 std::filesystem::perms::owner_all);
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
