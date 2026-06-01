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

// Tests for the read-only M_LAZY (kSyncToFile) fast path that skips the
// snapshot-to-tmp copy by mmapping snapshot files directly with PROT_READ +
// MAP_SHARED. See NeugDB::Open for the gating logic.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <unordered_map>
#include "column_assertions.h"
#include "neug/main/neug_db.h"
#include "neug/storages/file_names.h"

namespace {

// Helper: populate a fresh DB at `db_dir` and checkpoint on close so the
// snapshot is on disk.
void PopulateAndCheckpoint(const std::string& db_dir) {
  neug::NeugDBConfig config(db_dir);
  config.mode = neug::DBMode::READ_WRITE;
  config.memory_level = neug::MemoryLevel::kSyncToFile;
  config.checkpoint_on_close = true;
  neug::NeugDB db;
  ASSERT_TRUE(db.Open(config));
  auto conn = db.Connect();
  ASSERT_TRUE(conn->Query(
      "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));"));
  ASSERT_TRUE(conn->Query("CREATE (n:person {id: 1, name: 'Alice'});"));
  ASSERT_TRUE(conn->Query("CREATE (n:person {id: 2, name: 'Bob'});"));
  ASSERT_TRUE(conn->Query(
      "CREATE REL TABLE knows(FROM person TO person, since INT64);"));
  ASSERT_TRUE(
      conn->Query("MATCH (a:person), (b:person) WHERE a.id=1 AND b.id=2 CREATE "
                  "(a)-[:knows {since: 2021}]->(b);"));
  db.Close();
}

// Snapshot (path → mtime tick count + size) so we can detect any mutation
// through the RO mmap. Storing the tick count instead of file_time_type
// keeps EXPECT_EQ printable on platforms where libc++ lacks std::to_chars
// for long double.
struct FileFingerprint {
  int64_t mtime_ticks;
  std::uintmax_t size;
};

std::unordered_map<std::string, FileFingerprint> SnapshotCheckpointDir(
    const std::string& db_dir) {
  std::unordered_map<std::string, FileFingerprint> out;
  auto cp = neug::checkpoint_dir(db_dir);
  if (!std::filesystem::exists(cp)) {
    return out;
  }
  for (const auto& entry : std::filesystem::recursive_directory_iterator(cp)) {
    if (!entry.is_regular_file()) {
      continue;
    }
    out[entry.path().string()] = {
        static_cast<int64_t>(
            entry.last_write_time().time_since_epoch().count()),
        entry.file_size()};
  }
  return out;
}

class ReadOnlyLazyFastPathTest : public ::testing::Test {
 protected:
  std::string db_dir;
  void SetUp() override {
    db_dir = "/tmp/test_ro_lazy_fastpath_" +
             std::to_string(::testing::UnitTest::GetInstance()
                                ->current_test_info()
                                ->result()
                                ->start_timestamp());
    if (std::filesystem::exists(db_dir)) {
      std::filesystem::remove_all(db_dir);
    }
    std::filesystem::create_directories(db_dir);
  }
  void TearDown() override {
    if (std::filesystem::exists(db_dir)) {
      std::filesystem::remove_all(db_dir);
    }
  }
};

}  // namespace

// When wal is empty, RO+M_LAZY should take the fast path: snapshot files are
// not modified, runtime/tmp is not populated.
TEST_F(ReadOnlyLazyFastPathTest, FastPathLeavesSnapshotAndTmpAlone) {
  PopulateAndCheckpoint(db_dir);

  auto fingerprint_before = SnapshotCheckpointDir(db_dir);
  ASSERT_FALSE(fingerprint_before.empty())
      << "checkpoint dir should contain snapshot files after Populate";

  // wal dir should be empty after a clean checkpoint -> wal_dir_is_empty true.
  ASSERT_TRUE(neug::wal_dir_is_empty(db_dir));

  // Open read-only with M_LAZY.
  neug::NeugDBConfig config(db_dir);
  config.mode = neug::DBMode::READ_ONLY;
  config.memory_level = neug::MemoryLevel::kSyncToFile;
  neug::NeugDB db;
  ASSERT_TRUE(db.Open(config));

  // Fast path should NOT have created runtime/tmp.
  EXPECT_FALSE(std::filesystem::exists(neug::tmp_dir(db_dir)))
      << "RO+M_LAZY fast path must not create runtime/tmp";

  // Queries return the populated data.
  auto conn = db.Connect();
  auto res = conn->Query("MATCH (v:person) RETURN v.id, v.name ORDER BY v.id;");
  ASSERT_TRUE(res);
  const auto& table = res.value().response();
  ASSERT_EQ(table.row_count(), 2);
  neug::test::AssertInt64Column(table, 0, {1, 2});
  neug::test::AssertStringColumn(table, 1, {"Alice", "Bob"});

  auto res2 = conn->Query(
      "MATCH (a:person)-[r:knows]->(b:person) RETURN a.id, b.id, r.since;");
  ASSERT_TRUE(res2);
  const auto& table2 = res2.value().response();
  ASSERT_EQ(table2.row_count(), 1);
  neug::test::AssertInt64Column(table2, 0, {1});
  neug::test::AssertInt64Column(table2, 1, {2});
  neug::test::AssertInt64Column(table2, 2, {2021});

  db.Close();

  // Snapshot files must be byte-identical after a read-only open.
  auto fingerprint_after = SnapshotCheckpointDir(db_dir);
  EXPECT_EQ(fingerprint_before.size(), fingerprint_after.size());
  for (const auto& [path, fp] : fingerprint_before) {
    auto it = fingerprint_after.find(path);
    ASSERT_NE(it, fingerprint_after.end())
        << "snapshot file disappeared: " << path;
    EXPECT_EQ(fp.size, it->second.size) << path << " size changed";
    EXPECT_EQ(fp.mtime_ticks, it->second.mtime_ticks)
        << path << " mtime changed";
  }
}

// `wal_dir_is_empty` is the predicate that gates the fast path: returns true
// for a missing dir, an empty dir, or a dir with no `*.wal` files; returns
// false as soon as any `.wal` file is present. Exercising the actual fallback
// open with a planted .wal file would require a real (parseable) WAL record,
// which is out of scope for this test — the helper alone is what NeugDB::Open
// reads, so testing the helper covers the gating contract.
TEST_F(ReadOnlyLazyFastPathTest, WalDirIsEmptyPredicate) {
  // Case 1: wal dir does not exist.
  EXPECT_TRUE(neug::wal_dir_is_empty(db_dir));

  // Case 2: wal dir exists but is empty.
  auto wal_path = neug::wal_dir(db_dir);
  std::filesystem::create_directories(wal_path);
  EXPECT_TRUE(neug::wal_dir_is_empty(db_dir));

  // Case 3: wal dir has a non-.wal file -> still considered empty.
  {
    std::ofstream out(wal_path + "ignore.txt");
    out << "not a wal";
  }
  EXPECT_TRUE(neug::wal_dir_is_empty(db_dir));

  // Case 4: wal dir has a .wal file -> not empty.
  {
    std::ofstream out(wal_path + "leftover.wal", std::ios::binary);
    out.write("noise", 5);
  }
  EXPECT_FALSE(neug::wal_dir_is_empty(db_dir));
}

// Two concurrent RO+M_LAZY opens of the same snapshot dir must coexist —
// F_RDLCK allows shared locks across processes/sessions, and our mmap holds
// no exclusive resources.
TEST_F(ReadOnlyLazyFastPathTest, TwoConcurrentReadOnlyOpens) {
  PopulateAndCheckpoint(db_dir);

  neug::NeugDBConfig config(db_dir);
  config.mode = neug::DBMode::READ_ONLY;
  config.memory_level = neug::MemoryLevel::kSyncToFile;

  neug::NeugDB db1;
  neug::NeugDB db2;
  ASSERT_TRUE(db1.Open(config));
  ASSERT_TRUE(db2.Open(config));
  auto c1 = db1.Connect();
  auto c2 = db2.Connect();
  auto r1 = c1->Query("MATCH (v:person) RETURN v.id ORDER BY v.id;");
  auto r2 = c2->Query("MATCH (v:person) RETURN v.id ORDER BY v.id;");
  ASSERT_TRUE(r1);
  ASSERT_TRUE(r2);
  EXPECT_EQ(r1.value().response().row_count(), 2);
  EXPECT_EQ(r2.value().response().row_count(), 2);
  db1.Close();
  db2.Close();
}
