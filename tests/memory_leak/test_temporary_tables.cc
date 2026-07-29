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
 *
 * NeuG counterpart of DuckDB's tests/memory_leak/test_temporary_tables.cpp.
 *
 * The DuckDB original contains three [memoryleak]-tagged Catch2 cases that
 * exercise three distinct allocation paths:
 *
 *   1. "Test in-memory database scanning from tables" — bulk-load 1M rows,
 *      then loop `SELECT *` forever to stress the scan/result allocator.
 *   2. "Rollback create table" — repeatedly BEGIN / CREATE TABLE / ROLLBACK
 *      to stress the catalog DDL undo path.
 *   3. "DB temporary table insertion" — persistent 1M-row source table +
 *      a loop of BEGIN / CREATE OR REPLACE TEMPORARY TABLE / INSERT FROM
 *      source / ROLLBACK to stress temp-table + insert + rollback.
 *
 * NeuG has no client-side BEGIN/ROLLBACK statements and no temporary table
 * concept, so we keep the *shape* of each scenario and substitute the
 * closest available primitive:
 *
 *   - "ROLLBACK" → `DROP TABLE` (catalog deletion).  The two are not
 *     semantically identical (rollback unwinds an in-flight tx, drop
 *     commits a deletion), but both stress the catalog alloc/free path
 *     that is the actual subject of the leak test.
 *   - "TEMPORARY TABLE" → ordinary `NODE TABLE` that is dropped at the
 *     end of every iteration.
 *   - "SELECT *"        → `MATCH (n:t1) RETURN n.id, n.s`.
 *
 * Like the DuckDB original these tests do not assert anything about
 * memory usage themselves — the verdict is delivered out-of-band by
 * valgrind / massif / heaptrack / RSS sampling.  Set
 *
 *     NEUG_RUN_MEMORY_LEAK_TESTS=1
 *
 * to actually run.  Iteration counts are tunable via:
 *
 *     NEUG_LEAK_OUTER_ITERS         (default 1000)
 *     NEUG_LEAK_INNER_ROWS          (default 1000)
 *     NEUG_LEAK_BULK_ROWS           (default 100000) -- big-table size
 *     NEUG_LEAK_SCAN_ITERS          (default 200)    -- scan loop count
 */

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <random>
#include <string>

#include "neug/main/connection.h"
#include "neug/main/neug_db.h"

namespace neug {
namespace test {
namespace {

bool RunMemoryLeakTests() {
  const char* env = std::getenv("NEUG_RUN_MEMORY_LEAK_TESTS");
  return env != nullptr && std::string(env) == "1";
}

int EnvInt(const char* name, int fallback) {
  const char* env = std::getenv(name);
  if (env == nullptr || env[0] == '\0') {
    return fallback;
  }
  try {
    return std::stoi(env);
  } catch (...) { return fallback; }
}

void RandStr(std::mt19937& rng, char* dest, std::size_t length) {
  static const char charset[] =
      "0123456789"
      "abcdefghijklmnopqrstuvwxyz"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  std::uniform_int_distribution<int> dist(0, sizeof(charset) - 2);
  for (std::size_t i = 0; i < length; ++i) {
    dest[i] = charset[dist(rng)];
  }
  dest[length] = '\0';
}

NeugDBConfig MakeConfig(const std::string& dir) {
  NeugDBConfig config;
  config.data_dir = dir;
  config.mode = DBMode::READ_WRITE;
  config.checkpoint_on_close = true;
  return config;
}

std::filesystem::path FreshDbDir(const char* tag) {
  auto p = std::filesystem::temp_directory_path() /
           (std::string("neug_temp_table_leak_") + tag);
  if (std::filesystem::exists(p)) {
    std::filesystem::remove_all(p);
  }
  std::filesystem::create_directories(p);
  return p;
}

void OpenAndConnect(std::unique_ptr<NeugDB>& db,
                    std::shared_ptr<Connection>& conn, const std::string& dir) {
  db = std::make_unique<NeugDB>();
  ASSERT_TRUE(db->Open(MakeConfig(dir)))
      << "Failed to open NeuG database at " << dir;
  conn = db->Connect();
  ASSERT_NE(conn, nullptr);
}

void CloseAndRelease(std::unique_ptr<NeugDB>& db,
                     std::shared_ptr<Connection>& conn) {
  if (conn) {
    conn->Close();
    conn.reset();
  }
  if (db) {
    db->Close();
    db.reset();
  }
}

void DrainResult(QueryResult& qr) {
  std::size_t rows = 0;
  while (qr.hasNext()) {
    qr.next();
    ++rows;
  }
  (void) rows;
}

}  // namespace

TEST(MemoryLeakTempTableTest, RepeatedScanLargeTable) {
  if (!RunMemoryLeakTests()) {
    GTEST_SKIP() << "memory-leak tests are skipped by default; "
                    "set NEUG_RUN_MEMORY_LEAK_TESTS=1 to enable.";
  }
  const int bulk_rows = EnvInt("NEUG_LEAK_BULK_ROWS", 100000);
  const int scan_iters = EnvInt("NEUG_LEAK_SCAN_ITERS", 200);
  ASSERT_GT(bulk_rows, 0);
  ASSERT_GT(scan_iters, 0);

  const auto db_path = FreshDbDir("scan");
  std::unique_ptr<NeugDB> db;
  std::shared_ptr<Connection> conn;
  OpenAndConnect(db, conn, db_path.string());

  ASSERT_TRUE(conn->Query(
      "CREATE NODE TABLE t1 (id INT64, s STRING, PRIMARY KEY(id));", "schema"))
      << "create t1 failed";

  std::mt19937 rng(0xBADCAFEu);
  for (int i = 0; i < bulk_rows; ++i) {
    char str[33];
    RandStr(rng, str, sizeof(str) - 1);
    std::string q;
    q.reserve(96);
    q.append("CREATE (:t1 {id: ");
    q.append(std::to_string(i));
    q.append(", s: 'thisisalongstring");
    q.append(str);
    q.append("'});");
    auto res = conn->Query(q, "insert");
    ASSERT_TRUE(res) << "bulk insert failed at i=" << i << ": "
                     << res.error().ToString();
  }

  const auto t0 = std::chrono::steady_clock::now();
  for (int i = 0; i < scan_iters; ++i) {
    auto res = conn->Query("MATCH (n:t1) RETURN n.id, n.s", "read");
    ASSERT_TRUE(res) << "scan failed at i=" << i << ": "
                     << res.error().ToString();
    DrainResult(res.value());
  }
  const auto t1 = std::chrono::steady_clock::now();

  CloseAndRelease(db, conn);
  std::error_code ec;
  std::filesystem::remove_all(db_path, ec);

  const auto secs =
      std::chrono::duration_cast<std::chrono::seconds>(t1 - t0).count();
  std::printf("[memleak/scan] bulk_rows=%d scan_iters=%d elapsed=%llds\n",
              bulk_rows, scan_iters, static_cast<long long>(secs));
  EXPECT_TRUE(true);
}

TEST(MemoryLeakTempTableTest, RepeatedCreateDropTable) {
  if (!RunMemoryLeakTests()) {
    GTEST_SKIP() << "memory-leak tests are skipped by default; "
                    "set NEUG_RUN_MEMORY_LEAK_TESTS=1 to enable.";
  }
  const int outer_iters = EnvInt("NEUG_LEAK_OUTER_ITERS", 1000);
  ASSERT_GT(outer_iters, 0);

  const auto db_path = FreshDbDir("ddl");
  std::unique_ptr<NeugDB> db;
  std::shared_ptr<Connection> conn;
  OpenAndConnect(db, conn, db_path.string());

  const auto t0 = std::chrono::steady_clock::now();
  for (int i = 0; i < outer_iters; ++i) {
    std::string tname = "t_" + std::to_string(i);
    {
      std::string q = "CREATE NODE TABLE " + tname +
                      " (id INT64, name STRING, PRIMARY KEY(id));";
      auto res = conn->Query(q, "schema");
      ASSERT_TRUE(res) << "CREATE failed at i=" << i << ": "
                       << res.error().ToString();
    }
    {
      std::string q = "DROP TABLE " + tname + ";";
      auto res = conn->Query(q, "schema");
      ASSERT_TRUE(res) << "DROP failed at i=" << i << ": "
                       << res.error().ToString();
    }
  }
  const auto t1 = std::chrono::steady_clock::now();

  CloseAndRelease(db, conn);
  std::error_code ec;
  std::filesystem::remove_all(db_path, ec);

  const auto secs =
      std::chrono::duration_cast<std::chrono::seconds>(t1 - t0).count();
  std::printf("[memleak/ddl] outer_iters=%d elapsed=%llds\n", outer_iters,
              static_cast<long long>(secs));
  EXPECT_TRUE(true);
}

TEST(MemoryLeakTempTableTest, RepeatedTempTableInsertion) {
  if (!RunMemoryLeakTests()) {
    GTEST_SKIP() << "memory-leak tests are skipped by default; "
                    "set NEUG_RUN_MEMORY_LEAK_TESTS=1 to enable.";
  }
  // Use a smaller default for the source table since each outer iteration
  // copies it in full.  Override via env var if you want to scale up.
  const int outer_iters = EnvInt("NEUG_LEAK_OUTER_ITERS", 200);
  const int inner_rows = EnvInt("NEUG_LEAK_INNER_ROWS", 1000);
  ASSERT_GT(outer_iters, 0);
  ASSERT_GT(inner_rows, 0);

  const auto db_path = FreshDbDir("temp_insert");

  std::unique_ptr<NeugDB> db;
  std::shared_ptr<Connection> conn;
  OpenAndConnect(db, conn, db_path.string());

  // Persistent source table — populated once, never dropped.
  ASSERT_TRUE(conn->Query(
      "CREATE NODE TABLE t_src (id INT64, PRIMARY KEY(id));", "schema"))
      << "create t_src failed";
  for (int i = 0; i < inner_rows; ++i) {
    std::string q = "CREATE (:t_src {id: " + std::to_string(i) + "});";
    auto res = conn->Query(q, "insert");
    ASSERT_TRUE(res) << "seed failed at i=" << i << ": "
                     << res.error().ToString();
  }

  const auto t0 = std::chrono::steady_clock::now();
  for (int i = 0; i < outer_iters; ++i) {
    {
      auto res = conn->Query(
          "CREATE NODE TABLE t_sink (id INT64, PRIMARY KEY(id));", "schema");
      ASSERT_TRUE(res) << "CREATE t_sink failed at i=" << i << ": "
                       << res.error().ToString();
    }
    {
      auto res =
          conn->Query("MATCH (s:t_src) CREATE (:t_sink {id: s.id});", "insert");
      ASSERT_TRUE(res) << "copy-into failed at i=" << i << ": "
                       << res.error().ToString();
    }
    {
      auto res = conn->Query("DROP TABLE t_sink;", "schema");
      ASSERT_TRUE(res) << "DROP t_sink failed at i=" << i << ": "
                       << res.error().ToString();
    }
  }
  const auto t1 = std::chrono::steady_clock::now();

  CloseAndRelease(db, conn);
  std::error_code ec;
  std::filesystem::remove_all(db_path, ec);

  const auto secs =
      std::chrono::duration_cast<std::chrono::seconds>(t1 - t0).count();
  std::printf("[memleak/temp_insert] outer=%d src_rows=%d elapsed=%llds\n",
              outer_iters, inner_rows, static_cast<long long>(secs));
  EXPECT_TRUE(true);
}

}  // namespace test
}  // namespace neug
