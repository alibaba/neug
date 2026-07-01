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
 * NeuG counterpart of DuckDB's tests/memory_leak/test_appender.cpp.
 *
 * Mirrors the same structure: a single long-running test case that hammers
 * the write path with many small batches and triggers a checkpoint every N
 * outer iterations.  DuckDB exercises its `duckdb_appender_*` C-API; NeuG
 * has no Appender equivalent, so we use the closest thing — repeated Cypher
 * `CREATE (:test {...})` statements through Connection::Query.
 *
 * The test does **not** verify memory consumption itself.  Like the DuckDB
 * original it is meant to be observed externally via valgrind / massif /
 * heaptrack / RSS sampling (see tests/memory_leak/test_memory_leak.py and
 * bin/rss_workload.cc).
 *
 * Default-skipped: set environment variable
 *
 *     NEUG_RUN_MEMORY_LEAK_TESTS=1
 *
 * to actually run.  Iteration counts can be lowered for smoke runs via
 *
 *     NEUG_LEAK_OUTER_ITERS=<int>      (default 1000)
 *     NEUG_LEAK_INNER_ROWS=<int>       (default 1000)
 *     NEUG_LEAK_CHECKPOINT_EVERY=<int> (default 500, 0 to disable)
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
  } catch (...) {
    return fallback;
  }
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
  config.enable_auto_compaction = false;
  config.compact_on_close = false;
  config.compact_csr = true;
  config.checkpoint_on_close = true;
  return config;
}

void OpenAndConnect(std::unique_ptr<NeugDB>& db,
                    std::shared_ptr<Connection>& conn,
                    const std::string& dir) {
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

}  // namespace

TEST(MemoryLeakTest, RepeatedCreateNodeChunks) {
  if (!RunMemoryLeakTests()) {
    GTEST_SKIP() << "memory-leak tests are skipped by default; "
                    "set NEUG_RUN_MEMORY_LEAK_TESTS=1 to enable.";
  }

  const int outer_iters = EnvInt("NEUG_LEAK_OUTER_ITERS", 1000);
  const int inner_rows = EnvInt("NEUG_LEAK_INNER_ROWS", 1000);
  const int checkpoint_every = EnvInt("NEUG_LEAK_CHECKPOINT_EVERY", 500);
  ASSERT_GT(outer_iters, 0);
  ASSERT_GT(inner_rows, 0);

  const std::filesystem::path db_path =
      std::filesystem::temp_directory_path() / "neug_create_node_leak_test";
  if (std::filesystem::exists(db_path)) {
    std::filesystem::remove_all(db_path);
  }
  std::filesystem::create_directories(db_path);

  std::unique_ptr<NeugDB> db;
  std::shared_ptr<Connection> conn;
  OpenAndConnect(db, conn, db_path.string());

  {
    auto res = conn->Query(
        "CREATE NODE TABLE test (id INT64, col1 STRING, col2 STRING, "
        "col3 INT64, col4 INT64, col5 DOUBLE, PRIMARY KEY(id));",
        "schema");
    ASSERT_TRUE(res) << "Failed to create test table: "
                     << (res ? "" : res.error().ToString());
  }

  std::mt19937 rng(0xC0FFEEu);
  int64_t n1 = 0;
  double d1 = 0.5;
  int64_t pk = 0;

  const auto t0 = std::chrono::steady_clock::now();

  for (int i = 0; i < outer_iters; ++i) {
    for (int j = 0; j < inner_rows; ++j) {
      char str[41];
      RandStr(rng, str, sizeof(str) - 1);

      std::string q;
      q.reserve(192);
      q.append("CREATE (:test {id: ");
      q.append(std::to_string(pk++));
      q.append(", col1: '");
      q.append(str);
      q.append("', col2: 'hello', col3: ");
      q.append(std::to_string(n1++));
      q.append(", col4: ");
      q.append(std::to_string(n1++));
      q.append(", col5: ");
      q.append(std::to_string(d1));
      q.append("});");
      d1 += 1.25;

      auto res = conn->Query(q, "insert");
      ASSERT_TRUE(res) << "CREATE failed at i=" << i << ", j=" << j << ": "
                       << res.error().ToString();
    }

    if (checkpoint_every > 0 && i % checkpoint_every == 0) {
      std::printf("completed %d\n", i);
      CloseAndRelease(db, conn);
      OpenAndConnect(db, conn, db_path.string());
    }
  }

  CloseAndRelease(db, conn);

  const auto t1 = std::chrono::steady_clock::now();
  const auto secs =
      std::chrono::duration_cast<std::chrono::seconds>(t1 - t0).count();
  std::printf("[memory_leak] outer=%d inner=%d total_rows=%lld elapsed=%llds\n",
              outer_iters, inner_rows,
              static_cast<long long>(outer_iters) * inner_rows,
              static_cast<long long>(secs));

  // Cleanup the on-disk database so repeated runs start fresh.
  std::error_code ec;
  std::filesystem::remove_all(db_path, ec);

  EXPECT_TRUE(true);
}

}  // namespace test
}  // namespace neug
