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

#include "neug/neug.h"
#include "neug/server/neug_db_service.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/transaction/compact_transaction.h"
#include "neug/transaction/version_manager.h"

#include <atomic>
#include <chrono>
#include <thread>

#include "glog/logging.h"
#include "gtest/gtest.h"

class CompactTransactionTest : public ::testing::Test {
 protected:
  std::string db_dir;

  void SetUp() override {
    db_dir = "/tmp/test_compact_transaction_db";
    if (std::filesystem::exists(db_dir)) {
      std::filesystem::remove_all(db_dir);
    }
    std::filesystem::create_directories(db_dir);

    neug::NeugDB db;
    neug::NeugDBConfig config(db_dir);
    config.memory_level = neug::MemoryLevel::kInMemory;
    config.checkpoint_on_close = true;
    db.Open(db_dir);
    auto conn = db.Connect();
    EXPECT_TRUE(
        conn->Query("CREATE NODE TABLE person(id INT64, name STRING, "
                    "age INT64, PRIMARY KEY(id));"));
    EXPECT_TRUE(
        conn->Query("CREATE NODE TABLE software(id INT64, name STRING, "
                    "lang STRING, PRIMARY KEY(id));"));
    EXPECT_TRUE(conn->Query(
        "CREATE REL TABLE created(FROM person TO software, weight DOUBLE, "
        "since INT64);"));
    EXPECT_TRUE(
        conn->Query("CREATE REL TABLE knows(FROM person TO person, "
                    "closeness DOUBLE);"));
    EXPECT_TRUE(
        conn->Query("Create ( n:person {id: 1, name: 'Alice', age: 30});"));
    EXPECT_TRUE(conn->Query(
        "Create ( n:software {id: 1, name: 'GraphDB', lang: 'C++'});"));
    EXPECT_TRUE(
        conn->Query("Create ( n:person {id: 2, name: 'Bob', age: 25});"));
    EXPECT_TRUE(conn->Query(
        "Create ( n:software {id: 2, name: 'FastGraph', lang: 'Rust'});"));
    EXPECT_TRUE(
        conn->Query("MATCH (a:person {id: 1}), (b:software {id: 1}) "
                    "CREATE (a)-[:created {weight: 0.8, since: 2021}]->(b);"));
    EXPECT_TRUE(
        conn->Query("MATCH (a:person {id: 2}), (b:software {id: 2}) "
                    "CREATE (a)-[:created {weight: 0.7, since: 2020}]->(b);"));
    EXPECT_TRUE(
        conn->Query("MATCH (a:person {id: 1}), (b:person {id: 2}) "
                    "CREATE (a)-[:knows {closeness: 0.9}]->(b);"));
    db.Close();
  }

  void TearDown() override {
    if (std::filesystem::exists(db_dir)) {
      std::filesystem::remove_all(db_dir);
    }
  }

  size_t count_vertices(const neug::StorageReadInterface& gi,
                        neug::label_t label) {
    size_t vertex_count = 0;
    auto v_set = gi.GetVertexSet(label);
    v_set.foreach_vertex([&](neug::vid_t vid) { vertex_count++; });
    return vertex_count;
  }

  size_t count_edges(const neug::StorageReadInterface& gi,
                     neug::label_t src_label, neug::label_t neighbor_label,
                     neug::label_t edge_label, bool outgoing) {
    size_t edge_count = 0;
    auto view = outgoing ? gi.GetGenericOutgoingGraphView(
                               src_label, neighbor_label, edge_label)
                         : gi.GetGenericIncomingGraphView(
                               src_label, neighbor_label, edge_label);
    auto v_set = gi.GetVertexSet(src_label);
    v_set.foreach_vertex([&](neug::vid_t vid) {
      auto edge_iter = view.get_edges(vid);
      for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
        edge_count++;
      }
    });
    return edge_count;
  }
};

// Commit, Abort, and destructor (auto-abort) should all preserve data.
TEST_F(CompactTransactionTest, CommitAbortAndDestructorPreserveData) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  // 1) Compact + Commit
  {
    auto sess = svc->AcquireSession();
    auto compact_txn = sess->GetCompactTransaction();
    EXPECT_TRUE(compact_txn.Commit());
  }
  // 2) Compact + Abort
  {
    auto sess = svc->AcquireSession();
    auto compact_txn = sess->GetCompactTransaction();
    compact_txn.Abort();
  }
  // 3) Destructor auto-abort (no Commit/Abort call)
  {
    auto sess = svc->AcquireSession();
    auto compact_txn = sess->GetCompactTransaction();
  }

  // Verify all data intact after all three paths
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto created_label = gi.schema().get_edge_label_id("created");
    auto knows_label = gi.schema().get_edge_label_id("knows");

    EXPECT_EQ(count_vertices(gi, person_label), 2);
    EXPECT_EQ(count_vertices(gi, software_label), 2);
    EXPECT_EQ(
        count_edges(gi, person_label, software_label, created_label, true), 2);
    EXPECT_EQ(count_edges(gi, person_label, person_label, knows_label, true),
              1);
    EXPECT_TRUE(txn.Commit());
  }
  db.Close();
}

// Delete vertices, compact, verify deletions are permanent.
TEST_F(CompactTransactionTest, DeleteThenCompactPurgesData) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  // Delete person id=2 via Cypher
  {
    auto conn = db.Connect();
    EXPECT_TRUE(conn->Query("MATCH (v:person) WHERE v.id = 2 DELETE v;"));
    conn->Close();
  }

  // Verify deletion visible before compact
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    EXPECT_EQ(count_vertices(gi, person_label), 1);
    EXPECT_TRUE(txn.Commit());
  }

  // Compact
  {
    auto sess = svc->AcquireSession();
    auto compact_txn = sess->GetCompactTransaction();
    EXPECT_TRUE(compact_txn.Commit());
  }

  // Verify data after compact — deletion should be permanent
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto created_label = gi.schema().get_edge_label_id("created");
    auto knows_label = gi.schema().get_edge_label_id("knows");

    EXPECT_EQ(count_vertices(gi, person_label), 1);
    EXPECT_EQ(count_vertices(gi, software_label), 2);
    // Only person 1's edges remain
    EXPECT_EQ(
        count_edges(gi, person_label, software_label, created_label, true), 1);
    // knows edge: person1 → person2, but person2 is deleted
    EXPECT_EQ(count_edges(gi, person_label, person_label, knows_label, true),
              0);
    EXPECT_TRUE(txn.Commit());
  }
  db.Close();
}

// Compact, checkpoint, reopen — verify data persists across restart.
TEST_F(CompactTransactionTest, CompactAndReopenPersistsData) {
  {
    neug::NeugDB db;
    neug::NeugDBConfig config(db_dir);
    config.memory_level = neug::MemoryLevel::kInMemory;
    config.checkpoint_on_close = true;
    db.Open(config);
    auto svc = std::make_shared<neug::NeugDBService>(db);

    // Delete person id=1 via Cypher
    {
      auto conn = db.Connect();
      EXPECT_TRUE(conn->Query("MATCH (v:person) WHERE v.id = 1 DELETE v;"));
      conn->Close();
    }

    // Compact explicitly before close
    {
      auto sess = svc->AcquireSession();
      auto compact_txn = sess->GetCompactTransaction();
      EXPECT_TRUE(compact_txn.Commit());
    }

    db.Close();
  }

  // Reopen and verify
  {
    neug::NeugDB db2;
    neug::NeugDBConfig config2(db_dir);
    config2.memory_level = neug::MemoryLevel::kInMemory;
    db2.Open(config2);
    auto svc2 = std::make_shared<neug::NeugDBService>(db2);

    auto sess = svc2->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto created_label = gi.schema().get_edge_label_id("created");

    EXPECT_EQ(count_vertices(gi, person_label), 1);
    EXPECT_EQ(count_vertices(gi, software_label), 2);
    // Only person 2's created edge remains
    EXPECT_EQ(
        count_edges(gi, person_label, software_label, created_label, true), 1);
    EXPECT_TRUE(txn.Commit());
    db2.Close();
  }
}

// Repeated Commit and Abort-after-Commit should be safe no-ops.
TEST_F(CompactTransactionTest, IdempotentCommitAndAbort) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto compact_txn = sess->GetCompactTransaction();
    EXPECT_TRUE(compact_txn.Commit());
    EXPECT_TRUE(compact_txn.Commit());  // double commit — no-op
    compact_txn.Abort();                // abort after commit — no-op
  }

  // Verify data intact
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    EXPECT_EQ(count_vertices(gi, person_label), 2);
    EXPECT_TRUE(txn.Commit());
  }
  db.Close();
}

// ---------------------------------------------------------------------------
// Concurrency exclusion tests: CompactTransaction blocks all other
// transaction types (Read, Insert, Update, and another Compact).
//
// Tests at TPVersionManager level to avoid SessionPool size constraints.
// Strategy: main thread acquires compact timestamp (holds exclusive lock),
// worker thread tries to acquire another timestamp type — should be blocked
// until main thread releases compact.
// ---------------------------------------------------------------------------

// Helper: verify that `acquire_fn` is blocked while compact timestamp is
// held, and proceeds once released.
static void AssertCompactBlocksAcquire(
    neug::TPVersionManager& vm,
    std::function<void(neug::TPVersionManager&)> acquire_fn,
    std::function<void(neug::TPVersionManager&, uint32_t)> release_fn) {
  std::atomic<bool> worker_started{false};
  std::atomic<bool> worker_acquired{false};

  // Main thread: acquire compact timestamp (exclusive lock)
  uint32_t compact_ts = vm.acquire_update_timestamp();

  // Worker thread: try to acquire another timestamp
  std::thread worker([&]() {
    worker_started.store(true);
    acquire_fn(vm);
    worker_acquired.store(true);
    release_fn(vm, 0);  // release immediately; ts value unused for read
  });

  // Wait for worker to start
  while (!worker_started.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  // Give the worker time — if not blocked it would acquire in microseconds
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_FALSE(worker_acquired.load())
      << "Worker should be blocked while compact timestamp is held";

  // Release compact timestamp — worker should proceed
  vm.release_update_timestamp(compact_ts);

  worker.join();
  EXPECT_TRUE(worker_acquired.load())
      << "Worker should have acquired after compact released";
}

TEST_F(CompactTransactionTest, CompactBlocksRead) {
  neug::TPVersionManager vm;
  vm.init_ts(0, 1);

  AssertCompactBlocksAcquire(
      vm, [](neug::TPVersionManager& v) { v.acquire_read_timestamp(); },
      [](neug::TPVersionManager& v, uint32_t) { v.release_read_timestamp(); });
}

TEST_F(CompactTransactionTest, CompactBlocksInsert) {
  neug::TPVersionManager vm;
  vm.init_ts(0, 1);
  AssertCompactBlocksAcquire(
      vm, [](neug::TPVersionManager& v) { v.acquire_insert_timestamp(); },
      [](neug::TPVersionManager& v, uint32_t ts) {
        v.release_insert_timestamp(ts);
      });
}

TEST_F(CompactTransactionTest, CompactBlocksUpdate) {
  neug::TPVersionManager vm;
  vm.init_ts(0, 1);
  AssertCompactBlocksAcquire(
      vm, [](neug::TPVersionManager& v) { v.acquire_update_timestamp(); },
      [](neug::TPVersionManager& v, uint32_t ts) {
        v.release_update_timestamp(ts);
      });
}

TEST_F(CompactTransactionTest, CompactBlocksCompact) {
  neug::TPVersionManager vm;
  vm.init_ts(0, 1);
  AssertCompactBlocksAcquire(
      vm, [](neug::TPVersionManager& v) { v.acquire_update_timestamp(); },
      [](neug::TPVersionManager& v, uint32_t ts) {
        v.release_update_timestamp(ts);
      });
}
