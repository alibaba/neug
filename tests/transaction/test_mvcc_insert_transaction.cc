
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

#include "neug/common/types/value.h"
#include "neug/neug.h"
#include "neug/server/neug_db_service.h"
#include "neug/storages/csr/csr_view_utils.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/transaction/mvcc_insert_transaction.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/local_wal_parser.h"
#include "neug/transaction/wal/local_wal_writer.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/exception/exception.h"

#include <fcntl.h>
#include <sys/stat.h>
#ifndef _WIN32
#include <sys/resource.h>
#include <unistd.h>
#else
#include <io.h>
#include <process.h>
#define getpid _getpid
#endif
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace {

// MSVC's POSIX-compat I/O functions carry an underscore prefix. Object-like
// macros (#define open _open) would also rewrite LocalWalWriter::open() and
// LocalWalWriter::close() member calls, so route raw fd I/O through these
// platform wrappers instead.
#ifdef _WIN32
int OsOpen(const char* path, int flags, int mode) {
  return _open(path, flags, mode);
}

int OsClose(int fd) { return _close(fd); }

int OsWrite(int fd, const void* buf, size_t count) {
  return _write(fd, buf, static_cast<unsigned int>(count));
}
#else
int OsOpen(const char* path, int flags, int mode) {
  return ::open(path, flags, mode);
}

int OsClose(int fd) { return ::close(fd); }

ssize_t OsWrite(int fd, const void* buf, size_t count) {
  return ::write(fd, buf, count);
}
#endif

}  // namespace

class MvccInsertTransactionTest : public ::testing::Test {
 protected:
  std::string db_dir;
  neug::MemoryLevel memory_level;

  void SetUp() override {
    db_dir = "/tmp/test_mvcc_insert_transaction_db";
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
};

TEST_F(MvccInsertTransactionTest, MvccInsertTransactionBasic) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto slot = svc->AcquireExecutionSlot();
    auto txn = slot->BeginMvccInsertTransaction();
    EXPECT_EQ(txn.timestamp(), 1);
    EXPECT_TRUE(txn.schema().is_vertex_label_valid("person"));
  }
}

TEST_F(MvccInsertTransactionTest, AddVertex) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto slot = svc->AcquireExecutionSlot();
    auto txn = slot->BeginMvccInsertTransaction();
    neug::StorageTPInsertInterface interface(txn);
    auto person_label = interface.schema().get_vertex_label_id("person");
    neug::vid_t vid;
    EXPECT_TRUE(interface.AddVertex(
        person_label, neug::Value::INT64(3),
        {neug::Value::STRING(std::string("Eve")), neug::Value::INT64(28)},
        vid));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto slot = svc->AcquireExecutionSlot();
    auto txn = slot->BeginSnapshotReadTransaction();
    neug::StorageReadInterface gi(txn.view(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    EXPECT_EQ(count_vertices(gi, person_label), 3);
  }
  svc.reset();
  db.Close();
}

TEST_F(MvccInsertTransactionTest, AddEdge) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto slot = svc->AcquireExecutionSlot();
    auto txn = slot->BeginMvccInsertTransaction();
    neug::StorageTPInsertInterface interface(txn);
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    neug::vid_t vid;
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Value::INT64(1), vid));
    neug::vid_t vid2;
    EXPECT_TRUE(
        txn.GetVertexIndex(software_label, neug::Value::INT64(2), vid2));
    const void* edge_prop = nullptr;
    EXPECT_TRUE(interface.AddEdge(
        person_label, vid, software_label, vid2, created_label,
        {neug::Value::DOUBLE(0.9), neug::Value::INT64(2022)}, edge_prop));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto slot = svc->AcquireExecutionSlot();
    auto txn = slot->BeginSnapshotReadTransaction();
    neug::StorageReadInterface gi(txn.view(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto created_label = gi.schema().get_edge_label_id("created");
    auto view = gi.GetGenericOutgoingGraphView(person_label, software_label,
                                               created_label);

    size_t edge_count = 0;
    auto vertex_set = gi.GetVertexSet(person_label);
    for (neug::vid_t vid : vertex_set) {
      auto oid = gi.GetVertexId(person_label, vid);
      if (oid.GetValue<int64_t>() == 1) {
        auto edge_iter = view.get_edges(vid);
        for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
          edge_count++;
        }
      }
    }
    EXPECT_EQ(edge_count, 2);
  }
  svc.reset();
  db.Close();
}

TEST_F(MvccInsertTransactionTest, TestUnsupportedInterface) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto slot = svc->AcquireExecutionSlot();
    auto txn = slot->BeginMvccInsertTransaction();
    neug::StorageTPInsertInterface interface(txn);
    std::vector<neug::vid_t> vids;
    std::vector<std::tuple<neug::vid_t, neug::vid_t>> edges;
    std::vector<std::pair<neug::vid_t, int32_t>> oe_edges, ie_edges;
    EXPECT_EQ(interface.BatchAddVertices(0, nullptr).error().error_code(),
              neug::StatusCode::ERR_NOT_SUPPORTED);
    EXPECT_EQ(interface.BatchAddEdges(0, 0, 0, nullptr).error_code(),
              neug::StatusCode::ERR_NOT_SUPPORTED);
  }
}

namespace {
class InsertTestWalWriter : public neug::IWalWriter {
 public:
  std::function<bool()> on_append = [] { return true; };
  size_t calls = 0;
  std::string type() const override { return "insert-test"; }
  void open(const std::string&) override {}
  void close() override {}
  bool append(const char*, size_t) override {
    ++calls;
    return on_append();
  }
};

#if GTEST_HAS_DEATH_TEST
class CrashInsertWalWriter : public neug::IWalWriter {
 public:
  CrashInsertWalWriter(const std::string& dir, bool persist)
      : writer_(dir, 123), persist_(persist) {
    writer_.open(dir);
  }
  std::string type() const override { return "crash-insert-test"; }
  void open(const std::string&) override {}
  void close() override { writer_.close(); }
  bool append(const char* data, size_t size) override {
    if (persist_ && !writer_.append(data, size)) {
      std::_Exit(2);
    }
    // Neither destructors nor shutdown checkpointing may run.
    std::_Exit(0);
  }

 private:
  neug::LocalWalWriter writer_;
  bool persist_;
};
#endif
}  // namespace

#if GTEST_HAS_DEATH_TEST
TEST_F(MvccInsertTransactionTest, CrashBeforeWalDoesNotPersistAppliedVertex) {
  using namespace neug;
  for (auto level : {MemoryLevel::kInMemory, MemoryLevel::kSyncToFile}) {
    ASSERT_EXIT(
        {
          NeugDB db;
          NeugDBConfig config(db_dir);
          config.memory_level = level;
          config.checkpoint_on_close = false;
          db.Open(config);
          VersionManager vm;
          vm.init_ts({0, 0}, 4);
          Allocator alloc(MemoryLevel::kInMemory, "");
          CrashInsertWalWriter writer(db.graph().checkpoint().wal_dir(), false);
          MvccInsertTransaction txn(SnapshotGuard(db.graph_snapshot_store()),
                                    alloc, writer, vm,
                                    vm.acquire_insert_timestamp());
          vid_t lid;
          auto person = txn.schema().get_vertex_label_id("person");
          if (!txn.AddVertex(person, Value::INT64(3),
                             {Value::STRING("uncommitted"), Value::INT64(20)},
                             lid)) {
            std::_Exit(3);
          }
          txn.Commit();
          std::_Exit(4);
        },
        ::testing::ExitedWithCode(0), ".*");
    NeugDB reopened;
    NeugDBConfig config(db_dir);
    config.memory_level = level;
    config.checkpoint_on_close = false;
    reopened.Open(config);
    SnapshotGuard guard(reopened.graph_snapshot_store());
    const auto& view = guard.get().view();
    vid_t lid;
    EXPECT_FALSE(view.get_lid(view.schema().get_vertex_label_id("person"),
                              Value::INT64(3), lid, MAX_TIMESTAMP));
  }
}

TEST_F(MvccInsertTransactionTest,
       CrashAfterWalRecoversBeforeVisibilityRelease) {
  using namespace neug;
  ASSERT_EXIT(
      {
        NeugDB db;
        NeugDBConfig config(db_dir);
        config.memory_level = MemoryLevel::kSyncToFile;
        config.checkpoint_on_close = false;
        db.Open(config);
        VersionManager vm;
        vm.init_ts({0, 0}, 4);
        Allocator alloc(MemoryLevel::kInMemory, "");
        CrashInsertWalWriter writer(db.graph().checkpoint().wal_dir(), true);
        MvccInsertTransaction txn(SnapshotGuard(db.graph_snapshot_store()),
                                  alloc, writer, vm,
                                  vm.acquire_insert_timestamp());
        vid_t lid;
        auto person = txn.schema().get_vertex_label_id("person");
        if (!txn.AddVertex(person, Value::INT64(3),
                           {Value::STRING("committed"), Value::INT64(20)},
                           lid)) {
          std::_Exit(3);
        }
        txn.Commit();
        std::_Exit(4);
      },
      ::testing::ExitedWithCode(0), ".*");
  NeugDB reopened;
  reopened.Open(db_dir);
  SnapshotGuard guard(reopened.graph_snapshot_store());
  const auto& view = guard.get().view();
  vid_t lid;
  EXPECT_TRUE(view.get_lid(view.schema().get_vertex_label_id("person"),
                           Value::INT64(3), lid, MAX_TIMESTAMP));
}
#endif

TEST_F(MvccInsertTransactionTest, WalRejectionUndoesParallelEdgesThenVertices) {
  using namespace neug;
  NeugDB db;
  NeugDBConfig config(db_dir);
  config.memory_level = MemoryLevel::kInMemory;
  db.Open(config);
  VersionManager vm;
  vm.init_ts({0, 0}, 4);
  Allocator alloc(MemoryLevel::kInMemory, "");
  InsertTestWalWriter writer;
  SnapshotGuard observer(db.graph_snapshot_store());
  auto& view = observer.get().mutable_view();
  const auto person = view.schema().get_vertex_label_id("person");
  const auto knows = view.schema().get_edge_label_id("knows");
  const auto initial_edges = view.EdgeNum(person, person, knows);
  const auto ts = vm.acquire_insert_timestamp();
  MvccInsertTransaction txn(SnapshotGuard(db.graph_snapshot_store()), alloc,
                            writer, vm, ts);
  vid_t added, existing;
  ASSERT_TRUE(txn.AddVertex(person, Value::INT64(3),
                            {Value::STRING("new"), Value::INT64(20)}, added));
  ASSERT_TRUE(txn.GetVertexIndex(person, Value::INT64(1), existing));
  const void* prop = nullptr;
  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(txn.AddEdge(person, added, person, existing, knows,
                            {Value::DOUBLE(0.5)}, prop));
    ASSERT_TRUE(txn.AddEdge(person, existing, person, existing, knows,
                            {Value::DOUBLE(0.5)}, prop));
  }
  writer.on_append = [&] {
    vid_t physical;
    EXPECT_TRUE(view.get_lid(person, Value::INT64(3), physical, ts));
    EXPECT_FALSE(view.get_lid(person, Value::INT64(3), physical, 0));
    EXPECT_EQ(view.EdgeNum(person, person, knows), initial_edges + 6);
    EXPECT_EQ(vm.insert_read_timestamp(), 0u);
    return false;
  };
  EXPECT_FALSE(txn.Commit());
  EXPECT_EQ(writer.calls, 1u);
  EXPECT_EQ(view.VertexNum(person), 2u);
  EXPECT_EQ(view.EdgeNum(person, person, knows), initial_edges);
  auto incoming = view.GetGenericIncomingView(person, person, knows,
                                              vm.insert_read_timestamp());
  size_t incoming_count = 0;
  for (auto lid : view.GetVertexSet(person, vm.insert_read_timestamp())) {
    for (auto it = incoming.get_edges(lid).begin();
         it != incoming.get_edges(lid).end(); ++it) {
      ++incoming_count;
    }
  }
  EXPECT_EQ(incoming_count, initial_edges);
  txn.Abort();
  writer.on_append = [] { return true; };
  const auto next_ts = vm.acquire_insert_timestamp();
  MvccInsertTransaction next(SnapshotGuard(db.graph_snapshot_store()), alloc,
                             writer, vm, next_ts);
  ASSERT_TRUE(next.AddVertex(person, Value::INT64(3),
                             {Value::STRING("replacement"), Value::INT64(30)},
                             added));
  ASSERT_TRUE(next.Commit());
  // A stale/repeated undo must not remove the reused VID.
  view.UndoInsertedVertex(person, Value::INT64(3), ts);
  EXPECT_TRUE(view.get_lid(person, Value::INT64(3), added, next_ts));
  EXPECT_EQ(view.VertexNum(person), 3u);
  observer.release();
  db.Close();  // CSR allocation buffers must outlive shutdown checkpointing.
}

TEST_F(MvccInsertTransactionTest, ApplyConflictRollsBackEarlierVertexAndEdges) {
  using namespace neug;
  NeugDB db;
  db.Open(db_dir);
  VersionManager vm;
  vm.init_ts({0, 0}, 4);
  Allocator alloc(MemoryLevel::kInMemory, "");
  InsertTestWalWriter writer;
  SnapshotGuard observer(db.graph_snapshot_store());
  auto& view = observer.get().mutable_view();
  auto person = view.schema().get_vertex_label_id("person");
  auto knows = view.schema().get_edge_label_id("knows");
  const auto count = view.EdgeNum(person, person, knows);
  MvccInsertTransaction loser(SnapshotGuard(db.graph_snapshot_store()), alloc,
                              writer, vm, vm.acquire_insert_timestamp());
  vid_t lid, existing;
  ASSERT_TRUE(loser.AddVertex(person, Value::INT64(3),
                              {Value::STRING("loser"), Value::INT64(20)}, lid));
  ASSERT_TRUE(loser.GetVertexIndex(person, Value::INT64(1), existing));
  const void* prop = nullptr;
  ASSERT_TRUE(loser.AddEdge(person, lid, person, existing, knows,
                            {Value::DOUBLE(0.5)}, prop));
  ASSERT_TRUE(loser.AddVertex(person, Value::INT64(4),
                              {Value::STRING("conflict"), Value::INT64(20)},
                              lid));
  MvccInsertTransaction winner(SnapshotGuard(db.graph_snapshot_store()), alloc,
                               writer, vm, vm.acquire_insert_timestamp());
  ASSERT_TRUE(winner.AddVertex(person, Value::INT64(4),
                               {Value::STRING("winner"), Value::INT64(30)},
                               lid));
  ASSERT_TRUE(winner.Commit());
  EXPECT_FALSE(loser.Commit());
  loser.Abort();  // Caller cleanup after failed Commit must be idempotent.
  EXPECT_EQ(writer.calls, 1u);  // Failed apply never reaches WAL append.
  EXPECT_FALSE(view.get_lid(person, Value::INT64(3), lid, 2));
  EXPECT_TRUE(view.get_lid(person, Value::INT64(4), lid, 2));
  EXPECT_EQ(view.EdgeNum(person, person, knows), count);
  observer.release();
  db.Close();
}

// Regression for https://github.com/alibaba/neug/issues/315: overlapping
// transactions with ordered commits must not leak the failed apply prefix.
TEST_F(MvccInsertTransactionTest, CommitConflictMustNotLeavePartialVertices) {
  using namespace neug;
  NeugDB db;
  NeugDBConfig config(db_dir);
  config.max_thread_num = 2;
  config.memory_level = MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<NeugDBService>(db);
  {
    auto winner_slot = svc->AcquireExecutionSlot();
    auto loser_slot = svc->AcquireExecutionSlot();
    auto winner = winner_slot->BeginMvccInsertTransaction();
    auto loser = loser_slot->BeginMvccInsertTransaction();
    ASSERT_LT(winner.timestamp(), loser.timestamp());
    const auto label = winner.schema().get_vertex_label_id("person");
    vid_t vid;
    auto add = [&](MvccInsertTransaction& txn, int64_t id) {
      return txn.AddVertex(label, Value::INT64(id),
                           {Value::STRING("reproducer"), Value::INT64(28)},
                           vid);
    };
    // Both stage the same PK before either commits. The loser writes 101
    // before conflicting on 100; its trailing vertex 102 is never applied.
    ASSERT_TRUE(add(winner, 100));
    ASSERT_TRUE(add(loser, 101));
    ASSERT_TRUE(add(loser, 100));
    ASSERT_TRUE(add(loser, 102));
    ASSERT_TRUE(winner.Commit());
    EXPECT_FALSE(loser.Commit());
    loser.Abort();
  }
  {
    auto slot = svc->AcquireExecutionSlot();
    auto txn = slot->BeginSnapshotReadTransaction();
    StorageReadInterface gi(txn.view(), txn.timestamp());
    const auto label = gi.schema().get_vertex_label_id("person");
    vid_t vid;
    for (int64_t id : {1, 2, 100}) {
      EXPECT_TRUE(gi.GetVertexIndex(label, Value::INT64(id), vid));
    }
    EXPECT_FALSE(gi.GetVertexIndex(label, Value::INT64(101), vid))
        << "Failed transaction leaked its applied prefix after Abort";
    EXPECT_FALSE(gi.GetVertexIndex(label, Value::INT64(102), vid));
    EXPECT_EQ(count_vertices(gi, label), 3u);
  }
  svc.reset();
  db.Close();
}

TEST_F(MvccInsertTransactionTest, PropertyFailureAlsoUndoesCurrentVertex) {
  using namespace neug;
  NeugDB db;
  db.Open(db_dir);
  VersionManager vm;
  vm.init_ts({0, 0}, 4);
  Allocator alloc(MemoryLevel::kInMemory, "");
  InsertTestWalWriter writer;
  SnapshotGuard observer(db.graph_snapshot_store());
  auto& view = observer.get().mutable_view();
  auto person = view.schema().get_vertex_label_id("person");
  // Deliberately damage a single column's capacity to inject a deterministic
  // failure after the second vertex's PK/timestamp and first property are set.
  auto* age = observer.get()
                  .mutable_graph()
                  ->get_vertex_table(person)
                  .get_table()
                  .get_column_by_id(1);
  const auto capacity = age->size();
  const auto base = view.LidNum(person);
  age->resize(base + 1);
  MvccInsertTransaction txn(SnapshotGuard(db.graph_snapshot_store()), alloc,
                            writer, vm, vm.acquire_insert_timestamp());
  vid_t lid;
  for (int id : {3, 4}) {
    ASSERT_TRUE(txn.AddVertex(person, Value::INT64(id),
                              {Value::STRING("partial"), Value::INT64(20)},
                              lid));
  }
  EXPECT_FALSE(txn.Commit());
  EXPECT_EQ(writer.calls, 0u);
  EXPECT_FALSE(view.get_lid(person, Value::INT64(3), lid, 1));
  EXPECT_FALSE(view.get_lid(person, Value::INT64(4), lid, 1));
  EXPECT_EQ(view.VertexNum(person), 2u);
  EXPECT_EQ(view.LidNum(person),
            base + 2);  // Tombstones retain physical slots.
  age->resize(capacity);
  observer.release();
  db.Close();
}

TEST_F(MvccInsertTransactionTest,
       EdgePropertyFailureUndoesBothDirectionsAndPoint) {
  using namespace neug;
  NeugDB db;
  db.Open(db_dir);
  VersionManager vm;
  vm.init_ts({0, 0}, 4);
  Allocator alloc(MemoryLevel::kInMemory, "");
  InsertTestWalWriter writer;
  SnapshotGuard observer(db.graph_snapshot_store());
  auto& view = observer.get().mutable_view();
  auto person = view.schema().get_vertex_label_id("person");
  auto software = view.schema().get_vertex_label_id("software");
  auto created = view.schema().get_edge_label_id("created");
  auto& table =
      observer.get().mutable_graph()->get_edge_table(person, software, created);
  const auto count = table.EdgeNum();
  auto* since = table.table()->get_column_by_id(1);
  const auto capacity = since->size();
  // Fail after both CSR records and the first property have been appended.
  since->resize(table.PropTableSize());
  MvccInsertTransaction txn(SnapshotGuard(db.graph_snapshot_store()), alloc,
                            writer, vm, vm.acquire_insert_timestamp());
  vid_t src, dst;
  ASSERT_TRUE(txn.AddVertex(person, Value::INT64(3),
                            {Value::STRING("partial"), Value::INT64(20)}, src));
  ASSERT_TRUE(txn.GetVertexIndex(software, Value::INT64(1), dst));
  const void* prop = nullptr;
  ASSERT_TRUE(txn.AddEdge(person, src, software, dst, created,
                          {Value::DOUBLE(0.2), Value::INT64(2026)}, prop));
  EXPECT_FALSE(txn.Commit());
  EXPECT_EQ(writer.calls, 0u);
  EXPECT_FALSE(view.get_lid(person, Value::INT64(3), src, 1));
  EXPECT_EQ(table.EdgeNum(), count);
  auto incoming = view.GetGenericIncomingView(person, software, created, 1);
  size_t seen = 0;
  for (auto lid : view.GetVertexSet(software, 1)) {
    auto edges = incoming.get_edges(lid);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      ++seen;
    }
  }
  EXPECT_EQ(seen, count);
  since->resize(capacity);
  observer.release();
  db.Close();
}

TEST_F(MvccInsertTransactionTest,
       EdgePropertyTombstonesStayInvisibleAcrossCheckpoints) {
  using namespace neug;
  for (auto level : {MemoryLevel::kInMemory, MemoryLevel::kSyncToFile}) {
    SCOPED_TRACE(static_cast<int>(level));
    // Start each storage mode from the same committed fixture.
    SetUp();
    NeugDBConfig config(db_dir);
    config.memory_level = level;
    config.checkpoint_on_close = true;
    NeugDB db;
    db.Open(config);
    auto svc = std::make_shared<NeugDBService>(db);
    auto verify = [&](bool replacement) {
      auto slot = svc->AcquireExecutionSlot();
      auto txn = slot->BeginSnapshotReadTransaction();
      StorageReadInterface gi(txn.view(), txn.timestamp());
      const auto person = gi.schema().get_vertex_label_id("person");
      const auto software = gi.schema().get_vertex_label_id("software");
      const auto created = gi.schema().get_edge_label_id("created");
      auto weight = gi.GetEdgeDataAccessor(person, software, created, 0);
      auto since = gi.GetEdgeDataAccessor(person, software, created, 1);
      for (bool incoming : {false, true}) {
        auto edges =
            incoming
                ? gi.GetGenericIncomingGraphView(software, person, created)
                : gi.GetGenericOutgoingGraphView(person, software, created);
        size_t seen = 0;
        for (auto lid : gi.GetVertexSet(incoming ? software : person)) {
          auto adjacency = edges.get_edges(lid);
          for (auto it = adjacency.begin(); it != adjacency.end(); ++it) {
            const auto src =
                gi.GetVertexId(person, incoming ? it.get_vertex() : lid)
                    .GetValue<int64_t>();
            const auto dst =
                gi.GetVertexId(software, incoming ? lid : it.get_vertex())
                    .GetValue<int64_t>();
            if (src == 1 && dst == 1) {
              EXPECT_DOUBLE_EQ(weight.get_data(it).GetValue<double>(), 0.8);
              EXPECT_EQ(since.get_data(it).GetValue<int64_t>(), 2021);
            } else if (src == 2 && dst == 2) {
              EXPECT_DOUBLE_EQ(weight.get_data(it).GetValue<double>(), 0.7);
              EXPECT_EQ(since.get_data(it).GetValue<int64_t>(), 2020);
            } else {
              EXPECT_TRUE(replacement);
              EXPECT_EQ(src, 1);
              EXPECT_EQ(dst, 2);
              EXPECT_DOUBLE_EQ(weight.get_data(it).GetValue<double>(), 0.9);
              EXPECT_EQ(since.get_data(it).GetValue<int64_t>(), 2030);
            }
            ++seen;
          }
        }
        EXPECT_EQ(seen, replacement ? 3u : 2u);
      }
    };
    size_t retained_rows;
    {
      SnapshotGuard observer(db.graph_snapshot_store());
      auto& graph = *observer.get().mutable_graph();
      const auto person = graph.schema().get_vertex_label_id("person");
      const auto software = graph.schema().get_vertex_label_id("software");
      const auto created = graph.schema().get_edge_label_id("created");
      auto& table = graph.get_edge_table(person, software, created);
      const auto initial_rows = table.PropTableSize();
      auto* since = table.table()->get_column_by_id(1);
      const auto capacity = since->size();
      for (int i = 0; i < 3; ++i) {
        auto slot = svc->AcquireExecutionSlot();
        auto txn = slot->BeginMvccInsertTransaction();
        vid_t src, dst;
        ASSERT_TRUE(txn.GetVertexIndex(person, Value::INT64(1), src));
        ASSERT_TRUE(txn.GetVertexIndex(software, Value::INT64(2), dst));
        // Both endpoints survive: a leaked edge cannot hide behind a deleted
        // vertex. Fail only after the CSR entries and weight are written.
        since->resize(table.PropTableSize());
        const void* prop = nullptr;
        ASSERT_TRUE(txn.AddEdge(person, src, software, dst, created,
                                {Value::DOUBLE(0.2), Value::INT64(2026)},
                                prop));
        EXPECT_FALSE(txn.Commit());
        txn.Abort();
        // Restore the injected capacity restriction before checkpointing.
        since->resize(capacity);
        EXPECT_EQ(table.PropTableSize(), initial_rows + i + 1);
        EXPECT_EQ(table.EdgeNum(), 2u);
      }
      retained_rows = table.PropTableSize();
    }
    verify(false);
    svc.reset();
    db.Close();  // Publish a checkpoint containing the unreachable rows.
    db.Open(config);
    svc = std::make_shared<NeugDBService>(db);
    verify(false);
    {
      auto slot = svc->AcquireExecutionSlot();
      auto txn = slot->BeginMvccInsertTransaction();
      const auto person = txn.schema().get_vertex_label_id("person");
      const auto software = txn.schema().get_vertex_label_id("software");
      const auto created = txn.schema().get_edge_label_id("created");
      EXPECT_EQ(
          db.graph().get_edge_table(person, software, created).PropTableSize(),
          retained_rows);
      vid_t src, dst;
      ASSERT_TRUE(txn.GetVertexIndex(person, Value::INT64(1), src));
      ASSERT_TRUE(txn.GetVertexIndex(software, Value::INT64(2), dst));
      const void* prop = nullptr;
      ASSERT_TRUE(txn.AddEdge(person, src, software, dst, created,
                              {Value::DOUBLE(0.9), Value::INT64(2030)}, prop));
      ASSERT_TRUE(txn.Commit());
      EXPECT_EQ(
          db.graph().get_edge_table(person, software, created).PropTableSize(),
          retained_rows + 1);
    }
    verify(true);
    svc.reset();
    db.Close();
    db.Open(config);
    svc = std::make_shared<NeugDBService>(db);
    verify(true);
    svc.reset();
    db.Close();
  }
}

TEST_F(MvccInsertTransactionTest, InsertCannotUseAnotherUnpublishedVertex) {
  using namespace neug;
  NeugDB db;
  db.Open(db_dir);
  VersionManager vm;
  vm.init_ts({0, 0}, 4);
  Allocator alloc(MemoryLevel::kInMemory, "");
  InsertTestWalWriter writer;
  SnapshotGuard observer(db.graph_snapshot_store());
  auto& view = observer.get().mutable_view();
  auto person = view.schema().get_vertex_label_id("person");
  auto knows = view.schema().get_edge_label_id("knows");
  MvccInsertTransaction first(SnapshotGuard(db.graph_snapshot_store()), alloc,
                              writer, vm, vm.acquire_insert_timestamp());
  vid_t lid;
  ASSERT_TRUE(first.AddVertex(person, Value::INT64(3),
                              {Value::STRING("pending"), Value::INT64(20)},
                              lid));
  writer.on_append = [&] {
    // Interleave a second insert after apply, before WAL/publication.
    InsertTestWalWriter second_writer;
    MvccInsertTransaction second(SnapshotGuard(db.graph_snapshot_store()),
                                 alloc, second_writer, vm,
                                 vm.acquire_insert_timestamp());
    vid_t physical, existing;
    EXPECT_TRUE(view.get_lid(person, Value::INT64(3), physical, 1));
    EXPECT_FALSE(second.GetVertexIndex(person, Value::INT64(3), lid));
    EXPECT_FALSE(view.GetInsertEndpoint(person, Value::INT64(3), lid, 0, 2));
    EXPECT_TRUE(second.GetVertexIndex(person, Value::INT64(1), existing));
    const void* prop = nullptr;
    EXPECT_THROW(second.AddEdge(person, physical, person, existing, knows,
                                {Value::DOUBLE(0.5)}, prop),
                 std::exception);
    EXPECT_EQ(second_writer.calls, 0u);
    return false;
  };
  EXPECT_FALSE(first.Commit());
  EXPECT_FALSE(view.get_lid(person, Value::INT64(3), lid, 2));
  observer.release();
  db.Close();
}

class LocalWalParserTest : public ::testing::Test {
 protected:
  std::string wal_dir_;

  void SetUp() override {
    auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    auto dir_name = std::string("neug_wal_parser_test_") +
                    std::to_string(::getpid()) + "_" + info->test_suite_name() +
                    "_" + info->name();
    wal_dir_ = (std::filesystem::temp_directory_path() / dir_name).string();
    if (std::filesystem::exists(wal_dir_)) {
      std::filesystem::remove_all(wal_dir_);
    }
    std::filesystem::create_directories(wal_dir_);
  }

  void TearDown() override {
    if (std::filesystem::exists(wal_dir_)) {
      std::filesystem::remove_all(wal_dir_);
    }
  }

  // Write a single WAL entry (header + payload) into a buffer.
  void AppendWalEntry(std::vector<char>& buf, uint32_t ts, uint8_t type,
                      const std::string& payload) {
    neug::WalHeader header;
    header.timestamp = ts;
    header.type = type;
    header.length = static_cast<int32_t>(payload.size());
    const char* hdr = reinterpret_cast<const char*>(&header);
    buf.insert(buf.end(), hdr, hdr + sizeof(neug::WalHeader));
    buf.insert(buf.end(), payload.begin(), payload.end());
  }

  // Append a terminator entry (timestamp=0) to mark end of WAL stream.
  void AppendWalTerminator(std::vector<char>& buf) {
    neug::WalHeader terminator;
    terminator.timestamp = 0;
    terminator.type = 0;
    terminator.length = 0;
    const char* hdr = reinterpret_cast<const char*>(&terminator);
    buf.insert(buf.end(), hdr, hdr + sizeof(neug::WalHeader));
  }

  // Write buffer contents to a .wal file in the WAL directory.
  void WriteWalFile(const std::string& filename, const std::vector<char>& buf) {
    auto path = wal_dir_ + "/" + filename;
    int fd = OsOpen(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_NE(fd, -1);
    auto bytes_written = OsWrite(fd, buf.data(), buf.size());
    ASSERT_NE(bytes_written, -1);
    ASSERT_EQ(static_cast<size_t>(bytes_written), buf.size());
    OsClose(fd);
  }
};

// Test: LocalWalParser can correctly parse a valid WAL file.
TEST_F(LocalWalParserTest, OpenAndParseValidWalFile) {
  std::vector<char> buf;
  std::string payload = "insert_vertex_data";
  AppendWalEntry(buf, /*ts=*/1, /*type=*/0, payload);  // insert WAL
  AppendWalTerminator(buf);
  WriteWalFile("thread_0_0.wal", buf);

  neug::LocalWalParser parser(wal_dir_);
  EXPECT_EQ(parser.last_ts(), 1);
  const auto& unit = parser.get_insert_wal(1);
  EXPECT_EQ(unit.size, payload.size());
  // The ptr should point to the payload data within the mmap region.
  EXPECT_NE(unit.ptr, nullptr);
  EXPECT_EQ(std::string(unit.ptr, unit.size), payload);
}

TEST_F(LocalWalParserTest, WriterOpenCloseWithoutAppendCreatesNoFile) {
  neug::LocalWalWriter writer(wal_dir_, 0);
  static constexpr char kUnused = '\0';
  for (size_t i = 0; i < 5; ++i) {
    writer.open(wal_dir_);
    EXPECT_TRUE(writer.append(&kUnused, 0));
    writer.close();
  }
  EXPECT_TRUE(std::filesystem::is_empty(wal_dir_));
}

TEST_F(LocalWalParserTest, WriterPersistsTerminatorAfterEachAppend) {
  neug::LocalWalWriter writer(wal_dir_, 0);
  writer.open(wal_dir_);

  std::vector<char> first_record;
  AppendWalEntry(first_record, 1, 0, "first");
  ASSERT_TRUE(writer.append(first_record.data(), first_record.size()));

  const auto wal_path = std::filesystem::directory_iterator(wal_dir_)->path();
  const auto expect_terminator_at = [&](size_t offset) {
    std::ifstream wal(wal_path, std::ios::binary);
    ASSERT_TRUE(wal);
    wal.seekg(static_cast<std::streamoff>(offset));
    neug::WalHeader terminator;
    wal.read(reinterpret_cast<char*>(&terminator), sizeof(terminator));
    ASSERT_TRUE(wal);
    EXPECT_EQ(terminator.timestamp, 0u);
    EXPECT_EQ(terminator.type, 0u);
    EXPECT_EQ(terminator.length, 0);
  };
  expect_terminator_at(first_record.size());

  std::vector<char> second_record;
  AppendWalEntry(second_record, 2, 0, "second");
  ASSERT_TRUE(writer.append(second_record.data(), second_record.size()));
  expect_terminator_at(first_record.size() + second_record.size());
  writer.close();

  neug::LocalWalParser parser(wal_dir_);
  EXPECT_EQ(parser.last_ts(), 2u);
}

TEST_F(LocalWalParserTest, WriterCreationFailureCanBeRetried) {
  const auto invalid_wal_dir =
      (std::filesystem::path(wal_dir_) / "not_a_directory").string();
  const int fd =
      OsOpen(invalid_wal_dir.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
  ASSERT_NE(fd, -1);
  ASSERT_EQ(OsClose(fd), 0);

  neug::LocalWalWriter writer(invalid_wal_dir, 0);
  writer.open(invalid_wal_dir);
  std::vector<char> record;
  const std::string payload = "retry_after_creation_failure";
  AppendWalEntry(record, 1, 0, payload);
  EXPECT_FALSE(writer.append(record.data(), record.size()));

  ASSERT_TRUE(std::filesystem::remove(invalid_wal_dir));
  ASSERT_TRUE(std::filesystem::create_directory(invalid_wal_dir));
  ASSERT_TRUE(writer.append(record.data(), record.size()));
  EXPECT_NO_THROW(writer.close());

  neug::LocalWalParser parser(invalid_wal_dir);
  const auto& unit = parser.get_insert_wal(1);
  EXPECT_EQ(std::string(unit.ptr, unit.size), payload);
}

TEST_F(LocalWalParserTest, WriterDoesNotCreateMissingDirectory) {
  const auto missing_wal_dir = wal_dir_ + "/missing/wal";
  neug::LocalWalWriter writer(missing_wal_dir, 0);
  writer.open(missing_wal_dir);
  EXPECT_TRUE(writer.append(nullptr, 0));
  const char record = '\0';
  EXPECT_FALSE(writer.append(&record, 1));
  EXPECT_FALSE(std::filesystem::exists(wal_dir_ + "/missing"));
}

#ifndef _WIN32
TEST_F(LocalWalParserTest, WriterRetriesDirectorySyncBeforeWriting) {
  if (::geteuid() == 0) {
    GTEST_SKIP() << "Running as root; directory permissions are not enforced";
  }
  struct RestorePermissions {
    const std::string& path;
    ~RestorePermissions() { ::chmod(path.c_str(), 0700); }
  } restore{wal_dir_};

  // Allow creating a WAL file but deny opening its directory for fsync().
  ASSERT_EQ(::chmod(wal_dir_.c_str(), 0300), 0);
  const int probe_fd = ::open(wal_dir_.c_str(), O_RDONLY);
  if (probe_fd != -1) {
    ::close(probe_fd);
    GTEST_SKIP() << "Directory permissions are not enforced on this filesystem";
  }

  neug::LocalWalWriter writer(wal_dir_, 0);
  writer.open(wal_dir_);
  std::vector<char> record;
  const std::string payload = "retry_after_directory_sync_failure";
  AppendWalEntry(record, 1, 0, payload);

  for (int attempt = 0; attempt < 2; ++attempt) {
    EXPECT_FALSE(writer.append(record.data(), record.size()));
  }

  ASSERT_EQ(::chmod(wal_dir_.c_str(), 0700), 0);
  for (const auto& file : std::filesystem::directory_iterator(wal_dir_)) {
    EXPECT_EQ(file.file_size(), 0u);
  }
  ASSERT_TRUE(writer.append(record.data(), record.size()));
  writer.close();
  neug::LocalWalParser parser(wal_dir_);
  const auto& unit = parser.get_insert_wal(1);
  EXPECT_EQ(std::string(unit.ptr, unit.size), payload);
}

// Test: LocalWalParser throws IOException when ::open() on a WAL file fails
// (e.g. permission denied). This covers the fd == -1 check.
// Note: running as root or on permission-ignoring filesystems bypasses
// chmod(0000), so we pre-flight the open() and skip when it still succeeds.
TEST_F(LocalWalParserTest, OpenUnreadableWalFileThrowsIOException) {
  // Root (euid 0) bypasses file-permission checks; skip early.
  if (::geteuid() == 0) {
    GTEST_SKIP() << "Running as root; chmod(0000) does not block open()";
  }

  auto file_path = wal_dir_ + "/unreadable.wal";
  int fd = ::open(file_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
  ASSERT_NE(fd, -1);
  const char data[] = "some wal data";
  auto bytes_written = ::write(fd, data, sizeof(data));
  ASSERT_NE(bytes_written, -1);
  ASSERT_EQ(static_cast<size_t>(bytes_written), sizeof(data));
  ::close(fd);

  ASSERT_EQ(chmod(file_path.c_str(), 0000), 0);

  // Pre-flight: verify the OS actually denies O_RDONLY access.
  // Some CI filesystems or capability sets may still allow the open.
  int probe_fd = ::open(file_path.c_str(), O_RDONLY);
  if (probe_fd != -1) {
    ::close(probe_fd);
    chmod(file_path.c_str(), 0644);
    GTEST_SKIP() << "chmod(0000) did not block open() on this platform";
  }

  try {
    neug::LocalWalParser parser(wal_dir_);
    FAIL() << "Expected IOException but none was thrown";
  } catch (const neug::exception::IOException& e) {
    EXPECT_NE(std::string(e.what()).find("Failed to open wal file"),
              std::string::npos);
  }

  chmod(file_path.c_str(), 0644);
}
#endif

#ifndef _WIN32
// Test: LocalWalParser throws IOException when mmap() fails.
// Uses RLIMIT_AS to deterministically exhaust the virtual-address space
// so mmap() reliably returns MAP_FAILED, avoiding the flakiness of the
// sparse-file approach (many kernels accept such mappings via overcommit
// and only fault on first access).
TEST_F(LocalWalParserTest, MmapFailureThrowsIOException) {
  // Create a small but non-empty WAL file so mmap() is genuinely attempted.
  auto file_path = wal_dir_ + "/mmap_fail_test.wal";
  {
    int fd = ::open(file_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_NE(fd, -1);
    const char data[] = "placeholder wal content";
    auto bytes_written = ::write(fd, data, sizeof(data));
    ASSERT_NE(bytes_written, -1);
    ASSERT_EQ(static_cast<size_t>(bytes_written), sizeof(data));
    ::close(fd);
  }

  // Clamp the virtual-address-space limit to 1 byte so any subsequent
  // mmap() call is guaranteed to fail with ENOMEM / MAP_FAILED.
  struct rlimit old_rlimit;
  ASSERT_EQ(::getrlimit(RLIMIT_AS, &old_rlimit), 0);
  const struct rlimit tight_rlimit = {1, old_rlimit.rlim_max};
  if (::setrlimit(RLIMIT_AS, &tight_rlimit) != 0) {
    GTEST_SKIP() << "Cannot lower RLIMIT_AS on this platform; skipping test";
  }

  bool threw = false;
  bool msg_ok = false;
  try {
    neug::LocalWalParser parser(wal_dir_);
  } catch (const neug::exception::IOException& e) {
    threw = true;
    // Use strstr (no heap allocation) while RLIMIT_AS is still clamped.
    msg_ok = (std::strstr(e.what(), "Failed to mmap wal file") != nullptr);
  }

  // Restore RLIMIT_AS before any GTEST assertions that may heap-allocate.
  ::setrlimit(RLIMIT_AS, &old_rlimit);

  EXPECT_TRUE(threw) << "Expected IOException but none was thrown";
  EXPECT_TRUE(msg_ok)
      << "Exception message did not contain 'Failed to mmap wal file'";
}
#endif

// Test: Opening an empty WAL directory does not throw and last_ts is 0.
TEST_F(LocalWalParserTest, OpenEmptyWalDirNoThrow) {
  neug::LocalWalParser parser(wal_dir_);
  EXPECT_EQ(parser.last_ts(), 0);
}
