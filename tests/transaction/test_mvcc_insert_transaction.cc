
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
#include "neug/transaction/transaction_utils.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/local_wal_parser.h"
#include "neug/transaction/wal/local_wal_writer.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/array_column.h"
#include "neug/utils/property/vec_column.h"

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
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <stdexcept>
#include <string>
#include <thread>
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
    neug::SetBeforeInsertApplyForTest({});
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

TEST_F(MvccInsertTransactionTest, ReplayEdgeIndexVisitsGrowLinearlyAcrossWals) {
  using namespace neug;
  NeugDB db;
  db.Open(db_dir);
  SnapshotGuard snapshot(db.graph_snapshot_store());
  Allocator alloc(MemoryLevel::kInMemory, "");
  auto graph = snapshot.get().mutable_graph()->Clone();
  timestamp_t ts = 1;
  int scenario = 0;
  for (bool bundled : {false, true}) {
    for (bool parallel : {false, true}) {
      for (int n : {64, 256}) {
        SCOPED_TRACE(::testing::Message()
                     << "bundled=" << bundled << " parallel=" << parallel
                     << " n=" << n);
        RecoveryEdgeIndex index;
        ++scenario;
        auto replay = [&](InArchive& arc) {
          ReplayCowGraphWal(*graph, ts++, arc.GetBuffer(), arc.GetSize(), alloc,
                            &index);
          arc.Clear();
        };
        const std::string dst_type = bundled ? "person" : "software";
        const std::string edge_type = bundled ? "knows" : "created";
        auto props = [&](double weight) {
          std::vector<Value> values{Value::DOUBLE(weight)};
          if (!bundled)
            values.push_back(Value::INT64(2024));
          return values;
        };
        auto dst_oid = [&](int i) {
          return Value::INT64(scenario * 1000 + (parallel ? 0 : i));
        };
        const auto source = Value::INT64(100000 + scenario);
        InArchive arc;
        InsertVertexRedo::Serialize(
            arc, "person", source, {Value::STRING("source"), Value::INT64(20)});
        for (int i = 0; i < (parallel ? 1 : n); ++i) {
          std::vector<Value> values{Value::STRING("target")};
          values.push_back(bundled ? Value::INT64(20) : Value::STRING("C++"));
          InsertVertexRedo::Serialize(arc, dst_type, dst_oid(i), values);
        }
        for (int i = 0; i < n; ++i) {
          InsertEdgeRedo::Serialize(arc, "person", source, dst_type, dst_oid(i),
                                    edge_type, props(0));
        }
        replay(arc);
        auto update = [&](int i, double before_value, double after_value) {
          auto before = props(before_value);
          // Deliberately invalid persisted offsets: replay must resolve its
          // own slots, and retain every identical parallel-edge candidate.
          UpdateEdgePropRedo::Serialize(arc, "person", source, dst_type,
                                        dst_oid(i), edge_type, 999999, 999999,
                                        0, Value::DOUBLE(after_value), &before);
        };
        for (int i = 0; i < n; ++i) {
          update(i, 0, 1);
          // Cover both a batch in one WAL and repeated single-operation WALs.
          if (i >= n / 2)
            replay(arc);
        }
        EXPECT_LE(index.GetSlotVisitsForTest(), 4u * n);
        for (int i = 0; i < n; ++i) {
          auto before = props(1);
          RemoveEdgeRedo::Serialize(arc, "person", source, dst_type, dst_oid(i),
                                    edge_type, 999999, 999999, &before);
          replay(arc);
        }
        EXPECT_LE(index.GetSlotVisitsForTest(), 6u * n);

        // Clone/grow between replay calls must not retain stale graph pointers.
        graph = graph->Clone();
        auto person = graph->schema().get_vertex_label_id("person");
        ASSERT_TRUE(graph->EnsureCapacity(person, 8192).ok());
        for (int i = 0; i < n; ++i) {
          InsertEdgeRedo::Serialize(arc, "person", source, dst_type, dst_oid(i),
                                    edge_type, props(2));
          // Exercise the separate insert-WAL ingestion path between updates.
          GraphView view(*graph);
          MvccInsertTransaction::IngestWal(view, ts++, arc.GetBuffer(),
                                           arc.GetSize(), alloc);
          arc.Clear();
          update(i, 2, 3);
          replay(arc);
        }
        // A tombstone prefix must not be scanned again by begin()/iteration.
        EXPECT_LE(index.GetSlotVisitsForTest(), 10u * n);
        auto dst_label = graph->schema().get_vertex_label_id(dst_type);
        auto edge = graph->schema().get_edge_label_id(edge_type);
        const auto& table = graph->get_edge_table(person, dst_label, edge);
        vid_t src;
        ASSERT_TRUE(graph->get_lid(person, source, src, MAX_TIMESTAMP));
        size_t count = 0;
        for (auto it : table.get_outgoing_view(MAX_TIMESTAMP).get_edges(src)) {
          (void) it;
          ++count;
        }
        EXPECT_EQ(count, static_cast<size_t>(n));
        for (int i = 0; i < (parallel ? 1 : n); ++i) {
          vid_t dst;
          ASSERT_TRUE(
              graph->get_lid(dst_label, dst_oid(i), dst, MAX_TIMESTAMP));
          auto edges = table.get_incoming_view(MAX_TIMESTAMP).get_edges(dst);
          size_t incoming = 0;
          for (auto it = edges.begin(); it != edges.end(); ++it) {
            EXPECT_EQ(it.get_vertex(), src);
            EXPECT_EQ(
                table.get_edge_data_accessor(0).get_data(it).GetValue<double>(),
                3);
            ++incoming;
          }
          EXPECT_EQ(incoming, static_cast<size_t>(parallel ? n : 1));
        }
      }
    }
  }
  snapshot.release();
  db.Close();
}

TEST_F(MvccInsertTransactionTest,
       ReplayEdgeIndexTracksSchemaAndCascadeChanges) {
  using namespace neug;
  NeugDB db;
  db.Open(db_dir);
  SnapshotGuard snapshot(db.graph_snapshot_store());
  auto graph = snapshot.get().mutable_graph()->Clone();
  Allocator alloc(MemoryLevel::kInMemory, "");
  RecoveryEdgeIndex index;
  timestamp_t ts = 1;
  InArchive arc;
  auto replay = [&] {
    ReplayCowGraphWal(*graph, ts++, arc.GetBuffer(), arc.GetSize(), alloc,
                      &index);
    arc.Clear();
  };
  auto update = [&](int64_t endpoint, int column,
                    const std::vector<Value>& before, const Value& value) {
    UpdateEdgePropRedo::Serialize(arc, "person", Value::INT64(endpoint),
                                  "software", Value::INT64(endpoint), "created",
                                  999999, 999999, column, value, &before);
    replay();
  };
  update(1, 0, {Value::DOUBLE(0.8), Value::INT64(2021)}, Value::DOUBLE(1));
  // Legacy WAL mutations must refresh an index built by newer WAL records.
  UpdateEdgePropRedo::Serialize(arc, "person", Value::INT64(1), "software",
                                Value::INT64(1), "created", 0, 0, 0,
                                Value::DOUBLE(2));
  replay();
  update(1, 0, {Value::DOUBLE(2), Value::INT64(2021)}, Value::DOUBLE(3));
  AddEdgePropertiesParamBuilder add;
  AddEdgePropertiesRedo::Serialize(
      arc, "person", "software", "created",
      add.Properties({{"extra", Value::INT64(7)}}).Build());
  replay();
  update(1, 2, {Value::DOUBLE(3), Value::INT64(2021), Value::INT64(7)},
         Value::INT64(8));
  DeleteEdgePropertiesParamBuilder drop;
  DeleteEdgePropertiesRedo::Serialize(arc, "person", "software", "created",
                                      drop.DeleteProperties({"since"}).Build());
  replay();
  update(1, 0, {Value::DOUBLE(3), Value::INT64(8)}, Value::DOUBLE(4));
  // Removing a vertex also deletes incident edges outside the edge-WAL path.
  RemoveVertexRedo::Serialize(arc, "software", Value::INT64(1));
  replay();
  InsertVertexRedo::Serialize(
      arc, "software", Value::INT64(1),
      {Value::STRING("replacement"), Value::STRING("C++")});
  replay();
  InsertEdgeRedo::Serialize(arc, "person", Value::INT64(1), "software",
                            Value::INT64(1), "created",
                            {Value::DOUBLE(5), Value::INT64(9)});
  replay();
  update(1, 0, {Value::DOUBLE(5), Value::INT64(9)}, Value::DOUBLE(6));
  index.Clear();  // Same boundary used by NeugDB::ingestWals.
  graph->Compact();
  update(1, 1, {Value::DOUBLE(6), Value::INT64(9)}, Value::INT64(10));
  update(2, 0, {Value::DOUBLE(0.7), Value::INT64(7)}, Value::DOUBLE(11));
  snapshot.release();
  db.Close();
}

TEST_F(MvccInsertTransactionTest,
       ReplayEdgeIndexHandlesSingleSlotReuseAndOneDirection) {
  using namespace neug;
  NeugDB db;
  db.Open(db_dir);
  SnapshotGuard snapshot(db.graph_snapshot_store());
  Allocator alloc(MemoryLevel::kInMemory, "");
  for (bool incoming_only : {false, true}) {
    auto graph = snapshot.get().mutable_graph()->Clone();
    RecoveryEdgeIndex index;
    timestamp_t ts = 1;
    InArchive arc;
    auto replay = [&] {
      ReplayCowGraphWal(*graph, ts++, arc.GetBuffer(), arc.GetSize(), alloc,
                        &index);
      arc.Clear();
    };
    CreateEdgeTypeParamBuilder builder;
    CreateEdgeTypeRedo::Serialize(
        arc, builder.SrcLabel("person")
                 .DstLabel("software")
                 .EdgeLabel("single")
                 .Properties({{"weight", Value::DOUBLE(0)}})
                 .OEEdgeStrategy(incoming_only ? EdgeStrategy::kNone
                                               : EdgeStrategy::kSingle)
                 .IEEdgeStrategy(EdgeStrategy::kSingle)
                 .Build());
    replay();
    for (int i = 0; i < 32; ++i) {
      InsertEdgeRedo::Serialize(arc, "person", Value::INT64(1), "software",
                                Value::INT64(1), "single", {Value::DOUBLE(i)});
      replay();
      std::vector<Value> before{Value::DOUBLE(i)};
      if (!incoming_only) {
        UpdateEdgePropRedo::Serialize(
            arc, "person", Value::INT64(1), "software", Value::INT64(1),
            "single", 999999, 999999, 0, Value::DOUBLE(i + 100), &before);
        replay();
        before = {Value::DOUBLE(i + 100)};
      }
      RemoveEdgeRedo::Serialize(arc, "person", Value::INT64(1), "software",
                                Value::INT64(1), "single", 999999, 999999,
                                &before);
      replay();
    }
    EXPECT_LE(index.GetSlotVisitsForTest(), 8u * 32);
  }
  snapshot.release();
  db.Close();
}

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

TEST_F(MvccInsertTransactionTest, CrashRecoversEdgeUpdatesAfterUndoHoles) {
  using namespace neug;
  ASSERT_EXIT(
      ([&] {
        NeugDB db;
        NeugDBConfig config(db_dir);
        config.memory_level = MemoryLevel::kSyncToFile;
        config.max_thread_num = 1;
        config.checkpoint_on_close = false;
        db.Open(config);
        Allocator alloc(MemoryLevel::kInMemory, "");
        VersionManager vm;
        vm.init_ts({0, 0}, 4);
        InsertTestWalWriter reject;
        reject.on_append = [] { return false; };
        {
          MvccInsertTransaction txn(SnapshotGuard(db.graph_snapshot_store()),
                                    alloc, reject, vm,
                                    vm.acquire_insert_timestamp());
          const auto person = txn.schema().get_vertex_label_id("person");
          const auto software = txn.schema().get_vertex_label_id("software");
          vid_t src, dst, sw;
          txn.GetVertexIndex(person, Value::INT64(1), src);
          txn.GetVertexIndex(person, Value::INT64(2), dst);
          txn.GetVertexIndex(software, Value::INT64(1), sw);
          const void* prop = nullptr;
          if (!txn.AddEdge(person, src, person, dst,
                           txn.schema().get_edge_label_id("knows"),
                           {Value::DOUBLE(99)}, prop) ||
              !txn.AddEdge(person, src, software, sw,
                           txn.schema().get_edge_label_id("created"),
                           {Value::DOUBLE(99), Value::INT64(2024)}, prop) ||
              txn.Commit() || reject.calls != 1) {
            std::_Exit(2);
          }
        }
        auto conn = db.Connect();
        for (int weight : {42, 42, 43, 44}) {
          if (!conn->Query("MATCH (a:person {id:1}), (b:person {id:2}) "
                           "CREATE (a)-[:knows {closeness:" +
                           std::to_string(weight) + ".0}]->(b)") ||
              !conn->Query("MATCH (a:person {id:1}), (b:software {id:1}) "
                           "CREATE (a)-[:created {weight:" +
                           std::to_string(weight) + ".0, since:2024}]->(b)")) {
            std::_Exit(3);
          }
        }
        for (const std::string query :
             {"MATCH ()-[r:knows]->() WHERE r.closeness=42.0 SET "
              "r.closeness=52.0",
              "MATCH ()-[r:created]->() WHERE r.weight=42.0 SET r.weight=52.0, "
              "r.since=2025",
              "MATCH ()-[r:knows]->() WHERE r.closeness=43.0 DELETE r",
              "MATCH ()-[r:created]->() WHERE r.weight=43.0 DELETE r"}) {
          if (!conn->Query(query)) {
            std::_Exit(4);
          }
        }
        // No destructors or checkpoint: recovery must reconstruct successful
        // inserts without the physical slots left by the failed transaction.
        std::_Exit(0);
      }()),
      ::testing::ExitedWithCode(0), ".*");
  NeugDB reopened;
  NeugDBConfig config(db_dir);
  config.checkpoint_on_close = false;
  reopened.Open(config);
  SnapshotGuard observer(reopened.graph_snapshot_store());
  auto& graph = *observer.get().mutable_graph();
  const auto person = graph.schema().get_vertex_label_id("person");
  vid_t src;
  ASSERT_TRUE(graph.get_lid(person, Value::INT64(1), src, MAX_TIMESTAMP));
  for (bool bundled : {false, true}) {
    auto dst_label =
        bundled ? person : graph.schema().get_vertex_label_id("software");
    auto edge = graph.schema().get_edge_label_id(bundled ? "knows" : "created");
    auto index = graph.schema().generate_edge_label(person, dst_label, edge);
    const auto& table = graph.get_edge_table_by_index(index);
    vid_t dst;
    ASSERT_TRUE(graph.get_lid(dst_label, Value::INT64(bundled ? 2 : 1), dst,
                              MAX_TIMESTAMP));
    for (bool incoming : {false, true}) {
      auto edges = incoming
                       ? table.get_incoming_view(MAX_TIMESTAMP).get_edges(dst)
                       : table.get_outgoing_view(MAX_TIMESTAMP).get_edges(src);
      size_t updated = 0, untouched = 0;
      for (auto it = edges.begin(); it != edges.end(); ++it) {
        const auto weight =
            table.get_edge_data_accessor(0).get_data(it).GetValue<double>();
        updated += weight == 52;
        untouched += weight == 44;
        if (!bundled && weight == 52) {
          EXPECT_EQ(
              table.get_edge_data_accessor(1).get_data(it).GetValue<int64_t>(),
              2025);
        }
        EXPECT_NE(weight, 42);
        EXPECT_NE(weight, 43);
        EXPECT_NE(weight, 99);
      }
      EXPECT_EQ(updated, 2u);
      EXPECT_EQ(untouched, 1u);
    }
  }
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
  // Repeated transaction Abort has no remaining journal entries, even after
  // another transaction has reused the VID.
  txn.Abort();
  EXPECT_TRUE(view.get_lid(person, Value::INT64(3), added, next_ts));
  EXPECT_EQ(view.VertexNum(person), 3u);
  observer.release();
  db.Close();  // CSR allocation buffers must outlive shutdown checkpointing.
}

TEST_F(MvccInsertTransactionTest,
       RepeatedAbortPreservesVidReusedByEarlierTimestamp) {
  using namespace neug;
  NeugDB db;
  NeugDBConfig config(db_dir);
  config.memory_level = MemoryLevel::kInMemory;
  db.Open(config);
  VersionManager vm;
  vm.init_ts({0, 0}, 4);
  Allocator alloc(MemoryLevel::kInMemory, "");
  InsertTestWalWriter replacement_writer, failed_writer;
  SnapshotGuard observer(db.graph_snapshot_store());
  auto& view = observer.get().mutable_view();
  const auto person = view.schema().get_vertex_label_id("person");
  const auto initial_count = view.VertexNum(person);
  const auto initial_slots = view.LidNum(person);

  // Reservation order differs from apply order. A stale delete using <= ts
  // would accept the replacement's earlier timestamp.
  const auto replacement_ts = vm.acquire_insert_timestamp();
  MvccInsertTransaction replacement(SnapshotGuard(db.graph_snapshot_store()),
                                    alloc, replacement_writer, vm,
                                    replacement_ts);
  const auto failed_ts = vm.acquire_insert_timestamp();
  MvccInsertTransaction failed(SnapshotGuard(db.graph_snapshot_store()), alloc,
                               failed_writer, vm, failed_ts);
  ASSERT_LT(replacement_ts, failed_ts);
  vid_t provisional;
  ASSERT_TRUE(replacement.AddVertex(
      person, Value::INT64(100),
      {Value::STRING("replacement"), Value::INT64(30)}, provisional));
  ASSERT_TRUE(failed.AddVertex(person, Value::INT64(100),
                               {Value::STRING("failed"), Value::INT64(20)},
                               provisional));
  vid_t failed_vid = INVALID_VID;
  failed_writer.on_append = [&] {
    EXPECT_TRUE(view.get_lid(person, Value::INT64(100), failed_vid, failed_ts));
    EXPECT_EQ(view.VertexNum(person), initial_count + 1);
    return false;
  };
  ASSERT_FALSE(failed.Commit());
  ASSERT_EQ(failed_writer.calls,
            1u);  // The insertion actually reached storage.
  ASSERT_NE(failed_vid, INVALID_VID);
  ASSERT_EQ(view.VertexNum(person), initial_count);
  EXPECT_EQ(vm.insert_read_timestamp(), 0u);

  ASSERT_TRUE(replacement.Commit());
  ASSERT_EQ(replacement_writer.calls, 1u);
  vid_t replacement_vid = INVALID_VID;
  ASSERT_TRUE(
      view.get_lid(person, Value::INT64(100), replacement_vid, replacement_ts));
  ASSERT_EQ(replacement_vid, failed_vid);  // Real reuse, not another VID.
  ASSERT_EQ(view.LidNum(person), initial_slots + 1);
  for (int i = 0; i < 3; ++i) {
    failed.Abort();
  }
  EXPECT_TRUE(view.IsValidLid(person, replacement_vid, replacement_ts));
  EXPECT_EQ(view.VertexNum(person), initial_count + 1);
  EXPECT_EQ(view.GetVertexPropertyColumn(person, 0)
                ->get_any(replacement_vid)
                .GetValue<std::string>(),
            "replacement");
  size_t visible = 0;
  for (auto vid : view.GetVertexSet(person, vm.insert_read_timestamp())) {
    (void) vid;
    ++visible;
  }
  EXPECT_EQ(visible, initial_count + 1);
  observer.release();
  db.Close();
}

TEST_F(MvccInsertTransactionTest, RepeatedAbortPreservesReusedSingleEdgeSlots) {
  using namespace neug;
  NeugDB db;
  NeugDBConfig config(db_dir);
  config.memory_level = MemoryLevel::kInMemory;
  config.checkpoint_on_close = true;
  db.Open(config);
  {
    auto svc = std::make_shared<NeugDBService>(db);
    auto slot = svc->AcquireExecutionSlot();
    auto txn = slot->BeginSnapshotCowWriteTransaction();
    auto storage = txn.OpenStorage();
    ASSERT_TRUE(
        storage.CreateEdgeType(CreateEdgeTypeParamBuilder()
                                   .SrcLabel("person")
                                   .DstLabel("person")
                                   .EdgeLabel("single_link")
                                   .Properties({{"weight", Value::DOUBLE(0)}})
                                   .OEEdgeStrategy(EdgeStrategy::kSingle)
                                   .IEEdgeStrategy(EdgeStrategy::kSingle)
                                   .Build()));
    ASSERT_TRUE(txn.Commit());
  }
  db.Close();
  db.Open(config);
  VersionManager vm;
  vm.init_ts({0, 0}, 4);
  Allocator alloc(MemoryLevel::kInMemory, "");
  InsertTestWalWriter replacement_writer, failed_writer;
  SnapshotGuard observer(db.graph_snapshot_store());
  auto& view = observer.get().mutable_view();
  const auto person = view.schema().get_vertex_label_id("person");
  const auto edge = view.schema().get_edge_label_id("single_link");
  auto& table =
      observer.get().mutable_graph()->get_edge_table(person, person, edge);
  const auto replacement_ts = vm.acquire_insert_timestamp();
  MvccInsertTransaction replacement(SnapshotGuard(db.graph_snapshot_store()),
                                    alloc, replacement_writer, vm,
                                    replacement_ts);
  const auto failed_ts = vm.acquire_insert_timestamp();
  MvccInsertTransaction failed(SnapshotGuard(db.graph_snapshot_store()), alloc,
                               failed_writer, vm, failed_ts);
  ASSERT_LT(replacement_ts, failed_ts);
  vid_t src, dst;
  ASSERT_TRUE(failed.GetVertexIndex(person, Value::INT64(1), src));
  ASSERT_TRUE(failed.GetVertexIndex(person, Value::INT64(2), dst));
  const void* prop = nullptr;
  ASSERT_TRUE(replacement.AddEdge(person, src, person, dst, edge,
                                  {Value::DOUBLE(20)}, prop));
  ASSERT_TRUE(failed.AddEdge(person, src, person, dst, edge,
                             {Value::DOUBLE(10)}, prop));
  auto check_edge = [&](timestamp_t ts, double weight) {
    EXPECT_EQ(table.EdgeNum(), 1u);
    for (bool incoming : {false, true}) {
      auto edges = incoming ? table.get_incoming_view(ts).get_edges(dst)
                            : table.get_outgoing_view(ts).get_edges(src);
      auto it = edges.begin();
      ASSERT_TRUE(it != edges.end());
      // Both single CSRs use offset zero, including after reuse.
      EXPECT_EQ(it.cur, edges.start_ptr);
      EXPECT_EQ(it.get_vertex(), incoming ? src : dst);
      EXPECT_EQ(it.get_timestamp(), ts);
      EXPECT_EQ(table.get_edge_data_accessor(0).get_data(it).GetValue<double>(),
                weight);
      ++it;
      EXPECT_TRUE(it == edges.end());
    }
  };
  failed_writer.on_append = [&] {
    check_edge(failed_ts, 10);
    return false;
  };
  ASSERT_FALSE(failed.Commit());
  ASSERT_EQ(failed_writer.calls, 1u);
  ASSERT_EQ(table.EdgeNum(), 0u);
  for (bool incoming : {false, true}) {
    auto edges = incoming ? table.get_incoming_view(failed_ts).get_edges(dst)
                          : table.get_outgoing_view(failed_ts).get_edges(src);
    EXPECT_TRUE(edges.begin() == edges.end());
  }
  ASSERT_TRUE(replacement.Commit());
  ASSERT_EQ(replacement_writer.calls, 1u);
  check_edge(replacement_ts, 20);
  for (int i = 0; i < 3; ++i) {
    failed.Abort();
    check_edge(replacement_ts, 20);
  }
  observer.release();
  db.Close();
}

TEST_F(MvccInsertTransactionTest, EdgeUndoVisitsOnlyInsertedSlots) {
  using namespace neug;
  for (bool parallel : {false, true}) {
    for (int n : {32, 256, 1024}) {
      SCOPED_TRACE(parallel ? "parallel" : "star");
      SCOPED_TRACE(n);
      NeugDB db;
      db.Open(db_dir);
      VersionManager vm;
      vm.init_ts({0, 0}, 4);
      Allocator alloc(MemoryLevel::kInMemory, "");
      InsertTestWalWriter writer;
      SnapshotGuard observer(db.graph_snapshot_store());
      auto& view = observer.get().mutable_view();
      const auto person = view.schema().get_vertex_label_id("person");
      const auto knows = view.schema().get_edge_label_id("knows");
      const auto initial_edges = view.EdgeNum(person, person, knows);
      MvccInsertTransaction txn(SnapshotGuard(db.graph_snapshot_store()), alloc,
                                writer, vm, vm.acquire_insert_timestamp());
      vid_t src, dst;
      ASSERT_TRUE(txn.GetVertexIndex(person, Value::INT64(1), src));
      ASSERT_TRUE(txn.GetVertexIndex(person, Value::INT64(2), dst));
      const void* prop = nullptr;
      for (int i = 0; i < n; ++i) {
        if (!parallel) {
          ASSERT_TRUE(txn.AddVertex(person, Value::INT64(i + 100),
                                    {Value::STRING("target"), Value::INT64(20)},
                                    dst));
        }
        ASSERT_TRUE(txn.AddEdge(person, src, person, dst, knows,
                                {Value::DOUBLE(0.5)}, prop));
      }
      writer.on_append = [&] {
        EXPECT_EQ(view.EdgeNum(person, person, knows), initial_edges + n);
        ResetDeleteEdgeSlotVisitsForTest();
        return false;
      };
      EXPECT_FALSE(txn.Commit());
      ASSERT_EQ(writer.calls, 1u);  // Must reach apply, not capacity retry.
      EXPECT_EQ(DeleteEdgeSlotVisitsForTest(), 2u * n);
      EXPECT_EQ(view.EdgeNum(person, person, knows), initial_edges);
      EXPECT_EQ(view.VertexNum(person), 2u);
      for (bool incoming : {false, true}) {
        auto csr = incoming
                       ? view.GetGenericIncomingView(person, person, knows, 1)
                       : view.GetGenericOutgoingView(person, person, knows, 1);
        size_t seen = 0;
        for (auto lid : view.GetVertexSet(person, 1)) {
          auto edges = csr.get_edges(lid);
          for (auto it = edges.begin(); it != edges.end(); ++it) {
            ++seen;
          }
        }
        EXPECT_EQ(seen, initial_edges);
      }
      observer.release();
      db.Close();
    }
  }
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
  MvccInsertTransaction txn(SnapshotGuard(db.graph_snapshot_store()), alloc,
                            writer, vm, vm.acquire_insert_timestamp());
  vid_t lid;
  for (int id : {3, 4}) {
    ASSERT_TRUE(txn.AddVertex(person, Value::INT64(id),
                              {Value::STRING("partial"), Value::INT64(20)},
                              lid));
  }
  SetBeforeInsertApplyForTest([&] { age->resize(base + 1); });
  EXPECT_FALSE(txn.Commit());
  EXPECT_FALSE(txn.RequiresCapacityRetry());
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

TEST_F(MvccInsertTransactionTest, DuplicatePrimaryKeyPartialWriteUndoesOwnVid) {
  using namespace neug;
  NeugDB db;
  db.Open(db_dir);
  VersionManager vm;
  vm.init_ts({0, 0}, 4);
  Allocator alloc_a(MemoryLevel::kInMemory, "");
  Allocator alloc_b(MemoryLevel::kInMemory, "");
  InsertTestWalWriter writer_a, writer_b;
  SnapshotGuard observer(db.graph_snapshot_store());
  auto& view = observer.get().mutable_view();
  auto person = view.schema().get_vertex_label_id("person");
  auto* age = observer.get()
                  .mutable_graph()
                  ->get_vertex_table(person)
                  .get_table()
                  .get_column_by_id(1);
  const auto capacity = age->size();
  const auto base = view.LidNum(person);
  MvccInsertTransaction a(SnapshotGuard(db.graph_snapshot_store()), alloc_a,
                          writer_a, vm, vm.acquire_insert_timestamp());
  MvccInsertTransaction b(SnapshotGuard(db.graph_snapshot_store()), alloc_b,
                          writer_b, vm, vm.acquire_insert_timestamp());
  vid_t vid;
  for (auto* txn : {&a, &b}) {
    ASSERT_TRUE(txn->AddVertex(person, Value::INT64(100),
                               {Value::STRING("duplicate"), Value::INT64(20)},
                               vid));
  }
  std::atomic<bool> b_looked_up{false}, a_done{false};
  bool a_committed = false, b_committed = false;
  // Fail the test instead of hanging CI if a commit fails before its hook.
  auto wait_for = [](const std::atomic<bool>& flag) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (!flag.load()) {
      if (std::chrono::steady_clock::now() >= deadline) {
        throw std::runtime_error(
            "Timed out waiting for vertex insert test hook");
      }
      std::this_thread::yield();
    }
  };
  std::thread first([&] {
    SetBeforeVertexIndexInsertForTest([&] { wait_for(b_looked_up); });
    a_committed = a.Commit();
    a_done.store(true);
  });
  std::thread second([&] {
    SetBeforeVertexIndexInsertForTest([&] {
      b_looked_up.store(true);
      wait_for(a_done);
      // B has already observed K as absent. Its distinct VID is acquired,
      // then the property write fails; PK lookup still returns A's VID.
      age->resize(base + 1);
    });
    b_committed = b.Commit();
  });
  first.join();
  second.join();
  age->resize(capacity);
  ASSERT_TRUE(a_committed);
  EXPECT_FALSE(b_committed);
  EXPECT_EQ(writer_b.calls, 0u);
  EXPECT_EQ(view.LidNum(person), base + 2);
  EXPECT_TRUE(view.get_lid(person, Value::INT64(100), vid, MAX_TIMESTAMP));
  EXPECT_EQ(vid, base);
  EXPECT_EQ(view.VertexNum(person), 3u);
  size_t visible = 0;
  for (auto ignored : view.GetVertexSet(person, MAX_TIMESTAMP)) {
    (void) ignored;
    ++visible;
  }
  EXPECT_EQ(visible, 3u);
  b.Abort();  // Repeated undo must preserve A.
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
  MvccInsertTransaction txn(SnapshotGuard(db.graph_snapshot_store()), alloc,
                            writer, vm, vm.acquire_insert_timestamp());
  vid_t src, dst;
  ASSERT_TRUE(txn.AddVertex(person, Value::INT64(3),
                            {Value::STRING("partial"), Value::INT64(20)}, src));
  ASSERT_TRUE(txn.GetVertexIndex(software, Value::INT64(1), dst));
  const void* prop = nullptr;
  ASSERT_TRUE(txn.AddEdge(person, src, software, dst, created,
                          {Value::DOUBLE(0.2), Value::INT64(2026)}, prop));
  SetBeforeInsertApplyForTest([&] { since->resize(table.PropTableSize()); });
  EXPECT_FALSE(txn.Commit());
  EXPECT_FALSE(txn.RequiresCapacityRetry());
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
        const void* prop = nullptr;
        ASSERT_TRUE(txn.AddEdge(person, src, software, dst, created,
                                {Value::DOUBLE(0.2), Value::INT64(2026)},
                                prop));
        SetBeforeInsertApplyForTest(
            [&] { since->resize(table.PropTableSize()); });
        EXPECT_FALSE(txn.Commit());
        EXPECT_FALSE(txn.RequiresCapacityRetry());
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

// A deterministic overlap: neither reservation has applied any rows yet.
TEST_F(MvccInsertTransactionTest, CapacityReservationsPreventOverbooking) {
  neug::NeugDB db;
  db.Open(db_dir);
  auto& graph = const_cast<neug::PropertyGraph&>(db.graph());
  auto label = graph.schema().get_vertex_label_id("person");
  auto& table = graph.get_vertex_table(label);
  ASSERT_GT(table.Capacity(), table.Size() + 2);
  neug::InsertCapacityRequest large, small, growth;
  for (size_t i = 0; i < table.Capacity() - table.Size() - 1; ++i) {
    large.AddVertex("person", neug::Value::INT64(i + 100),
                    {neug::Value::STRING(""), neug::Value::INT64(0)});
  }
  for (size_t i = 0; i < 2; ++i) {
    small.AddVertex("person", neug::Value::INT64(i + 10000),
                    {neug::Value::STRING(""), neug::Value::INT64(0)});
  }
  neug::InsertCapacityReservations reservations;
  EXPECT_TRUE(reservations.TryReserve(graph, large, growth));
  EXPECT_FALSE(reservations.TryReserve(graph, small, growth));
  reservations.Release(large);
  EXPECT_TRUE(reservations.TryReserve(graph, small, growth));
  reservations.Release(small);
  EXPECT_TRUE(reservations.TryReserve(graph, large, growth));
  reservations.Release(large);
  db.Close();
}

// Capacity rejection happens before any table is resized or reservation stored.
TEST_F(MvccInsertTransactionTest, RejectVectorGrowthBeforeMutation) {
  neug::NeugDB db;
  db.Open(db_dir);
  auto conn = db.Connect();
  ASSERT_TRUE(conn->Query(
      "CREATE NODE TABLE vector_item(id INT64 PRIMARY KEY, v FLOAT[2]);"));
  auto& graph = const_cast<neug::PropertyGraph&>(db.graph());
  auto& table =
      graph.get_vertex_table(graph.schema().get_vertex_label_id("vector_item"));
  auto* array =
      dynamic_cast<neug::ArrayColumn*>(table.get_table().get_column_by_id(0));
  ASSERT_NE(array, nullptr);
  const auto type = array->array_type();
  auto value =
      neug::Value::ARRAY(type, {neug::Value::FLOAT(1), neug::Value::FLOAT(2)});
  auto accessor = std::make_unique<neug::DefaultIndexIDAccessor>();
  accessor->Open(graph.checkpoint(), neug::ModuleDescriptor{},
                 neug::MemoryLevel::kInMemory);
  auto vector = std::make_unique<neug::VecColumn>(
      array->shared_buffer<float>(), std::move(accessor), type, array->size(),
      value, graph.checkpoint(), neug::MemoryLevel::kInMemory);
  const auto mapping_capacity = vector->size();
  const auto payload_capacity = vector->buffer_size();
  table.SetColumn(0, std::move(vector));
  const auto capacity = table.Capacity();
  neug::InsertCapacityRequest request, growth;
  for (size_t i = 0; i <= std::max(capacity, mapping_capacity); ++i)
    request.AddVertex("vector_item", neug::Value::INT64(i), {value});
  neug::InsertCapacityReservations reservations;
  EXPECT_THROW(reservations.TryReserve(graph, request, growth), std::exception);
  EXPECT_THROW(request.Grow(graph), std::exception);
  EXPECT_EQ(table.Capacity(), capacity);
  EXPECT_EQ(table.Size(), 0);
  auto* vec =
      dynamic_cast<neug::VecColumn*>(table.get_table().get_column_by_id(0));
  ASSERT_NE(vec, nullptr);
  EXPECT_EQ(vec->size(), mapping_capacity);
  EXPECT_EQ(vec->buffer_size(), payload_capacity);
  EXPECT_EQ(vec->get_offset_accessor()->GetNextIndexID(), 0);
  neug::InsertCapacityRequest ordinary;
  ordinary.AddVertex(
      "person", neug::Value::INT64(100),
      {neug::Value::STRING("after rejection"), neug::Value::INT64(0)});
  EXPECT_TRUE(reservations.TryReserve(graph, ordinary, growth));
  reservations.Release(ordinary);
  conn.reset();
  db.Close();
}

TEST_F(MvccInsertTransactionTest,
       AtomicGrowthPreservesSourceOnEveryAllocationFailure) {
  neug::NeugDB db;
  db.Open(db_dir);
  auto& graph = const_cast<neug::PropertyGraph&>(db.graph());
  const auto label = graph.schema().get_vertex_label_id("person");
  auto& table = graph.get_vertex_table(label);
  const auto capacity = table.Capacity();
  const auto size = table.Size();
  const auto alice = table.get_indexer().get_index(neug::Value::INT64(1));
  const auto original_name = table.get_table()
                                 .get_column_by_id(0)
                                 ->get_any(alice)
                                 .GetValue<std::string>();
  neug::InsertCapacityRequest request;
  for (size_t i = 0; i <= capacity; ++i)
    request.AddVertex(
        "person", neug::Value::INT64(100 + i),
        {neug::Value::STRING(std::string(200, 'x')), neug::Value::INT64(i)});
  bool succeeded = false;
  int failures = 0;
  for (int allocation = 0; allocation < 64; ++allocation) {
    auto candidate = graph.Clone();
    neug::SetResizeFailureCountdownForTest(allocation);
    try {
      request.Grow(*candidate);
      succeeded = true;
    } catch (const std::bad_alloc&) { ++failures; } catch (...) {
      neug::SetResizeFailureCountdownForTest(-1);
      throw;
    }
    neug::SetResizeFailureCountdownForTest(-1);
    EXPECT_EQ(table.Capacity(), capacity);
    EXPECT_EQ(table.Size(), size);
    EXPECT_EQ(table.get_indexer().get_index(neug::Value::INT64(1)), alice);
    EXPECT_EQ(table.get_table()
                  .get_column_by_id(0)
                  ->get_any(alice)
                  .GetValue<std::string>(),
              original_name);
    if (succeeded) {
      EXPECT_TRUE(request.Fits(*candidate));
      break;
    }
  }
  EXPECT_TRUE(succeeded);
  EXPECT_GE(failures,
            5);  // Includes columns, primary hash buckets and CSR directories.
  db.Close();
}

TEST_F(MvccInsertTransactionTest, NestedCapacityWithoutVertexGrowth) {
  neug::NeugDB db;
  db.Open(db_dir);
  auto conn = db.Connect();
  ASSERT_TRUE(conn->Query(
      "CREATE NODE TABLE nested_item(id INT64 PRIMARY KEY, values STRING[]);"));
  auto& graph = const_cast<neug::PropertyGraph&>(db.graph());
  const auto label = graph.schema().get_vertex_label_id("nested_item");
  const auto capacity = graph.get_vertex_table(label).Capacity();
  ASSERT_GT(capacity, 2);
  std::vector<neug::Value> strings(200,
                                   neug::Value::STRING(std::string(200, 'x')));
  auto value = neug::Value::LIST(neug::DataType(neug::DataTypeId::kVarchar),
                                 std::move(strings));
  neug::InsertCapacityRequest request;
  request.AddVertex("nested_item", neug::Value::INT64(1), {value});
  EXPECT_FALSE(request.Fits(graph));
  bool succeeded = false;
  int failures = 0;
  for (int allocation = 0; allocation < 16; ++allocation) {
    auto candidate = graph.Clone();
    neug::SetResizeFailureCountdownForTest(allocation);
    try {
      request.Grow(*candidate);
      succeeded = true;
    } catch (const std::bad_alloc&) { ++failures; } catch (...) {
      neug::SetResizeFailureCountdownForTest(-1);
      throw;
    }
    neug::SetResizeFailureCountdownForTest(-1);
    EXPECT_FALSE(request.Fits(graph));
    EXPECT_EQ(graph.get_vertex_table(label).Capacity(), capacity);
    if (succeeded) {
      EXPECT_TRUE(request.Fits(*candidate));
      EXPECT_EQ(candidate->get_vertex_table(label).Capacity(), capacity);
      // Once every nested buffer fits, growth must perform no allocations.
      neug::SetResizeFailureCountdownForTest(0);
      EXPECT_NO_THROW(request.Grow(*candidate));
      neug::SetResizeFailureCountdownForTest(-1);
      // Each reservation fits alone; their string payloads exceed the shared
      // capacity when combined. Releasing restores the complete nested budget.
      neug::InsertCapacityReservations reservations;
      neug::InsertCapacityRequest growth;
      EXPECT_TRUE(reservations.TryReserve(*candidate, request, growth));
      EXPECT_FALSE(reservations.TryReserve(*candidate, request, growth));
      reservations.Release(request);
      EXPECT_TRUE(reservations.TryReserve(*candidate, request, growth));
      reservations.Release(request);
      break;
    }
  }
  EXPECT_TRUE(succeeded);
  EXPECT_GE(failures, 2);
  conn.reset();
  db.Close();
}

TEST_F(MvccInsertTransactionTest, StringPayloadGrowthIsGeometric) {
  neug::NeugDB db;
  db.Open(db_dir);
  auto& graph = const_cast<neug::PropertyGraph&>(db.graph());
  auto& table =
      graph.get_vertex_table(graph.schema().get_vertex_label_id("person"));
  auto* column =
      dynamic_cast<neug::StringColumn*>(table.get_table().get_column_by_id(0));
  ASSERT_NE(column, nullptr);
  const auto rows = column->size();
  const auto vertex_capacity = table.Capacity();
  column->resize(rows, size_t{65536});
  const std::string payload(200, 'x');
  neug::InsertCapacityRequest request;
  request.AddVertex("person", neug::Value::INT64(100),
                    {neug::Value::STRING(payload), neug::Value::INT64(0)});
  for (int iteration = 0; iteration < 5; ++iteration) {
    while (column->available_space() >= payload.size())
      column->set_value(0, payload);
    const auto old_capacity = column->data_capacity();
    const auto required =
        old_capacity - column->available_space() + payload.size();
    EXPECT_FALSE(request.Fits(graph));
    request.Grow(graph);
    EXPECT_TRUE(request.Fits(graph));
    EXPECT_EQ(column->data_capacity(),
              std::max(size_t{4096}, required + required / 4));
    EXPECT_EQ(column->size(), rows);
    EXPECT_EQ(table.Capacity(), vertex_capacity);
    // A fitting request must not allocate again.
    neug::SetResizeFailureCountdownForTest(0);
    EXPECT_NO_THROW(request.Grow(graph));
    neug::SetResizeFailureCountdownForTest(-1);
  }
  db.Close();
}

TEST_F(MvccInsertTransactionTest, RowGrowthUsesCompleteBatchRequirement) {
  neug::NeugDB db;
  db.Open(db_dir);
  auto& graph = const_cast<neug::PropertyGraph&>(db.graph());
  const auto label = graph.schema().get_vertex_label_id("person");
  const auto& original = graph.get_vertex_table(label);
  for (const auto required :
       {original.Capacity() + 1, original.Capacity() * 3}) {
    neug::InsertCapacityRequest request;
    for (size_t i = original.Size(); i < required; ++i) {
      request.AddVertex("person", neug::Value::INT64(i + 100),
                        {neug::Value::STRING(""), neug::Value::INT64(0)});
    }
    auto candidate = graph.Clone();
    request.Grow(*candidate);
    EXPECT_TRUE(request.Fits(*candidate));
    EXPECT_EQ(candidate->get_vertex_table(label).Capacity(),
              std::max(size_t{4096}, required + required / 4));
  }
  db.Close();
}

TEST_F(MvccInsertTransactionTest, CapacityRetryLimitLeavesSlotUsable) {
  neug::NeugDB db;
  db.Open(db_dir);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  auto slot = svc->AcquireExecutionSlot();
  const auto label = db.graph().schema().get_vertex_label_id("person");
  const auto before = db.graph().get_vertex_table(label).Size();
  struct ResetInjection {
    ~ResetInjection() { neug::SetInsertReservationFailuresForTest(0); }
  } reset;
  neug::SetInsertReservationFailuresForTest(9);
  const std::string query =
      R"json({"query":"CREATE (:person {id: 100, name: ')json" +
      std::string(1 << 20, 'x') +
      R"json(', age: 20})","access_mode":"insert","parameters":{}})json";
  auto failed = slot->ExecuteTransactionalRequest(query);
  ASSERT_FALSE(failed);
  EXPECT_EQ(failed.error().error_message(),
            "Insert capacity retry limit exceeded");
  EXPECT_EQ(db.graph().get_vertex_table(label).Size(), before);
  neug::SetInsertReservationFailuresForTest(0);
  auto success = slot->ExecuteTransactionalRequest(query);
  ASSERT_TRUE(success) << success.error().error_message();
  EXPECT_EQ(db.graph().get_vertex_table(label).Size(), before + 1);
  EXPECT_TRUE(slot->ExecuteTransactionalRequest(
      R"json({"query":"MATCH (n:person) RETURN count(n)","access_mode":"read","parameters":{}})json"));
}

TEST_F(MvccInsertTransactionTest, RetryAfterCompetingInsertsConsumeGrowth) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir, 2);
  config.checkpoint_on_close = false;
  ASSERT_TRUE(db.Open(config));
  auto conn = db.Connect();
  ASSERT_TRUE(conn->Query(
      "CREATE NODE TABLE RetryItem(id STRING, value STRING, PRIMARY KEY(id))"));
  conn->Close();
  db.PrepareForServing();
  {
    neug::NeugDBService svc(db);
    auto slot = svc.AcquireExecutionSlot();
    auto competitor = svc.AcquireExecutionSlot();
    auto batch = [](int first, int last) {
      std::string ids = "[";
      for (int i = first; i <= last; ++i) {
        if (i != first)
          ids += ",";
        ids += "\"" + std::to_string(i) + std::string(100, 'k') + "\"";
      }
      ids += "]";
      return R"json({"query":"UNWIND CAST($ids, 'STRING[]') AS id CREATE (:RetryItem {id:id,value:$v})","access_mode":"insert","parameters":{"ids":)json" +
             ids + R"json(,"v":")json" + std::string(200, 'x') + "\"}}";
    };
    auto seeded = slot->ExecuteTransactionalRequest(batch(10000, 12999));
    ASSERT_TRUE(seeded) << seeded.error().ToString();

    struct ResetHook {
      ~ResetHook() { neug::SetInsertRetryHookForTest({}); }
    } reset;
    size_t retries = 0;
    neug::SetInsertRetryHookForTest([&](bool retry) {
      if (!retry)
        return;
      ++retries;
      // Run a competing request after maintenance releases admission, before
      // the original request can reserve the newly available row/key/payload
      // space. No timing assumptions or injected reservation failures.
      if (retries <= 2) {
        // A separate worker also keeps its test-hook TLS independent.
        std::thread writer([&] {
          auto inserted = competitor->ExecuteTransactionalRequest(
              retries == 1 ? batch(14500, 15999) : batch(16000, 18999));
          EXPECT_TRUE(inserted) << inserted.error().ToString();
        });
        writer.join();
      }
    });
    auto inserted = slot->ExecuteTransactionalRequest(batch(13000, 14499));
    ASSERT_TRUE(inserted) << inserted.error().ToString();
    EXPECT_EQ(retries, 3);
    neug::SetInsertRetryHookForTest({});
    auto count = slot->ExecuteTransactionalRequest(
        R"json({"query":"MATCH (n:RetryItem) RETURN count(n)","access_mode":"read","parameters":{}})json");
    ASSERT_TRUE(count);
    EXPECT_EQ(neug::QueryResult::From(count.value()).GetInt64(0), 9000);
  }
  db.Close();
  // All four successful batches must survive WAL recovery exactly once.
  ASSERT_TRUE(db.Open(config));
  auto recovered = db.Connect();
  auto count = recovered->Query("MATCH (n:RetryItem) RETURN count(n)");
  ASSERT_TRUE(count);
  EXPECT_EQ(count.value().GetInt64(0), 9000);
  auto value = recovered->Query("MATCH (n:RetryItem) WHERE n.id = '13000" +
                                std::string(100, 'k') + "' RETURN n.value");
  ASSERT_TRUE(value);
  EXPECT_EQ(value.value().GetString(0), std::string(200, 'x'));
}

TEST_F(MvccInsertTransactionTest, RetryWhileAppliedRowsRemainReserved) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir, 2);
  config.checkpoint_on_close = false;
  ASSERT_TRUE(db.Open(config));
  auto conn = db.Connect();
  ASSERT_TRUE(conn->Query(
      "CREATE NODE TABLE ReservationWindow(id INT64, PRIMARY KEY(id))"));
  conn->Close();
  db.PrepareForServing();
  neug::NeugDBService svc(db);
  auto slot = svc.AcquireExecutionSlot();
  auto competitor = svc.AcquireExecutionSlot();
  auto batch = [](int first, int count) {
    std::string ids = "[";
    for (int i = 0; i < count; ++i) {
      if (i)
        ids += ",";
      ids += std::to_string(first + i);
    }
    return R"json({"query":"UNWIND CAST($ids, 'INT64[]') AS id CREATE (:ReservationWindow {id:id})","access_mode":"insert","parameters":{"ids":)json" +
           ids + "]}}";
  };
  auto seeded = slot->ExecuteTransactionalRequest(batch(1, 4000));
  ASSERT_TRUE(seeded) << seeded.error().ToString();
  std::promise<void> written, release;
  auto applied = written.get_future();
  auto released = release.get_future();
  std::thread writer;
  auto finish_writer = [&] {
    if (writer.joinable()) {
      release.set_value();
      writer.join();
    }
  };
  struct ResetHook {
    std::function<void()> finish;
    ~ResetHook() {
      finish();
      neug::SetInsertRetryHookForTest({});
    }
  } reset{finish_writer};
  size_t retries = 0;
  neug::SetInsertRetryHookForTest([&](bool retry) {
    if (!retry) {
      // A has failed reservation while B's applied rows were double-counted.
      // Let B release its reservation before A enters exclusive maintenance.
      finish_writer();
      return;
    }
    if (++retries != 1)
      return;
    writer = std::thread([&] {
      neug::SetInsertPostIngestHookForTest([&] {
        written.set_value();
        released.wait();
      });
      auto inserted =
          competitor->ExecuteTransactionalRequest(batch(10001, 600));
      neug::SetInsertPostIngestHookForTest({});
      EXPECT_TRUE(inserted) << inserted.error().ToString();
    });
    EXPECT_EQ(applied.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
  });
  auto inserted = slot->ExecuteTransactionalRequest(batch(4001, 100));
  ASSERT_TRUE(inserted) << inserted.error().ToString();
  EXPECT_EQ(retries, 2);
  neug::SetInsertRetryHookForTest({});
  auto count = slot->ExecuteTransactionalRequest(
      R"json({"query":"MATCH (n:ReservationWindow) RETURN count(n)","access_mode":"read","parameters":{}})json");
  ASSERT_TRUE(count);
  EXPECT_EQ(neug::QueryResult::From(count.value()).GetInt64(0), 4700);
}

class InsertCapacityBudgetTest : public MvccInsertTransactionTest,
                                 public ::testing::WithParamInterface<bool> {};

TEST_P(InsertCapacityBudgetTest, CompetingGrowthStopsAtTotalRetryLimit) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir, 2);
  config.checkpoint_on_close = false;
  ASSERT_TRUE(db.Open(config));
  auto conn = db.Connect();
  ASSERT_TRUE(conn->Query(
      "CREATE NODE TABLE RetryLimitItem(id INT64, PRIMARY KEY(id))"));
  conn->Close();
  db.PrepareForServing();
  {
    neug::NeugDBService svc(db);
    auto slot = svc.AcquireExecutionSlot();
    auto competitor = svc.AcquireExecutionSlot();
    auto batch = [](int first, int count) {
      std::string ids = "[";
      for (int i = 0; i < count; ++i) {
        if (i)
          ids += ",";
        ids += std::to_string(first + i);
      }
      return R"json({"query":"UNWIND CAST($ids, 'INT64[]') AS id CREATE (:RetryLimitItem {id:id})","access_mode":"insert","parameters":{"ids":)json" +
             ids + "]}}";
    };
    auto seeded = slot->ExecuteTransactionalRequest(batch(10000, 3000));
    ASSERT_TRUE(seeded) << seeded.error().ToString();
    const auto label = db.schema().get_vertex_label_id("RetryLimitItem");
    struct ResetHook {
      ~ResetHook() { neug::SetInsertRetryHookForTest({}); }
    } reset;
    size_t retries = 0;
    int next_id = 20000;
    neug::SetInsertRetryHookForTest([&](bool retry) {
      if (!retry)
        return;
      ++retries;
      // Stop the competing workload after the expected limit so a broken,
      // unbounded implementation fails this test instead of hanging it.
      if (retries > 8) {
        ADD_FAILURE() << "Insert exceeded the total retry budget";
        return;
      }
      if (!GetParam() && retries == 8)
        return;
      const auto& table = db.graph().get_vertex_table(label);
      const auto available = table.Capacity() - table.Size();
      std::thread writer([&] {
        auto inserted = competitor->ExecuteTransactionalRequest(
            batch(next_id, static_cast<int>(available)));
        EXPECT_TRUE(inserted) << inserted.error().ToString();
      });
      writer.join();
      next_id += static_cast<int>(available);
    });
    auto result = slot->ExecuteTransactionalRequest(batch(13000, 1500));
    if (GetParam()) {
      ASSERT_FALSE(result);
      EXPECT_EQ(result.error().error_message(),
                "Insert capacity retry limit exceeded");
    } else {
      ASSERT_TRUE(result) << result.error().ToString();
    }
    EXPECT_EQ(retries, 8);
    neug::SetInsertRetryHookForTest({});
    auto count = slot->ExecuteTransactionalRequest(
        R"json({"query":"MATCH (n:RetryLimitItem) WHERE n.id >= 13000 AND n.id < 14500 RETURN count(n)","access_mode":"read","parameters":{}})json");
    ASSERT_TRUE(count);
    EXPECT_EQ(neug::QueryResult::From(count.value()).GetInt64(0),
              GetParam() ? 0 : 1500);
    // The failed batch did not publish any rows, and its slot remains usable.
    if (GetParam()) {
      auto success = slot->ExecuteTransactionalRequest(batch(13000, 1500));
      ASSERT_TRUE(success) << success.error().ToString();
    }
    EXPECT_EQ(db.graph().get_vertex_table(label).Size(),
              static_cast<size_t>(3000 + next_id - 20000 + 1500));
  }
}

INSTANTIATE_TEST_SUITE_P(InsertCapacity, InsertCapacityBudgetTest,
                         ::testing::Values(false, true),
                         [](const ::testing::TestParamInfo<bool>& info) {
                           return info.param ? "RetryBudgetExhausted"
                                             : "SuccessOnLastRetry";
                         });

TEST_F(MvccInsertTransactionTest, EdgeCapacityGrowthCoversRowsAndPayload) {
  neug::NeugDB db;
  db.Open(db_dir);
  auto conn = db.Connect();
  ASSERT_TRUE(
      conn->Query("CREATE REL TABLE payload(FROM person TO person, seq "
                  "INT64, note STRING)"));
  auto& graph = const_cast<neug::PropertyGraph&>(db.graph());
  auto label = graph.schema().get_vertex_label_id("person");
  auto edge = graph.schema().get_edge_label_id("payload");
  auto& table = graph.get_edge_table(label, label, edge);
  ASSERT_FALSE(table.meta()->is_bundled());
  neug::InsertCapacityRequest rows;
  for (size_t i = 0; i <= table.Capacity(); ++i)
    rows.AddEdge("person", "person", "payload",
                 {neug::Value::INT64(i), neug::Value::STRING("note")});
  ASSERT_FALSE(rows.Fits(graph));
  auto candidate = graph.Clone();
  rows.Grow(*candidate);
  EXPECT_TRUE(rows.Fits(*candidate));
  EXPECT_FALSE(rows.Fits(graph));
  auto& grown = candidate->get_edge_table(label, label, edge);
  const auto capacity = grown.Capacity();
  neug::InsertCapacityRequest payload;
  payload.AddEdge(
      "person", "person", "payload",
      {neug::Value::INT64(0), neug::Value::STRING(std::string(1 << 20, 'x'))});
  ASSERT_FALSE(payload.Fits(*candidate));
  payload.Grow(*candidate);
  EXPECT_TRUE(payload.Fits(*candidate));
  EXPECT_EQ(grown.Capacity(), capacity);
}

class InsertRetryFailureTest : public MvccInsertTransactionTest,
                               public ::testing::WithParamInterface<int> {};

TEST_P(InsertRetryFailureTest, FailureLeavesSlotUsable) {
  neug::NeugDB db;
  db.Open(db_dir);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  auto slot = svc->AcquireExecutionSlot();
  auto ddl_slot = svc->AcquireExecutionSlot();
  const auto label = db.graph().schema().get_vertex_label_id("person");
  const auto before = db.graph().get_vertex_table(label).Size();
  struct ResetInjection {
    ~ResetInjection() {
      neug::SetInsertRetryHookForTest({});
      neug::SetInsertReservationFailuresForTest(0);
    }
  } reset;
  bool schema_changed = false;
  const int scenario = GetParam();
  neug::SetInsertReservationFailuresForTest(1);
  neug::SetInsertRetryHookForTest(
      [&](bool second_attempt) {
        if (scenario == 2 || second_attempt != (scenario == 1))
          return;
        auto changed = ddl_slot->ExecuteTransactionalRequest(
            R"json({"query":"CREATE NODE TABLE AddedDuringRetry(id INT64, PRIMARY KEY(id))","access_mode":"schema","parameters":{}})json");
        EXPECT_TRUE(changed) << changed.error().error_message();
        schema_changed = bool(changed);
      },
      scenario == 2);
  const std::string query =
      R"json({"query":"CREATE (:person {id: 100, name: 'retry', age: 20})","access_mode":"insert","parameters":{}})json";
  auto failed = slot->ExecuteTransactionalRequest(query);
  ASSERT_FALSE(failed);
  EXPECT_EQ(failed.error().error_message(),
            scenario == 2 ? "Capacity maintenance is unavailable"
                          : "Schema changed during insert retry");
  EXPECT_EQ(schema_changed, scenario != 2);
  EXPECT_EQ(db.graph().get_vertex_table(label).Size(), before);
  neug::SetInsertRetryHookForTest({});
  neug::SetInsertReservationFailuresForTest(0);
  auto success = slot->ExecuteTransactionalRequest(query);
  ASSERT_TRUE(success) << success.error().error_message();
  EXPECT_EQ(db.graph().get_vertex_table(label).Size(), before + 1);
  EXPECT_TRUE(slot->ExecuteTransactionalRequest(
      R"json({"query":"MATCH (n:person) RETURN count(n)","access_mode":"read","parameters":{}})json"));
}

INSTANTIATE_TEST_SUITE_P(InsertCapacity, InsertRetryFailureTest,
                         ::testing::Values(0, 1, 2),
                         [](const ::testing::TestParamInfo<int>& info) {
                           return info.param == 0
                                      ? "SchemaChangedBeforeMaintenance"
                                      : info.param == 1
                                            ? "SchemaChangedBeforeRetry"
                                            : "MaintenanceUnavailable";
                         });
