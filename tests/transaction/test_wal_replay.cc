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
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/version_manager.h"

#include <unistd.h>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>

#include "gtest/gtest.h"

namespace {

using neug::Value;

std::string make_test_dir() {
  const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
  const auto dir_name = std::string("neug_wal_replay_test_") +
                        std::to_string(::getpid()) + "_" +
                        info->test_suite_name() + "_" + info->name();
  return (std::filesystem::temp_directory_path() / dir_name).string();
}

neug::NeugDBConfig make_config(const std::string& db_dir) {
  neug::NeugDBConfig config(db_dir, 1);
  config.memory_level = neug::MemoryLevel::kInMemory;
  config.checkpoint_on_close = false;
  config.checkpoint_on_recovery = false;
  return config;
}

void assert_query_ok(neug::Connection& conn, const std::string& query) {
  auto result = conn.Query(query);
  ASSERT_TRUE(result) << query << ": " << result.error().ToString();
}

void create_checkpointed_base_graph(const std::string& db_dir) {
  auto config = make_config(db_dir);
  config.checkpoint_on_close = true;

  neug::NeugDB db;
  ASSERT_TRUE(db.Open(config));
  auto conn = db.Connect();
  for (const auto* query : {
           "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));",
           "CREATE REL TABLE knows(FROM person TO person, since INT64);",
           "CREATE (:person {id: 1, name: 'seed'});",
       }) {
    assert_query_ok(*conn, query);
  }
  db.Close();
}

void create_person_schema(neug::NeugDB& db) {
  auto conn = db.Connect();
  assert_query_ok(
      *conn,
      "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));");
}

bool replayed_graph_matches(neug::NeugDB& db) {
  neug::SnapshotGuard guard(db.graph_snapshot_store());
  neug::StorageReadInterface graph(guard.get().view(), neug::MAX_TIMESTAMP);
  const auto person_label = graph.schema().get_vertex_label_id("person");
  const auto knows_label = graph.schema().get_edge_label_id("knows");

  size_t person_count = 0;
  graph.GetVertexSet(person_label).foreach_vertex([&](neug::vid_t) {
    ++person_count;
  });
  if (person_count != 2) {
    return false;
  }

  neug::vid_t src_vid = 0;
  neug::vid_t dst_vid = 0;
  if (!graph.GetVertexIndex(person_label, Value::INT64(1), src_vid) ||
      !graph.GetVertexIndex(person_label, Value::INT64(2), dst_vid)) {
    return false;
  }

  auto edges = graph.GetGenericOutgoingGraphView(person_label, person_label,
                                                 knows_label);
  auto since =
      graph.GetEdgeDataAccessor(person_label, person_label, knows_label, 0);
  size_t matching_edges = 0;
  auto edge_iter = edges.get_edges(src_vid);
  for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
    if (it.get_vertex() == dst_vid &&
        since.get_typed_data<int64_t>(it) == 2026) {
      ++matching_edges;
    }
  }
  return matching_edges == 1;
}

neug::timestamp_t insert_person_and_return_ts(neug::NeugDBService& service,
                                              int64_t id,
                                              const std::string& name) {
  auto sess = service.AcquireSession();
  auto txn = sess->GetInsertTransaction();
  const auto ts = txn.timestamp();
  neug::StorageTPInsertInterface interface(txn);
  const auto person_label = txn.schema().get_vertex_label_id("person");
  neug::vid_t vid = 0;
  EXPECT_TRUE(interface.AddVertex(person_label, Value::INT64(id),
                                  {Value::STRING(name)}, vid));
  EXPECT_TRUE(txn.Commit());
  return ts;
}

void insert_person(neug::NeugDBService& service, int64_t id,
                   const std::string& name) {
  (void) insert_person_and_return_ts(service, id, name);
}

void compact(neug::NeugDBService& service) {
  auto sess = service.AcquireSession();
  auto txn = sess->GetCompactTransaction();
  ASSERT_TRUE(txn.Commit());
}

void insert_knows_edge(neug::NeugDBService& service, int64_t src_id,
                       int64_t dst_id, int64_t since) {
  auto sess = service.AcquireSession();
  auto txn = sess->GetInsertTransaction();
  neug::StorageTPInsertInterface interface(txn);
  const auto person_label = txn.schema().get_vertex_label_id("person");
  const auto knows_label = txn.schema().get_edge_label_id("knows");

  neug::vid_t src_vid = 0;
  neug::vid_t dst_vid = 0;
  ASSERT_TRUE(txn.GetVertexIndex(person_label, Value::INT64(src_id), src_vid));
  ASSERT_TRUE(txn.GetVertexIndex(person_label, Value::INT64(dst_id), dst_vid));

  const void* prop = nullptr;
  ASSERT_TRUE(interface.AddEdge(person_label, src_vid, person_label, dst_vid,
                                knows_label, {Value::INT64(since)}, prop));
  ASSERT_TRUE(txn.Commit());
}

size_t read_person_count(neug::NeugDBService& service) {
  auto sess = service.AcquireSession();
  auto txn = sess->GetReadTransaction();
  neug::StorageReadInterface graph(txn.view(), txn.timestamp());
  const auto person_label = graph.schema().get_vertex_label_id("person");
  size_t count = 0;
  graph.GetVertexSet(person_label).foreach_vertex([&](neug::vid_t) {
    ++count;
  });
  EXPECT_TRUE(txn.Commit());
  return count;
}

bool read_has_person(neug::NeugDBService& service, int64_t id) {
  auto sess = service.AcquireSession();
  auto txn = sess->GetReadTransaction();
  neug::StorageReadInterface graph(txn.view(), txn.timestamp());
  const auto person_label = graph.schema().get_vertex_label_id("person");
  neug::vid_t vid = 0;
  bool found = graph.GetVertexIndex(person_label, Value::INT64(id), vid);
  EXPECT_TRUE(txn.Commit());
  return found;
}

void create_wal_with_insert_compact_insert_collision(
    const std::string& db_dir) {
  neug::NeugDB db;
  ASSERT_TRUE(db.Open(make_config(db_dir)));
  {
    neug::NeugDBService service(db);
    insert_person(service, 2, "wal-dst");
    compact(service);
    insert_knows_edge(service, 1, 2, 2026);
  }
  db.Close();
}

int reopen_and_verify_replayed_graph(const std::string& db_dir) {
  try {
    neug::NeugDB db;
    if (!db.Open(make_config(db_dir))) {
      return 1;
    }

    if (!replayed_graph_matches(db)) {
      return 3;
    }

    db.Close();
    return 0;
  } catch (const std::exception& e) {
    std::cerr << e.what() << "\n";
    return 10;
  } catch (...) {
    std::cerr << "unknown exception\n";
    return 11;
  }
}

}  // namespace

class WalReplayTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_dir_ = make_test_dir();
    std::filesystem::remove_all(db_dir_);
    std::filesystem::create_directories(db_dir_);
  }

  void TearDown() override { std::filesystem::remove_all(db_dir_); }

  std::string db_dir_;
};

static void expect_compact_completes_timestamp_and_preserves_next_insert(
    bool commit) {
  neug::VersionManager version_manager;
  version_manager.init_ts(0, 1);

  const auto insert_ts = version_manager.acquire_insert_timestamp();
  version_manager.release_insert_timestamp(insert_ts);

  const auto compact_ts = version_manager.acquire_compact_timestamp();
  if (commit) {
    version_manager.release_compact_timestamp(compact_ts);
  } else {
    version_manager.revert_compact_timestamp(compact_ts);
  }

  const auto read_after_compact_ts = version_manager.acquire_read_timestamp();
  EXPECT_EQ(read_after_compact_ts, compact_ts)
      << "compaction timestamps must be marked complete so readers can advance";
  version_manager.release_read_timestamp();

  const auto next_insert_ts = version_manager.acquire_insert_timestamp();
  EXPECT_GT(next_insert_ts, compact_ts)
      << "insert timestamps must remain monotonic after compaction so WAL "
         "replay cannot collide with pre-compaction insert records";
  version_manager.release_insert_timestamp(next_insert_ts);

  const auto read_after_insert_ts = version_manager.acquire_read_timestamp();
  EXPECT_EQ(read_after_insert_ts, next_insert_ts)
      << "a compact timestamp gap must not block later insert visibility";
  version_manager.release_read_timestamp();
}

TEST(WalReplayVersionManagerTest,
     CommittedCompactCompletesTimestampAndDoesNotReusePriorInsertTimestamp) {
  expect_compact_completes_timestamp_and_preserves_next_insert(true);
}

TEST(WalReplayVersionManagerTest,
     RevertedCompactCompletesTimestampAndDoesNotReusePriorInsertTimestamp) {
  expect_compact_completes_timestamp_and_preserves_next_insert(false);
}

TEST_F(WalReplayTest, CloseCheckpointAlwaysResetsServiceTimeline) {
  {
    auto config = make_config(db_dir_);
    config.checkpoint_on_close = true;

    neug::NeugDB db;
    ASSERT_TRUE(db.Open(config));
    create_person_schema(db);
    {
      neug::NeugDBService service(db);
      EXPECT_EQ(insert_person_and_return_ts(service, 1, "old"), 1);
      EXPECT_EQ(read_person_count(service), 1);
    }
    db.Close();
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    {
      neug::NeugDBService service(db);
      EXPECT_TRUE(read_has_person(service, 1));
      EXPECT_EQ(insert_person_and_return_ts(service, 2, "new"), 1);
      EXPECT_EQ(read_person_count(service), 2);
    }
    db.Close();
  }
}

TEST_F(WalReplayTest, RecoveryWithoutCheckpointContinuesFromWalTimeline) {
  create_checkpointed_base_graph(db_dir_);

  neug::timestamp_t wal_ts = 0;
  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    {
      neug::NeugDBService service(db);
      wal_ts = insert_person_and_return_ts(service, 2, "wal");
    }
    db.Close();
  }

  {
    auto config = make_config(db_dir_);
    config.checkpoint_on_recovery = false;

    neug::NeugDB db;
    ASSERT_TRUE(db.Open(config));
    {
      neug::NeugDBService service(db);
      EXPECT_TRUE(read_has_person(service, 2));
      EXPECT_EQ(insert_person_and_return_ts(service, 3, "post-wal"),
                wal_ts + 1);
      EXPECT_EQ(read_person_count(service), 3);
    }
    db.Close();
  }
}

TEST_F(WalReplayTest, RecoveryCheckpointResetsServiceTimeline) {
  create_checkpointed_base_graph(db_dir_);

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    {
      neug::NeugDBService service(db);
      EXPECT_EQ(insert_person_and_return_ts(service, 2, "wal"), 1);
    }
    db.Close();
  }

  {
    auto config = make_config(db_dir_);
    config.checkpoint_on_recovery = true;

    neug::NeugDB db;
    ASSERT_TRUE(db.Open(config));
    {
      neug::NeugDBService service(db);
      EXPECT_TRUE(read_has_person(service, 2));
      EXPECT_EQ(insert_person_and_return_ts(service, 3, "post-recovery"), 1);
      EXPECT_EQ(read_person_count(service), 3);
    }
    db.Close();
  }
}

TEST_F(WalReplayTest, ReopenReplaysInsertWalAcrossCompactionInDependencyOrder) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  const auto db_dir = db_dir_;
  ASSERT_EXIT(
      {
        create_checkpointed_base_graph(db_dir);
        create_wal_with_insert_compact_insert_collision(db_dir);
        const int code = reopen_and_verify_replayed_graph(db_dir);
        std::exit(code);
      },
      ::testing::ExitedWithCode(0), ".*");
}
