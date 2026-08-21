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
#include "neug/main/checkpoint_coordinator.h"
#include "neug/neug.h"
#include "neug/server/neug_db_service.h"
#include "neug/storages/allocators.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/timestamp_lease.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/wal.h"

#ifndef _WIN32
#include <unistd.h>
#else
#include <process.h>
#endif
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "neug/transaction/wal/wal.h"
#include "unittest/utils.h"

namespace {

using neug::Value;

std::atomic<bool> in_place_mutation_waiting{false};

void ObserveInPlaceMutationWait(neug::RuntimeWaitAction) noexcept {
  in_place_mutation_waiting.store(true, std::memory_order_release);
  std::this_thread::yield();
}

std::string make_test_dir() {
  const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
  const auto dir_name = std::string("neug_wal_replay_test_") +
                        std::to_string(::getpid()) + "_" +
                        info->test_suite_name() + "_" + info->name();
  return (std::filesystem::temp_directory_path() / dir_name).string();
}

TEST(WalWriterTest, ReopensSameInstanceOnNewTimeline) {
  const auto test_dir = make_test_dir();
  const auto old_wal_dir =
      (std::filesystem::path(test_dir) / "checkpoint-0" / "wal").string();
  const auto new_wal_dir =
      (std::filesystem::path(test_dir) / "checkpoint-1" / "wal").string();
  constexpr uint32_t old_marker = 17;
  constexpr uint32_t new_marker = 29;

  {
    auto writer = neug::WalWriterFactory::CreateWalWriter(old_wal_dir, 0);
    auto* const identity = writer.get();
    writer->open(old_wal_dir);
    ASSERT_TRUE(writer->append(reinterpret_cast<const char*>(&old_marker),
                               sizeof(old_marker)));

    writer->open(new_wal_dir);
    EXPECT_EQ(writer.get(), identity);
    ASSERT_TRUE(writer->append(reinterpret_cast<const char*>(&new_marker),
                               sizeof(new_marker)));
    writer->close();
  }

  const auto read_marker = [](const std::string& wal_dir) {
    const auto begin = std::filesystem::directory_iterator(wal_dir);
    const auto end = std::filesystem::directory_iterator();
    EXPECT_NE(begin, end);
    std::ifstream wal_file(begin->path(), std::ios::binary);
    uint32_t marker = 0;
    wal_file.read(reinterpret_cast<char*>(&marker), sizeof(marker));
    return marker;
  };
  EXPECT_EQ(read_marker(old_wal_dir), old_marker);
  EXPECT_EQ(read_marker(new_wal_dir), new_marker);

  std::filesystem::remove_all(test_dir);
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
  conn->Close();
  db.Close();
}

void create_person_schema(neug::NeugDB& db) {
  auto conn = db.Connect();
  assert_query_ok(
      *conn,
      "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));");
  conn->Close();
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
  auto slot = service.AcquireExecutionSlot();
  auto txn = slot->GetInsertTransaction();
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
  auto slot = service.AcquireExecutionSlot();
  auto txn = slot->GetCompactTransaction();
  ASSERT_TRUE(txn.Commit());
}

void insert_knows_edge(neug::NeugDBService& service, int64_t src_id,
                       int64_t dst_id, int64_t since) {
  auto slot = service.AcquireExecutionSlot();
  auto txn = slot->GetInsertTransaction();
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
  auto slot = service.AcquireExecutionSlot();
  auto txn = slot->GetReadTransaction();
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
  auto slot = service.AcquireExecutionSlot();
  auto txn = slot->GetReadTransaction();
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
  version_manager.init_ts({0, 0}, 1);

  const auto insert_ts = version_manager.acquire_insert_timestamp();
  version_manager.release_insert_timestamp(insert_ts);

  const auto compact_ts = version_manager.acquire_compact_timestamp();
  if (commit) {
    version_manager.release_compact_timestamp(compact_ts);
  } else {
    version_manager.revert_compact_timestamp(compact_ts);
  }

  {
    auto read = version_manager.acquire_read_operation();
    EXPECT_EQ(read.published_view.visibility_ts, compact_ts)
        << "compaction timestamps must be marked complete so readers can "
           "advance";
  }

  const auto next_insert_ts = version_manager.acquire_insert_timestamp();
  EXPECT_GT(next_insert_ts, compact_ts)
      << "insert timestamps must remain monotonic after compaction so WAL "
         "replay cannot collide with pre-compaction insert records";
  version_manager.release_insert_timestamp(next_insert_ts);

  {
    auto read = version_manager.acquire_read_operation();
    EXPECT_EQ(read.published_view.visibility_ts, next_insert_ts)
        << "a compact timestamp gap must not block later insert visibility";
  }
}

TEST(WalReplayVersionManagerTest,
     CommittedCompactCompletesTimestampAndDoesNotReusePriorInsertTimestamp) {
  expect_compact_completes_timestamp_and_preserves_next_insert(true);
}

TEST(WalReplayVersionManagerTest,
     RevertedCompactCompletesTimestampAndDoesNotReusePriorInsertTimestamp) {
  expect_compact_completes_timestamp_and_preserves_next_insert(false);
}

TEST(WalReplayVersionManagerTest, ResetTimelineStartsFreshTimestampTimeline) {
  neug::VersionManager version_manager;
  version_manager.init_ts({40, 7}, 1);

  const auto old_insert_ts = version_manager.acquire_insert_timestamp();
  EXPECT_EQ(old_insert_ts, 41);
  version_manager.release_insert_timestamp(old_insert_ts);

  neug::UpdateTimestampLease update_lease(version_manager);
  EXPECT_EQ(update_lease.Timestamp(), 42);
  update_lease.MakeUpdateExclusive();
  update_lease.FinishAndResetTimeline();

  {
    auto read = version_manager.acquire_read_operation();
    EXPECT_EQ(read.published_view.visibility_ts, 0);
    EXPECT_EQ(read.published_view.snapshot_generation, 7);
  }

  const auto new_insert_ts = version_manager.acquire_insert_timestamp();
  EXPECT_EQ(new_insert_ts, 1);
  version_manager.release_insert_timestamp(new_insert_ts);

  {
    auto read = version_manager.acquire_read_operation();
    EXPECT_EQ(read.published_view.visibility_ts, new_insert_ts);
    EXPECT_EQ(read.published_view.snapshot_generation, 7);
  }
}

TEST(WalReplayVersionManagerTest, ResetTimelineAfterMakeUpdateExclusive) {
  neug::VersionManager version_manager;
  version_manager.init_ts({0, 0}, 1);

  neug::UpdateTimestampLease update_lease(version_manager);
  update_lease.MakeUpdateExclusive();
  update_lease.FinishAndResetTimeline();

  auto read = version_manager.acquire_read_operation();
  EXPECT_EQ(read.published_view.visibility_ts, 0);
}

TEST(WalReplayVersionManagerTest,
     UpdateLeaseReleaseCompletesWithoutResettingTimeline) {
  neug::VersionManager version_manager;
  version_manager.init_ts({40, 0}, 1);

  uint32_t update_ts = 0;
  {
    neug::UpdateTimestampLease original(version_manager);
    update_ts = original.Timestamp();
    auto moved = std::move(original);
  }

  {
    auto read = version_manager.acquire_read_operation();
    EXPECT_EQ(read.published_view.visibility_ts, update_ts);
  }

  const auto next_insert_ts = version_manager.acquire_insert_timestamp();
  EXPECT_EQ(next_insert_ts, update_ts + 1);
  version_manager.release_insert_timestamp(next_insert_ts);
}

TEST(WalReplayVersionManagerTest, UpdateLeaseFinishDoesNotResetTimeline) {
  neug::VersionManager version_manager;
  version_manager.init_ts({40, 0}, 1);

  neug::UpdateTimestampLease update_lease(version_manager);
  const auto update_ts = update_lease.Timestamp();
  update_lease.Finish(std::nullopt);

  auto read = version_manager.acquire_read_operation();
  EXPECT_EQ(read.published_view.visibility_ts, update_ts);
}

TEST(WalReplayVersionManagerTest, BeginUpdateCommitRejectsMissingUpdateLease) {
  neug::VersionManager version_manager;
  version_manager.init_ts({0, 0}, 1);

  EXPECT_THROW(version_manager.begin_update_commit(1), std::exception);
}

TEST(WalReplayVersionManagerTest,
     OrdinaryUpdateCommitDoesNotWaitForExistingReader) {
  using namespace std::chrono_literals;

  neug::VersionManager version_manager;
  version_manager.init_ts({0, 0}, 1);
  auto reader = version_manager.acquire_read_operation();
  neug::UpdateTimestampLease update_lease(version_manager);

  auto commit = std::async(std::launch::async, [&]() {
    update_lease.BeginCommit();
    update_lease.Finish(std::nullopt);
  });
  const auto status = commit.wait_for(100ms);
  reader.admission.release();

  EXPECT_EQ(status, std::future_status::ready);
  commit.get();
}

TEST(WalReplayVersionManagerTest,
     InPlaceMutationExclusivityWaitsForExistingReaderAndBlocksNewReader) {
  using namespace std::chrono_literals;

  neug::VersionManager version_manager;
  version_manager.init_ts({40, 0}, 1);
  ASSERT_TRUE(version_manager.try_set_runtime_wait_if_quiescent(
      &ObserveInPlaceMutationWait));
  auto existing_reader = version_manager.acquire_read_operation();

  neug::UpdateTimestampLease update_lease(version_manager);

  in_place_mutation_waiting.store(false, std::memory_order_relaxed);
  std::atomic<bool> exclusivity_finished{false};
  std::thread exclusivity_thread([&]() {
    update_lease.MakeUpdateExclusive();
    exclusivity_finished.store(true, std::memory_order_release);
  });
  while (!in_place_mutation_waiting.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }

  auto new_reader = std::async(std::launch::async, [&]() {
    auto read = version_manager.acquire_read_operation();
    return read.published_view;
  });
  EXPECT_EQ(new_reader.wait_for(20ms), std::future_status::timeout);
  EXPECT_FALSE(exclusivity_finished.load(std::memory_order_acquire));

  existing_reader.admission.release();
  exclusivity_thread.join();
  EXPECT_TRUE(exclusivity_finished.load(std::memory_order_acquire));
  EXPECT_EQ(new_reader.wait_for(20ms), std::future_status::timeout);

  update_lease.FinishAndResetTimeline();
  EXPECT_EQ(new_reader.get().visibility_ts, 0);
}

class WalEpochActivationHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = make_test_dir();
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);

    checkpoint_manager_.Open(test_dir_);
    graph_ = std::make_shared<neug::PropertyGraph>();
    graph_->Open(make_checkpoint(checkpoint_manager_),
                 neug::MemoryLevel::kInMemory);
    snapshot_store_ = std::make_unique<neug::GraphSnapshotStore>(2, graph_);
    coordinator_ = std::make_unique<neug::CheckpointCoordinator>(
        checkpoint_manager_, *snapshot_store_, neug::MemoryLevel::kInMemory,
        [this](const std::string& allocator_dir) {
          handler_calls_.push_back(allocator_dir);
        });
  }

  void TearDown() override {
    coordinator_.reset();
    snapshot_store_.reset();
    graph_.reset();
    checkpoint_manager_.Close();
    std::filesystem::remove_all(test_dir_);
  }

  std::string test_dir_;
  neug::CheckpointManager checkpoint_manager_;
  std::shared_ptr<neug::PropertyGraph> graph_;
  std::unique_ptr<neug::GraphSnapshotStore> snapshot_store_;
  std::unique_ptr<neug::CheckpointCoordinator> coordinator_;
  std::vector<std::string> handler_calls_;
};

TEST_F(WalEpochActivationHandlerTest,
       RecoveryAndManualReopenBeforeWalActivation) {
  size_t activation_calls = 0;
  std::string observed_wal_uri;
  coordinator_->SetWalEpochActivationHandler([&](const std::string& wal_uri) {
    // The mandatory post-reopen handler runs before service WAL activation.
    EXPECT_EQ(handler_calls_.size(), 2u);
    ++activation_calls;
    observed_wal_uri = wal_uri;
  });

  ASSERT_TRUE(coordinator_->PublishRecoveryCheckpoint().ok());
  EXPECT_EQ(handler_calls_.size(), 1u);
  EXPECT_EQ(activation_calls, 0u);

  neug::VersionManager version_manager;
  version_manager.init_ts({40, 0}, 1);
  neug::UpdateTimestampLease update_lease(version_manager);
  ASSERT_TRUE(
      coordinator_->PublishManualCheckpoint(std::move(update_lease)).ok());

  EXPECT_EQ(activation_calls, 1u);
  const auto current_checkpoint = checkpoint_manager_.Current();
  ASSERT_NE(current_checkpoint, nullptr);
  EXPECT_EQ(observed_wal_uri, current_checkpoint->wal_dir());
  ASSERT_EQ(handler_calls_.size(), 2u);
  EXPECT_EQ(handler_calls_[1], current_checkpoint->allocator_dir());
  {
    auto read = version_manager.acquire_read_operation();
    EXPECT_EQ(read.published_view.visibility_ts, 0u);
  }
}

TEST_F(WalEpochActivationHandlerTest,
       IncrementalCheckpointReopensOnlyDirtyModulesAndKeepsTimeline) {
  neug::CreateVertexTypeParamBuilder dirty_builder;
  ASSERT_TRUE(
      graph_
          ->CreateVertexType(dirty_builder.VertexLabel("dirty")
                                 .AddProperty("id", neug::Value::INT64(0))
                                 .AddProperty("value", neug::Value::INT32(0))
                                 .AddPrimaryKeyName("id")
                                 .Build())
          .ok());
  neug::CreateVertexTypeParamBuilder clean_builder;
  ASSERT_TRUE(
      graph_
          ->CreateVertexType(clean_builder.VertexLabel("clean")
                                 .AddProperty("id", neug::Value::INT64(0))
                                 .AddProperty("value", neug::Value::INT32(0))
                                 .AddPrimaryKeyName("id")
                                 .Build())
          .ok());
  const auto dirty_label = graph_->schema().get_vertex_label_id("dirty");
  const auto clean_label = graph_->schema().get_vertex_label_id("clean");
  neug::vid_t dirty_vid;
  neug::vid_t clean_vid;
  ASSERT_TRUE(graph_
                  ->AddVertex(dirty_label, neug::Value::INT64(1),
                              {neug::Value::INT32(10)}, dirty_vid, 0)
                  .ok());
  ASSERT_TRUE(graph_
                  ->AddVertex(clean_label, neug::Value::INT64(1),
                              {neug::Value::INT32(20)}, clean_vid, 0)
                  .ok());
  graph_->MarkSchemaDirty();
  graph_->MarkVertexTableDirty(dirty_label);
  graph_->MarkVertexTableDirty(clean_label);

  size_t wal_activation_calls = 0;
  coordinator_->SetWalEpochActivationHandler(
      [&](const std::string&) { ++wal_activation_calls; });
  neug::VersionManager version_manager;
  version_manager.init_ts({0, 0}, 1);
  ASSERT_TRUE(
      coordinator_
          ->PublishManualCheckpoint(neug::UpdateTimestampLease(version_manager))
          .ok());
  ASSERT_EQ(handler_calls_.size(), 1u);
  ASSERT_EQ(wal_activation_calls, 1u);

  const auto full_checkpoint = checkpoint_manager_.Current();
  ASSERT_NE(full_checkpoint, nullptr);
  const auto descriptor_path = [](const neug::Checkpoint& checkpoint,
                                  const std::string& key) {
    const auto* descriptor = checkpoint.manifest().FindModule(key);
    EXPECT_NE(descriptor, nullptr);
    return descriptor == nullptr
               ? std::optional<std::string>()
               : descriptor->get_path(neug::ModuleDescriptor::kDataPath);
  };
  const auto dirty_key = neug::VertexTable::KeyKeys("dirty");
  const auto clean_key = neug::VertexTable::KeyKeys("clean");
  const auto old_dirty_path = descriptor_path(*full_checkpoint, dirty_key);
  const auto old_clean_path = descriptor_path(*full_checkpoint, clean_key);
  ASSERT_TRUE(old_dirty_path.has_value());
  ASSERT_TRUE(old_clean_path.has_value());
  const auto* clean_timestamp =
      &graph_->get_vertex_table(clean_label).get_vertex_timestamp();

  neug::UpdateTimestampLease incremental_lease(version_manager);
  const auto incremental_ts = incremental_lease.Timestamp();
  neug::vid_t new_vid;
  ASSERT_TRUE(graph_
                  ->AddVertex(dirty_label, neug::Value::INT64(2),
                              {neug::Value::INT32(11)}, new_vid, incremental_ts)
                  .ok());
  graph_->MarkVertexTableDirty(dirty_label);
  const auto dirty_capacity = graph_->get_vertex_table(dirty_label).Capacity();
  uint64_t planning_generation = 0;
  {
    neug::SnapshotGuard current(*snapshot_store_);
    planning_generation = current.get().planning_generation();
  }
  ASSERT_TRUE(
      coordinator_->PublishIncrementalCheckpoint(std::move(incremental_lease))
          .ok());

  const auto incremental_checkpoint = checkpoint_manager_.Current();
  ASSERT_NE(incremental_checkpoint, nullptr);
  EXPECT_GT(incremental_checkpoint->id(), full_checkpoint->id());
  EXPECT_EQ(incremental_checkpoint->manifest().base_timestamp(),
            incremental_ts);
  EXPECT_NE(descriptor_path(*incremental_checkpoint, dirty_key),
            old_dirty_path);
  EXPECT_EQ(descriptor_path(*incremental_checkpoint, clean_key),
            old_clean_path);
  EXPECT_EQ(&graph_->get_vertex_table(clean_label).get_vertex_timestamp(),
            clean_timestamp);
  EXPECT_EQ(graph_->get_vertex_table(dirty_label).Capacity(), dirty_capacity);
  EXPECT_EQ(graph_->VertexNum(dirty_label, incremental_ts - 1), 1u);
  EXPECT_EQ(graph_->VertexNum(dirty_label, incremental_ts), 2u);
  EXPECT_EQ(handler_calls_.size(), 1u)
      << "incremental checkpoint must not reopen allocators";
  EXPECT_EQ(wal_activation_calls, 2u);
  {
    neug::SnapshotGuard current(*snapshot_store_);
    EXPECT_EQ(current.get().planning_generation(), planning_generation);
  }

  neug::UpdateTimestampLease next_lease(version_manager);
  EXPECT_EQ(next_lease.Timestamp(), incremental_ts + 1);
  const auto checkpoint_id = incremental_checkpoint->id();
  ASSERT_TRUE(
      coordinator_->PublishIncrementalCheckpoint(std::move(next_lease)).ok());
  ASSERT_NE(checkpoint_manager_.Current(), nullptr);
  EXPECT_EQ(checkpoint_manager_.Current()->id(), checkpoint_id);
  EXPECT_EQ(wal_activation_calls, 2u)
      << "a clean graph must not rotate its WAL epoch";

  neug::CreateVertexTypeParamBuilder schema_builder;
  ASSERT_TRUE(
      graph_
          ->CreateVertexType(schema_builder.VertexLabel("temporary_name")
                                 .AddProperty("id", neug::Value::INT64(0))
                                 .AddPrimaryKeyName("id")
                                 .Build())
          .ok());
  const auto added_label =
      graph_->schema().get_vertex_label_id("temporary_name");
  graph_->MarkSchemaDirty();
  neug::UpdateTimestampLease schema_lease(version_manager);
  ASSERT_TRUE(
      coordinator_->PublishIncrementalCheckpoint(std::move(schema_lease)).ok());
  {
    neug::SnapshotGuard current(*snapshot_store_);
    EXPECT_EQ(current.get().planning_generation(), planning_generation + 1);
  }
  EXPECT_TRUE(graph_->schema().is_vertex_label_valid(added_label));
  EXPECT_EQ(handler_calls_.size(), 1u);
  EXPECT_EQ(wal_activation_calls, 3u);

  ASSERT_TRUE(graph_->DeleteVertexType(added_label).ok());
  graph_->MarkSchemaDirty();
  neug::UpdateTimestampLease drop_lease(version_manager);
  ASSERT_TRUE(
      coordinator_->PublishIncrementalCheckpoint(std::move(drop_lease)).ok());
  {
    neug::SnapshotGuard current(*snapshot_store_);
    EXPECT_EQ(current.get().planning_generation(), planning_generation + 2);
  }
  EXPECT_FALSE(graph_->schema().is_vertex_label_valid(added_label));
  EXPECT_EQ(handler_calls_.size(), 1u);
  EXPECT_EQ(wal_activation_calls, 4u);
}

TEST(CheckpointCoordinatorTest,
     PreparationFailureKeepsOldWalAndTimestampTimelineUsable) {
  const auto test_dir = make_test_dir();
  std::filesystem::remove_all(test_dir);
  std::filesystem::create_directories(test_dir);

  neug::CheckpointManager checkpoint_manager;
  checkpoint_manager.Open(test_dir);
  auto graph = std::make_shared<neug::PropertyGraph>();
  graph->Open(make_checkpoint(checkpoint_manager),
              neug::MemoryLevel::kInMemory);
  neug::GraphSnapshotStore snapshot_store(2, graph);
  std::vector<std::shared_ptr<neug::Allocator>> allocators;
  allocators.emplace_back(
      std::make_shared<neug::Allocator>(neug::MemoryLevel::kInMemory, ""));
  constexpr size_t allocator_marker_size = 64;
  ASSERT_NE(allocators[0]->allocate(allocator_marker_size), nullptr);
  bool allocator_reopened = false;
  bool cache_invalidated = false;
  neug::CheckpointCoordinator coordinator(
      checkpoint_manager, snapshot_store, neug::MemoryLevel::kInMemory,
      [&](const std::string&) {
        allocators[0]->Reopen(neug::MemoryLevel::kInMemory, "");
        allocator_reopened = true;
      });

  const auto old_wal_dir =
      (std::filesystem::path(test_dir) / "old-wal").string();
  auto wal_writer = neug::WalWriterFactory::CreateWalWriter(old_wal_dir, 0);
  wal_writer->open(old_wal_dir);
  constexpr uint32_t before_marker = 17;
  constexpr uint32_t after_marker = 29;
  ASSERT_TRUE(wal_writer->append(reinterpret_cast<const char*>(&before_marker),
                                 sizeof(before_marker)));

  // Keep the checkpoint manager's only staging slot occupied.
  // PublishManualCheckpoint must fail before destructive graph maintenance,
  // release the update lease normally, and leave the existing WAL writer
  // untouched.
  auto conflicting_staging = checkpoint_manager.CreateStaging();
  neug::VersionManager version_manager;
  version_manager.init_ts({40, 0}, 1);
  neug::UpdateTimestampLease update_lease(version_manager);
  const auto update_ts = update_lease.Timestamp();
  coordinator.SetWalEpochActivationHandler([&](const std::string& wal_uri) {
    wal_writer->close();
    wal_writer->open(wal_uri);
    cache_invalidated = true;
  });
  auto status = coordinator.PublishManualCheckpoint(std::move(update_lease));

  EXPECT_FALSE(status.ok());
  EXPECT_FALSE(allocator_reopened);
  EXPECT_EQ(allocators[0]->allocated_memory(), allocator_marker_size);
  EXPECT_FALSE(cache_invalidated);
  EXPECT_TRUE(wal_writer->append(reinterpret_cast<const char*>(&after_marker),
                                 sizeof(after_marker)));

  {
    auto read = version_manager.acquire_read_operation();
    EXPECT_EQ(read.published_view.visibility_ts, update_ts);
  }
  const auto next_insert_ts = version_manager.acquire_insert_timestamp();
  EXPECT_EQ(next_insert_ts, update_ts + 1);
  version_manager.release_insert_timestamp(next_insert_ts);

  wal_writer->close();
  coordinator.ClearWalEpochActivationHandler();
  conflicting_staging.Discard();
  checkpoint_manager.Close();
  std::filesystem::remove_all(test_dir);
}

TEST_F(WalReplayTest, CloseCheckpointResetsSharedApTpTimeline) {
  {
    auto config = make_config(db_dir_);
    config.checkpoint_on_close = true;

    neug::NeugDB db;
    ASSERT_TRUE(db.Open(config));
    create_person_schema(db);
    {
      neug::NeugDBService service(db);
      EXPECT_EQ(insert_person_and_return_ts(service, 1, "old"), 2)
          << "TP must continue after the AP schema statement timestamp";
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

TEST_F(WalReplayTest, ExplainCheckpointDoesNotConsumeApOrTpUpdateTimestamp) {
  neug::NeugDB db;
  ASSERT_TRUE(db.Open(make_config(db_dir_)));
  create_person_schema(db);

  {
    auto conn = db.Connect();
    auto result = conn->Query("EXPLAIN CHECKPOINT;");
    ASSERT_TRUE(result) << result.error().ToString();
    conn->Close();
  }

  {
    neug::NeugDBService service(db);
    {
      auto slot = service.AcquireExecutionSlot();
      auto result = slot->ExecuteTransactionalRequest(
          R"({"query":"  explain CHECKPOINT;","parameters":{}})");
      ASSERT_TRUE(result) << result.error().ToString();
    }

    EXPECT_EQ(insert_person_and_return_ts(service, 1, "after-explain"), 2);
  }
  db.Close();
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

TEST_F(WalReplayTest, ReadOnlyServiceExecutesReadsAndRejectsWrites) {
  create_checkpointed_base_graph(db_dir_);

  auto config = make_config(db_dir_);
  config.mode = neug::DBMode::READ_ONLY;

  neug::NeugDB db;
  ASSERT_TRUE(db.Open(config));
  {
    neug::NeugDBService service(db);
    EXPECT_EQ(read_person_count(service), 1);
    auto slot = service.AcquireExecutionSlot();
    auto result = slot->ExecuteTransactionalRequest(
        R"({"query":"CREATE (:person {id: 2, name: 'blocked'});","access_mode":"insert","parameters":{}})");
    EXPECT_FALSE(result);
    if (!result) {
      EXPECT_EQ(result.error().error_code(),
                neug::StatusCode::ERR_INVALID_ARGUMENT);
    }
  }
  db.Close();
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

std::optional<uint64_t> read_current_checkpoint_id(const std::string& db_dir) {
  const auto current = std::filesystem::path(db_dir) / "checkpoint" / "CURRENT";
  std::error_code ec;
  if (!std::filesystem::exists(current, ec)) {
    return std::nullopt;
  }
  std::ifstream input(current);
  uint64_t id = 0;
  if (!(input >> id)) {
    return std::nullopt;
  }
  return id;
}

std::optional<std::string> read_person_name(neug::NeugDBService& service,
                                            int64_t id) {
  auto slot = service.AcquireExecutionSlot();
  auto txn = slot->GetReadTransaction();
  neug::StorageReadInterface graph(txn.view(), txn.timestamp());
  const auto person_label = graph.schema().get_vertex_label_id("person");
  neug::vid_t vid = 0;
  std::optional<std::string> name;
  if (graph.GetVertexIndex(person_label, Value::INT64(id), vid)) {
    // "name" is the first (and only) property column; the primary key is
    // stored separately and does not consume a property column id.
    name =
        graph.GetVertexProperty(person_label, vid, 0).GetValue<std::string>();
  }
  EXPECT_TRUE(txn.Commit());
  return name;
}

void write_copy_csv(const std::string& path, const std::string& rows) {
  std::ofstream csv(path);
  csv << "id|name\n" << rows;
}

TEST_F(WalReplayTest, ApWalIsRestoredAfterTpServiceStops) {
  create_checkpointed_base_graph(db_dir_);

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    {
      neug::NeugDBService service(db);
      EXPECT_EQ(read_person_name(service, 1),
                std::optional<std::string>("seed"));
    }

    auto conn = db.Connect();
    auto update = conn->Query(
        "MATCH (n:person {id: 1}) SET n.name = 'after-tp';", "update");
    ASSERT_TRUE(update) << update.error().ToString();
    conn->Close();
    db.Close();
  }

  // checkpoint_on_close is disabled. Recovery therefore proves that the AP
  // write used a real WAL writer restored after the TP pool was destroyed.
  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    {
      neug::NeugDBService service(db);
      EXPECT_EQ(read_person_name(service, 1),
                std::optional<std::string>("after-tp"));
    }
    db.Close();
  }
}

TEST_F(WalReplayTest, MandatoryCloseCheckpointCanBeRetried) {
  const auto csv_a = (std::filesystem::path(db_dir_) / "people-a.csv").string();
  const auto csv_b = (std::filesystem::path(db_dir_) / "people-b.csv").string();
  write_copy_csv(csv_a, "2|copy-a\n");
  write_copy_csv(csv_b, "3|copy-b\n");

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    auto conn = db.Connect();
    assert_query_ok(
        *conn,
        "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));");
    ASSERT_TRUE(conn->Query("COPY person FROM \"" + csv_a + "\";", "update"));
    conn->Close();

    const auto runtime_dir = std::filesystem::path(db_dir_) / "runtime";
    std::filesystem::remove_all(runtime_dir);
    {
      std::ofstream block(runtime_dir);
      block << "not a directory";
    }

    auto conn2 = db.Connect();
    auto failed_copy =
        conn2->Query("COPY person FROM \"" + csv_b + "\";", "update");
    ASSERT_FALSE(failed_copy);
    conn2->Close();

    EXPECT_THROW(db.Close(), neug::exception::IOException);
    EXPECT_FALSE(db.IsClosed());

    std::filesystem::remove(runtime_dir);
    std::filesystem::create_directories(runtime_dir);
    EXPECT_NO_THROW(db.Close());
    EXPECT_TRUE(db.IsClosed());
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    {
      neug::NeugDBService service(db);
      EXPECT_EQ(read_person_count(service), 2u);
      EXPECT_TRUE(read_has_person(service, 2));
      EXPECT_TRUE(read_has_person(service, 3));
    }
    db.Close();
  }
}

// Captures everything written to stderr while installed. glog routes its
// output to stderr before InitGoogleLogging() runs (which holds for this
// test binary), so advisory WARNINGs emitted by the storage layer can be
// asserted on. A google::LogSink cannot be used here: the library embeds
// its own glog copy inside libneug.dylib, so a sink registered from the
// test executable never observes the library's log calls.
#ifndef _WIN32
class ScopedStderrCapture {
 public:
  ScopedStderrCapture() {
    ::fflush(stderr);
    saved_fd_ = ::dup(STDERR_FILENO);
    capture_fd_ = ::mkstemp(path_);
    ::dup2(capture_fd_, STDERR_FILENO);
  }

  ~ScopedStderrCapture() {
    ::fflush(stderr);
    ::dup2(saved_fd_, STDERR_FILENO);
    ::close(saved_fd_);
    ::close(capture_fd_);
    ::unlink(path_);
  }

  bool capturedContains(const std::string& needle) {
    ::fflush(stderr);
    std::ifstream in(path_);
    const std::string content((std::istreambuf_iterator<char>(in)),
                              std::istreambuf_iterator<char>());
    return content.find(needle) != std::string::npos;
  }

 private:
  int saved_fd_{-1};
  int capture_fd_{-1};
  char path_[32]{"/tmp/neug_stderr_capture_XXXXXX"};
};
#endif  // _WIN32

// A successful COPY FROM must publish an incremental checkpoint before the
// statement returns, so the loaded data survives a crash even though neither
// checkpoint_on_close nor checkpoint_on_recovery is enabled.
TEST_F(WalReplayTest, CopyFromSealsIncrementalCheckpointAndSurvivesRecovery) {
  const auto csv_path =
      (std::filesystem::path(db_dir_) / "people.csv").string();
  write_copy_csv(csv_path, "2|copy-a\n3|copy-b\n");

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    auto conn = db.Connect();
    assert_query_ok(
        *conn,
        "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));");
    auto copy = conn->Query("COPY person FROM \"" + csv_path + "\";", "update");
    ASSERT_TRUE(copy) << copy.error().ToString();
    conn->Close();
    db.Close();
  }

  // checkpoint_on_close is disabled, so the only durable state comes from the
  // statement-level incremental checkpoint published by the COPY itself.
  const auto checkpoint_id = read_current_checkpoint_id(db_dir_);
  ASSERT_TRUE(checkpoint_id.has_value())
      << "a successful COPY must publish an incremental checkpoint";

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    {
      neug::NeugDBService service(db);
      EXPECT_EQ(read_person_count(service), 2u);
      EXPECT_TRUE(read_has_person(service, 2));
      EXPECT_TRUE(read_has_person(service, 3));
    }
    db.Close();
  }
}

// Regression for the WAL replay chain: an UPDATE records a WAL record that
// hard-CHECKs its target vertex during replay. Without the COPY-seal the
// replayed record would reference a vertex that only exists in unsealed
// in-memory state, crashing recovery. With the seal the checkpoint already
// contains the COPY data, so replay finds the vertex.
TEST_F(WalReplayTest, CopyFromThenWalUpdateRecoversAfterCrash) {
  const auto csv_path =
      (std::filesystem::path(db_dir_) / "people.csv").string();
  write_copy_csv(csv_path, "2|copy-a\n3|copy-b\n");

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    auto conn = db.Connect();
    assert_query_ok(
        *conn,
        "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));");
    auto copy = conn->Query("COPY person FROM \"" + csv_path + "\";", "update");
    ASSERT_TRUE(copy) << copy.error().ToString();
    const auto sealed_by_copy = read_current_checkpoint_id(db_dir_);
    ASSERT_TRUE(sealed_by_copy.has_value());
    auto update = conn->Query(
        "MATCH (n:person {id: 2}) SET n.name = 'updated';", "update");
    ASSERT_TRUE(update) << update.error().ToString();
    EXPECT_EQ(read_current_checkpoint_id(db_dir_), sealed_by_copy)
        << "an ordinary WAL-backed write must not trigger an incremental "
           "checkpoint";
    conn->Close();
    db.Close();
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    {
      neug::NeugDBService service(db);
      EXPECT_EQ(read_person_count(service), 2u);
      EXPECT_EQ(read_person_name(service, 2),
                std::optional<std::string>("updated"));
      EXPECT_EQ(read_person_name(service, 3),
                std::optional<std::string>("copy-b"));
    }
    db.Close();
  }
}

// A failed COPY must not publish an incremental checkpoint (decision D1).
// The malformed row makes the statement fail before any row is applied, so
// the durable checkpoint id must stay unchanged. (Failures that leave
// residual in-memory mutations are covered by the write-barrier tests.)
TEST_F(WalReplayTest, FailedCopyDoesNotPublishIncrementalCheckpoint) {
  const auto good_csv = (std::filesystem::path(db_dir_) / "good.csv").string();
  write_copy_csv(good_csv, "2|copy-a\n");
  const auto bad_csv = (std::filesystem::path(db_dir_) / "bad.csv").string();
  // A non-numeric id fails CSV parsing, so the statement fails up front.
  write_copy_csv(bad_csv, "not-a-number|copy-c\n");

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    auto conn = db.Connect();
    assert_query_ok(
        *conn,
        "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));");
    auto copy = conn->Query("COPY person FROM \"" + good_csv + "\";", "update");
    ASSERT_TRUE(copy) << copy.error().ToString();
    conn->Close();
    db.Close();
  }
  const auto sealed_id = read_current_checkpoint_id(db_dir_);
  ASSERT_TRUE(sealed_id.has_value());

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    auto conn = db.Connect();
    auto copy = conn->Query("COPY person FROM \"" + bad_csv + "\";", "update");
    ASSERT_FALSE(copy) << "the malformed row must fail the COPY statement";
    conn->Close();

    // The failed statement published nothing: the durable checkpoint id is
    // unchanged and no residual row was applied.
    EXPECT_EQ(read_current_checkpoint_id(db_dir_), sealed_id)
        << "a failed COPY must not publish an incremental checkpoint";
    {
      neug::NeugDBService service(db);
      EXPECT_EQ(read_person_count(service), 1u);
    }
    db.Close();
  }
}

// When incremental-checkpoint preparation fails before dirty modules are
// consumed (here: staging cannot create its runtime directories), COPY
// surfaces the error while the in-memory mutations stay queryable. A first
// successful COPY establishes a sealed baseline.
TEST_F(WalReplayTest, CopyFromSurfacesIncrementalCheckpointFailure) {
  const auto csv_a = (std::filesystem::path(db_dir_) / "people-a.csv").string();
  const auto csv_b = (std::filesystem::path(db_dir_) / "people-b.csv").string();
  write_copy_csv(csv_a, "2|copy-a\n");
  write_copy_csv(csv_b, "3|copy-b\n");

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    auto conn = db.Connect();
    assert_query_ok(
        *conn,
        "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));");
    auto copy_a = conn->Query("COPY person FROM \"" + csv_a + "\";", "update");
    ASSERT_TRUE(copy_a) << copy_a.error().ToString();
    conn->Close();

    // Block the staging checkpoint from creating its runtime directories by
    // replacing <db>/runtime with a regular file.
    const auto runtime_dir = std::filesystem::path(db_dir_) / "runtime";
    std::filesystem::remove_all(runtime_dir);
    {
      std::ofstream block(runtime_dir);
      block << "not a directory";
    }

    auto conn2 = db.Connect();
    auto copy_b = conn2->Query("COPY person FROM \"" + csv_b + "\";", "update");
    ASSERT_FALSE(copy_b)
        << "a failing incremental checkpoint must fail the COPY statement";
    if (!copy_b) {
      EXPECT_NE(copy_b.error().error_code(), neug::StatusCode::OK);
    }
    conn2->Close();

    // The published in-memory mutations remain queryable in AP mode. TP mode
    // is not opened until the pending barrier can be sealed.
    auto read_conn = db.Connect();
    auto count = read_conn->Query("MATCH (n:person) RETURN count(n);", "read");
    ASSERT_TRUE(count) << count.error().ToString();
    ASSERT_EQ(count->response().arrays_size(), 1);
    ASSERT_EQ(count->response().arrays(0).int64_array().values_size(), 1);
    EXPECT_EQ(count->response().arrays(0).int64_array().values(0), 2);
    read_conn->Close();

    // A failed AP-to-TP transition must leave the embedded runtime usable.
    EXPECT_THROW({ neug::NeugDBService service(db); },
                 neug::exception::IOException);
    auto retry_conn = db.Connect();
    retry_conn->Close();

    // Clear the fault. Entering TP mode seals the pending AP mutations before
    // transactional slots and their WAL writers are created.
    std::filesystem::remove(runtime_dir);
    std::filesystem::create_directories(runtime_dir);
    {
      neug::NeugDBService service(db);
      EXPECT_EQ(read_person_count(service), 2u);
    }
    db.Close();
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    {
      neug::NeugDBService service(db);
      EXPECT_EQ(read_person_count(service), 2u);
      EXPECT_TRUE(read_has_person(service, 2));
      EXPECT_TRUE(read_has_person(service, 3));
    }
    db.Close();
  }
}

// A failed incremental-checkpoint preparation leaves residual in-memory
// mutations. The next WAL-logging transaction must seal them first, otherwise
// replay of its UPDATE record hard-CHECKs on a vertex that only exists in
// unsealed memory.
TEST_F(WalReplayTest, WriteBarrierSealsResidualMutationsBeforeWalTransaction) {
  const auto csv_a = (std::filesystem::path(db_dir_) / "people-a.csv").string();
  const auto csv_b = (std::filesystem::path(db_dir_) / "people-b.csv").string();
  write_copy_csv(csv_a, "2|copy-a\n");
  write_copy_csv(csv_b, "5|copy-b\n");
  std::optional<uint64_t> sealed_by_copy;

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    auto conn = db.Connect();
    assert_query_ok(
        *conn,
        "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));");
    auto copy_a = conn->Query("COPY person FROM \"" + csv_a + "\";", "update");
    ASSERT_TRUE(copy_a) << copy_a.error().ToString();
    conn->Close();
    sealed_by_copy = read_current_checkpoint_id(db_dir_);
    ASSERT_TRUE(sealed_by_copy.has_value());

    // Leave residual in-memory mutations: the second COPY applies its rows,
    // but the incremental checkpoint fails, so the statement fails while the
    // graph stays dirty in memory.
    const auto runtime_dir = std::filesystem::path(db_dir_) / "runtime";
    std::filesystem::remove_all(runtime_dir);
    {
      std::ofstream block(runtime_dir);
      block << "not a directory";
    }
    auto conn2 = db.Connect();
    auto copy_b = conn2->Query("COPY person FROM \"" + csv_b + "\";", "update");
    ASSERT_FALSE(copy_b)
        << "the blocked incremental checkpoint must fail the COPY statement";
    conn2->Close();
    std::filesystem::remove(runtime_dir);
    std::filesystem::create_directories(runtime_dir);

    // The failed statement published nothing.
    EXPECT_EQ(read_current_checkpoint_id(db_dir_), sealed_by_copy)
        << "a failed COPY must not publish an incremental checkpoint";

    // The WAL-logging UPDATE passes the write barrier: residual rows are
    // sealed first, then the UPDATE records WAL against the sealed graph.
    auto conn3 = db.Connect();
    auto update = conn3->Query(
        "MATCH (n:person {id: 5}) SET n.name = 'updated';", "update");
    ASSERT_TRUE(update) << update.error().ToString();
    conn3->Close();
    db.Close();

    const auto sealed_by_barrier = read_current_checkpoint_id(db_dir_);
    ASSERT_TRUE(sealed_by_barrier.has_value());
    EXPECT_GT(*sealed_by_barrier, *sealed_by_copy)
        << "the write barrier must seal residual mutations before the "
           "WAL-logging UPDATE";
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    {
      neug::NeugDBService service(db);
      EXPECT_EQ(read_person_count(service), 2u);
      EXPECT_EQ(read_person_name(service, 2),
                std::optional<std::string>("copy-a"));
      EXPECT_EQ(read_person_name(service, 5),
                std::optional<std::string>("updated"))
          << "the UPDATE's WAL record must replay against the sealed rows";
    }
    db.Close();
  }
}

// A second COPY to the same table makes the incremental checkpoint rewrite
// a table the previous checkpoint already contains; the rewrite is logged as
// a WARNING advising batching, and the data stays consistent across
// recovery. The warning is asserted by capturing stderr (see
// ScopedStderrCapture for why a glog LogSink cannot be used).
TEST_F(WalReplayTest, RepeatedCopyToSameTableWarnsOnFullRewrite) {
#ifndef _WIN32
  const auto csv_a = (std::filesystem::path(db_dir_) / "people-a.csv").string();
  const auto csv_b = (std::filesystem::path(db_dir_) / "people-b.csv").string();
  write_copy_csv(csv_a, "2|copy-a\n");
  write_copy_csv(csv_b, "3|copy-b\n");

  ScopedStderrCapture capture;
  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    auto conn = db.Connect();
    assert_query_ok(
        *conn,
        "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));");
    auto copy_a = conn->Query("COPY person FROM \"" + csv_a + "\";", "update");
    ASSERT_TRUE(copy_a) << copy_a.error().ToString();
    // The first COPY seeds a new table absent from the previous checkpoint,
    // so no rewrite warning is expected yet.
    EXPECT_FALSE(capture.capturedContains("rewrites vertex table 'person'"))
        << "seeding a new table must not warn about full-table rewrites";

    auto copy_b = conn->Query("COPY person FROM \"" + csv_b + "\";", "update");
    ASSERT_TRUE(copy_b) << copy_b.error().ToString();
    conn->Close();
    db.Close();
  }

  EXPECT_TRUE(capture.capturedContains("rewrites vertex table 'person'"))
      << "the second COPY rewrites a table the previous checkpoint already "
         "contains and must warn";
  EXPECT_TRUE(capture.capturedContains("consider batching"))
      << "the warning should advise batching repeated bulk writes";

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    {
      neug::NeugDBService service(db);
      EXPECT_EQ(read_person_count(service), 2u);
      EXPECT_EQ(read_person_name(service, 2),
                std::optional<std::string>("copy-a"));
      EXPECT_EQ(read_person_name(service, 3),
                std::optional<std::string>("copy-b"));
    }
    db.Close();
  }
#else
  GTEST_SKIP() << "stderr capture relies on POSIX dup2";
#endif  // _WIN32
}

// Pure-read statements must never reach the write barrier: residual dirty
// state stays unsealed (the durable checkpoint id does not advance) across
// read traffic, while readers still observe the residual rows in memory.
TEST_F(WalReplayTest, PureReadWorkloadBypassesWriteBarrier) {
  const auto csv_a = (std::filesystem::path(db_dir_) / "people-a.csv").string();
  const auto csv_b = (std::filesystem::path(db_dir_) / "people-b.csv").string();
  write_copy_csv(csv_a, "2|copy-a\n");
  write_copy_csv(csv_b, "5|copy-b\n");
  std::optional<uint64_t> sealed_by_copy;

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    auto conn = db.Connect();
    assert_query_ok(
        *conn,
        "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));");
    auto copy_a = conn->Query("COPY person FROM \"" + csv_a + "\";", "update");
    ASSERT_TRUE(copy_a) << copy_a.error().ToString();
    conn->Close();
    sealed_by_copy = read_current_checkpoint_id(db_dir_);
    ASSERT_TRUE(sealed_by_copy.has_value());

    // Residual in-memory mutation via a failing incremental checkpoint.
    const auto runtime_dir = std::filesystem::path(db_dir_) / "runtime";
    std::filesystem::remove_all(runtime_dir);
    {
      std::ofstream block(runtime_dir);
      block << "not a directory";
    }
    auto conn2 = db.Connect();
    auto copy_b = conn2->Query("COPY person FROM \"" + csv_b + "\";", "update");
    ASSERT_FALSE(copy_b);
    conn2->Close();
    std::filesystem::remove(runtime_dir);
    std::filesystem::create_directories(runtime_dir);

    EXPECT_EQ(read_current_checkpoint_id(db_dir_), sealed_by_copy);

    // Read-only traffic observes the residual rows but must never seal them:
    // a barrier hit would advance the durable checkpoint id.
    auto conn3 = db.Connect();
    for (int i = 0; i < 3; ++i) {
      auto count = conn3->Query("MATCH (n:person) RETURN count(n);", "read");
      ASSERT_TRUE(count) << count.error().ToString();
    }
    EXPECT_EQ(read_current_checkpoint_id(db_dir_), sealed_by_copy)
        << "pure-read statements must not trigger the WAL write barrier";

    auto visible = conn3->Query("MATCH (n:person) RETURN count(n);", "read");
    ASSERT_TRUE(visible) << visible.error().ToString();
    ASSERT_EQ(visible->response().arrays_size(), 1);
    ASSERT_EQ(visible->response().arrays(0).int64_array().values_size(), 1);
    EXPECT_EQ(visible->response().arrays(0).int64_array().values(0), 2);
    conn3->Close();
    db.Close();
  }

  const auto sealed_on_close = read_current_checkpoint_id(db_dir_);
  ASSERT_TRUE(sealed_on_close.has_value());
  EXPECT_GT(*sealed_on_close, *sealed_by_copy)
      << "Close must seal a pending in-place checkpoint even when "
         "checkpoint_on_close is false";

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    neug::NeugDBService service(db);
    EXPECT_EQ(read_person_count(service), 2u);
  }
}
