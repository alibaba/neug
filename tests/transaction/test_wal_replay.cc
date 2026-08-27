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
#include "neug/main/wal_writer_set.h"
#include "neug/neug.h"
#include "neug/server/neug_db_service.h"
#include "neug/storages/allocators.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/graph/cow_detach_state.h"
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

std::atomic<bool> exclusive_mutation_waiting{false};

void ObserveExclusiveMutationWait(neug::RuntimeWaitAction) noexcept {
  exclusive_mutation_waiting.store(true, std::memory_order_release);
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

TEST(WalWriterSetTest, DirectWriterStaysStableAcrossTpActivation) {
  const auto test_dir = make_test_dir();
  const auto ap_wal_dir = (std::filesystem::path(test_dir) / "ap").string();
  const auto tp_wal_dir = (std::filesystem::path(test_dir) / "tp").string();
  constexpr uint32_t marker = 37;

  {
    neug::WalWriterSet writers(/*slot_num=*/3, neug::DBMode::READ_WRITE,
                               ap_wal_dir);
    auto* const direct_writer = &writers.DirectWriter();
    ASSERT_TRUE(direct_writer->append(reinterpret_cast<const char*>(&marker),
                                      sizeof(marker)));

    writers.ActivateTransactional(ap_wal_dir);
    auto* const first_tp_writer = &writers.WriterFor(1);
    auto* const second_tp_writer = &writers.WriterFor(2);
    EXPECT_EQ(&writers.DirectWriter(), direct_writer);

    writers.RotateActive(tp_wal_dir);
    EXPECT_EQ(&writers.DirectWriter(), direct_writer);
    EXPECT_EQ(&writers.WriterFor(1), first_tp_writer);
    EXPECT_EQ(&writers.WriterFor(2), second_tp_writer);
    ASSERT_TRUE(direct_writer->append(reinterpret_cast<const char*>(&marker),
                                      sizeof(marker)));

    writers.DeactivateTransactional();
    EXPECT_EQ(&writers.DirectWriter(), direct_writer);
    EXPECT_THROW((void) writers.WriterFor(1), std::exception);
  }
  std::filesystem::remove_all(test_dir);
}

neug::NeugDBConfig make_config(const std::string& db_dir,
                               int32_t max_thread_num = 1) {
  neug::NeugDBConfig config(db_dir, max_thread_num);
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

neug::timestamp_t insert_person_and_return_ts(neug::ExecutionSlot& slot,
                                              int64_t id,
                                              const std::string& name) {
  auto txn = slot.BeginMvccInsertTransaction();
  const auto ts = txn.timestamp();
  neug::StorageTPInsertInterface interface(txn);
  const auto person_label = txn.schema().get_vertex_label_id("person");
  neug::vid_t vid = 0;
  EXPECT_TRUE(interface.AddVertex(person_label, Value::INT64(id),
                                  {Value::STRING(name)}, vid));
  EXPECT_TRUE(txn.Commit());
  return ts;
}

neug::timestamp_t insert_person_and_return_ts(neug::NeugDBService& service,
                                              int64_t id,
                                              const std::string& name) {
  auto slot = service.AcquireExecutionSlot();
  return insert_person_and_return_ts(*slot, id, name);
}

void insert_person(neug::NeugDBService& service, int64_t id,
                   const std::string& name) {
  (void) insert_person_and_return_ts(service, id, name);
}

void compact(neug::NeugDBService& service) {
  auto slot = service.AcquireExecutionSlot();
  auto txn = slot->BeginInPlaceCompactionTransaction();
  ASSERT_TRUE(txn.Commit());
}

void checkpoint(neug::ExecutionSlot& slot) {
  auto result = slot.ExecuteTransactionalRequest(
      R"({"query":"CHECKPOINT;","access_mode":"update","parameters":{}})");
  ASSERT_TRUE(result) << result.error().ToString();
}

void insert_knows_edge(neug::NeugDBService& service, int64_t src_id,
                       int64_t dst_id, int64_t since) {
  auto slot = service.AcquireExecutionSlot();
  auto txn = slot->BeginMvccInsertTransaction();
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
  auto txn = slot->BeginSnapshotReadTransaction();
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
  auto txn = slot->BeginSnapshotReadTransaction();
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
     ExclusiveMutationWaitsForExistingReaderAndBlocksNewReader) {
  using namespace std::chrono_literals;

  neug::VersionManager version_manager;
  version_manager.init_ts({40, 0}, 1);
  ASSERT_TRUE(version_manager.try_set_runtime_wait_if_quiescent(
      &ObserveExclusiveMutationWait));
  auto existing_reader = version_manager.acquire_read_operation();

  neug::UpdateTimestampLease update_lease(version_manager);

  exclusive_mutation_waiting.store(false, std::memory_order_relaxed);
  std::atomic<bool> exclusivity_finished{false};
  std::thread exclusivity_thread([&]() {
    update_lease.MakeUpdateExclusive();
    exclusivity_finished.store(true, std::memory_order_release);
  });
  while (!exclusive_mutation_waiting.load(std::memory_order_acquire)) {
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
    auto staging = checkpoint_manager_.CreateStaging();
    neug::CheckpointManifest manifest;
    manifest.SetSchema(neug::Schema());
    staging.checkpoint()->SetManifest(std::move(manifest));
    graph_ = std::make_shared<neug::PropertyGraph>();
    graph_->Open(staging.Publish(), neug::MemoryLevel::kInMemory);
    snapshot_store_ = std::make_unique<neug::GraphSnapshotStore>(2, graph_);
    coordinator_ = std::make_unique<neug::CheckpointCoordinator>(
        checkpoint_manager_, *snapshot_store_, neug::MemoryLevel::kInMemory,
        [this](const std::string& allocator_dir) {
          handler_calls_.push_back(allocator_dir);
        },
        [this](const std::string& wal_uri) {
          ++wal_activation_calls_;
          observed_wal_uri_ = wal_uri;
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
  size_t wal_activation_calls_{0};
  std::string observed_wal_uri_;
};

TEST_F(WalEpochActivationHandlerTest,
       RecoveryAndManualReopenBeforeWalActivation) {
  ASSERT_TRUE(coordinator_->PublishRecoveryCheckpoint().ok());
  EXPECT_EQ(handler_calls_.size(), 1u);
  EXPECT_EQ(wal_activation_calls_, 0u);

  neug::VersionManager version_manager;
  version_manager.init_ts({40, 0}, 1);
  neug::UpdateTimestampLease update_lease(version_manager);
  ASSERT_TRUE(
      coordinator_->PublishManualCheckpoint(std::move(update_lease)).ok());

  EXPECT_EQ(wal_activation_calls_, 1u);
  const auto current_checkpoint = checkpoint_manager_.Current();
  ASSERT_NE(current_checkpoint, nullptr);
  EXPECT_EQ(observed_wal_uri_, current_checkpoint->wal_dir());
  ASSERT_EQ(handler_calls_.size(), 2u);
  EXPECT_EQ(handler_calls_[1], current_checkpoint->allocator_dir());
  {
    auto read = version_manager.acquire_read_operation();
    EXPECT_EQ(read.published_view.visibility_ts, 0u);
  }
}

TEST_F(WalEpochActivationHandlerTest,
       ConsumingCheckpointOnCloneDoesNotConsumeDirtyBase) {
  neug::CreateVertexTypeParamBuilder builder;
  ASSERT_TRUE(
      graph_
          ->CreateVertexType(builder.VertexLabel("dirty")
                                 .AddProperty("id", neug::Value::INT64(0))
                                 .AddProperty("value", neug::Value::INT32(0))
                                 .AddPrimaryKeyName("id")
                                 .Build())
          .ok());
  const auto label = graph_->schema().get_vertex_label_id("dirty");
  neug::vid_t vid;
  ASSERT_TRUE(graph_
                  ->AddVertex(label, neug::Value::INT64(1),
                              {neug::Value::INT32(10)}, vid, 1)
                  .ok());
  graph_->MarkSchemaDirty();
  graph_->MarkVertexTableDirty(label);

  const auto* base_timestamp =
      &graph_->get_vertex_table(label).get_vertex_timestamp();
  auto clone = graph_->Clone();
  auto detach_state = neug::CowDetachState::FromSchema(clone->schema());
  auto staging = checkpoint_manager_.CreateStaging();
  clone->DetachDirtyModulesForCheckpoint(detach_state);
  clone->DumpDirtyAndReopen(staging.checkpoint(), 1);

  EXPECT_EQ(&graph_->get_vertex_table(label).get_vertex_timestamp(),
            base_timestamp);
  EXPECT_EQ(graph_->VertexNum(label, 1), 1u);
  neug::GraphView base_view(*graph_);
  neug::StorageReadInterface base_reader(base_view, 1);
  EXPECT_EQ(base_reader.GetVertexProperty(label, vid, 0).GetValue<int32_t>(),
            10);
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
  const auto old_wal_dir =
      (std::filesystem::path(test_dir) / "old-wal").string();
  auto wal_writer = neug::WalWriterFactory::CreateWalWriter(old_wal_dir, 0);
  wal_writer->open(old_wal_dir);
  constexpr uint32_t before_marker = 17;
  constexpr uint32_t after_marker = 29;
  ASSERT_TRUE(wal_writer->append(reinterpret_cast<const char*>(&before_marker),
                                 sizeof(before_marker)));

  neug::CheckpointCoordinator coordinator(
      checkpoint_manager, snapshot_store, neug::MemoryLevel::kInMemory,
      [&](const std::string&) {
        allocators[0]->Reopen(neug::MemoryLevel::kInMemory, "");
        allocator_reopened = true;
      },
      [&](const std::string& wal_uri) {
        wal_writer->close();
        wal_writer->open(wal_uri);
        cache_invalidated = true;
      });

  // Keep the checkpoint manager's only staging slot occupied.
  // PublishManualCheckpoint must fail before destructive graph maintenance,
  // release the update lease normally, and leave the existing WAL writer
  // untouched.
  auto conflicting_staging = checkpoint_manager.CreateStaging();
  neug::VersionManager version_manager;
  version_manager.init_ts({40, 0}, 1);
  neug::UpdateTimestampLease update_lease(version_manager);
  const auto update_ts = update_lease.Timestamp();
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
  auto txn = slot->BeginSnapshotReadTransaction();
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

TEST_F(WalReplayTest,
       CheckpointRotationKeepsTpAndApWalWritersUsableAcrossServiceStop) {
  create_checkpointed_base_graph(db_dir_);

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_, 2)));
    {
      neug::NeugDBService service(db);
      // The pool returns the two distinct logical slots. Exercising both
      // before and after CHECKPOINT proves that RotateActive() reopens the
      // TP-only writer as well as the shared slot-0 writer.
      auto first_slot = service.AcquireExecutionSlot();
      auto second_slot = service.AcquireExecutionSlot();
      EXPECT_EQ(insert_person_and_return_ts(*first_slot, 2, "pre-checkpoint"),
                1);
      EXPECT_EQ(insert_person_and_return_ts(*second_slot, 3, "pre-checkpoint"),
                2);

      checkpoint(*first_slot);

      EXPECT_EQ(insert_person_and_return_ts(*first_slot, 4, "post-checkpoint"),
                1);
      EXPECT_EQ(insert_person_and_return_ts(*second_slot, 5, "post-checkpoint"),
                2);
    }

    auto conn = db.Connect();
    auto update = conn->Query(
        "MATCH (n:person {id: 1}) SET n.name = 'after-tp';", "update");
    ASSERT_TRUE(update) << update.error().ToString();
    conn->Close();
    db.Close();
  }

  // checkpoint_on_close is disabled. Recovery therefore proves that the
  // post-checkpoint TP writes used reopened writers in the new WAL epoch and
  // that the stable direct writer remained usable after the TP pool stopped.
  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    {
      neug::NeugDBService service(db);
      EXPECT_EQ(read_person_name(service, 1),
                std::optional<std::string>("after-tp"));
      EXPECT_EQ(read_person_count(service), 5u);
      EXPECT_TRUE(read_has_person(service, 2));
      EXPECT_TRUE(read_has_person(service, 3));
      EXPECT_TRUE(read_has_person(service, 4));
      EXPECT_TRUE(read_has_person(service, 5));
    }
    db.Close();
  }
}

// A successful COPY FROM must publish a bulk checkpoint before the
// statement returns, so the loaded data survives a crash even though neither
// checkpoint_on_close nor checkpoint_on_recovery is enabled.
TEST_F(WalReplayTest, CopyFromPublishesCheckpointAndSurvivesRecovery) {
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
  // statement-level bulk checkpoint published by the COPY itself.
  const auto checkpoint_id = read_current_checkpoint_id(db_dir_);
  ASSERT_TRUE(checkpoint_id.has_value())
      << "a successful COPY must publish a checkpoint";

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

TEST_F(WalReplayTest, CopyFromWithInferredSchemaCommitsCheckpoint) {
  const auto csv_path =
      (std::filesystem::path(db_dir_) / "inferred.csv").string();
  write_copy_csv(csv_path, "7|inferred\n");

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    auto conn = db.Connect();
    auto copy = conn->Query("COPY inferred_person FROM \"" + csv_path + "\";",
                            "update");
    ASSERT_TRUE(copy) << copy.error().ToString();
    conn->Close();
    db.Close();
  }

  ASSERT_TRUE(read_current_checkpoint_id(db_dir_).has_value());
  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    auto conn = db.Connect();
    auto result = conn->Query("MATCH (n:inferred_person) RETURN n.id, n.name;");
    ASSERT_TRUE(result) << result.error().ToString();
    ASSERT_EQ(result.value().response().row_count(), 1);
    EXPECT_EQ(result.value().response().arrays(0).int64_array().values(0), 7);
    EXPECT_EQ(result.value().response().arrays(1).string_array().values(0),
              "inferred");
    conn->Close();
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
        << "an ordinary WAL-backed write must not trigger a bulk "
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

// A failed COPY discards its private workspace and publishes no checkpoint.
TEST_F(WalReplayTest, FailedCopyDoesNotPublishCheckpoint) {
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
        << "a failed COPY must not publish a checkpoint";
    {
      neug::NeugDBService service(db);
      EXPECT_EQ(read_person_count(service), 1u);
    }
    db.Close();
  }
}

// A checkpoint preparation failure discards the private bulk workspace. The
// previously published graph and checkpoint remain current and usable.
TEST_F(WalReplayTest, CopyFromCheckpointPreparationFailureRollsBack) {
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
    const auto checkpoint_before_failure = read_current_checkpoint_id(db_dir_);
    ASSERT_TRUE(checkpoint_before_failure.has_value());

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
    ASSERT_FALSE(copy_b) << "a checkpoint failure must fail the COPY statement";
    if (!copy_b) {
      EXPECT_NE(copy_b.error().error_code(), neug::StatusCode::OK);
    }
    conn2->Close();

    EXPECT_EQ(read_current_checkpoint_id(db_dir_), checkpoint_before_failure);

    // The failed private workspace was discarded; AP readers still see only
    // the previously published COPY.
    auto read_conn = db.Connect();
    auto count = read_conn->Query("MATCH (n:person) RETURN count(n);", "read");
    ASSERT_TRUE(count) << count.error().ToString();
    ASSERT_EQ(count->response().arrays_size(), 1);
    ASSERT_EQ(count->response().arrays(0).int64_array().values_size(), 1);
    EXPECT_EQ(count->response().arrays(0).int64_array().values(0), 1);
    read_conn->Close();

    // Clearing the fault is sufficient; the failed private workspace left no
    // residual mutation to seal before TP mode starts.
    std::filesystem::remove(runtime_dir);
    std::filesystem::create_directories(runtime_dir);
    {
      neug::NeugDBService service(db);
      EXPECT_EQ(read_person_count(service), 1u);
      EXPECT_TRUE(read_has_person(service, 2));
      EXPECT_FALSE(read_has_person(service, 3));
    }
    db.Close();
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    {
      neug::NeugDBService service(db);
      EXPECT_EQ(read_person_count(service), 1u);
      EXPECT_TRUE(read_has_person(service, 2));
      EXPECT_FALSE(read_has_person(service, 3));
    }
    db.Close();
  }
}

// A second COPY to the same table rewrites modules already present in the
// previous checkpoint. Both completed batches must survive recovery.
TEST_F(WalReplayTest, RepeatedCopyToSameTableRecovers) {
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
    auto copy_b = conn->Query("COPY person FROM \"" + csv_b + "\";", "update");
    ASSERT_TRUE(copy_b) << copy_b.error().ToString();
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
                std::optional<std::string>("copy-a"));
      EXPECT_EQ(read_person_name(service, 3),
                std::optional<std::string>("copy-b"));
    }
    db.Close();
  }
}
