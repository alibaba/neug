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

#include <unistd.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
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

void write_person_csv(const std::filesystem::path& path,
                      std::string_view rows) {
  std::ofstream csv(path);
  csv << "id|name\n" << rows;
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
  auto txn = slot->BeginInsertTransaction();
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
  auto txn = slot->BeginCompactTransaction();
  ASSERT_TRUE(txn.Commit());
}

void insert_knows_edge(neug::NeugDBService& service, int64_t src_id,
                       int64_t dst_id, int64_t since) {
  auto slot = service.AcquireExecutionSlot();
  auto txn = slot->BeginInsertTransaction();
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
  auto txn = slot->BeginReadTransaction();
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
  auto txn = slot->BeginReadTransaction();
  neug::StorageReadInterface graph(txn.view(), txn.timestamp());
  const auto person_label = graph.schema().get_vertex_label_id("person");
  neug::vid_t vid = 0;
  bool found = graph.GetVertexIndex(person_label, Value::INT64(id), vid);
  EXPECT_TRUE(txn.Commit());
  return found;
}

void expect_embedded_name(neug::NeugDB& db, int64_t id,
                          const std::string& expected_name) {
  auto conn = db.Connect();
  auto result = conn->Query("MATCH (n:person {id: " + std::to_string(id) +
                            "}) RETURN n.name;");
  ASSERT_TRUE(result) << result.error().ToString();
  const auto& response = result.value().response();
  ASSERT_EQ(response.row_count(), 1);
  ASSERT_EQ(response.arrays_size(), 1);
  ASSERT_EQ(response.arrays(0).string_array().values_size(), 1);
  EXPECT_EQ(response.arrays(0).string_array().values(0), expected_name);
  conn->Close();
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

class CheckpointActivationHandlerTest : public ::testing::Test {
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
        },
        [this](const std::string& wal_dir) {
          wal_rotation_calls_.push_back(wal_dir);
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
  std::vector<std::string> wal_rotation_calls_;
};

TEST_F(CheckpointActivationHandlerTest,
       RecoveryAndManualRunOnlyTheirConfiguredHandlers) {
  size_t activation_calls = 0;
  std::string observed_wal_uri;
  coordinator_->SetActivationHandler([&](const std::string& wal_uri) {
    // The mandatory post-reopen handler always runs first.
    EXPECT_EQ(handler_calls_.size(), 2u);
    ++activation_calls;
    observed_wal_uri = wal_uri;
  });

  ASSERT_TRUE(coordinator_->PublishRecoveryCheckpoint().ok());
  EXPECT_EQ(handler_calls_.size(), 1u);
  EXPECT_EQ(wal_rotation_calls_.size(), 1u);
  EXPECT_EQ(activation_calls, 0u);

  neug::VersionManager version_manager;
  version_manager.init_ts({0, 0}, 1);
  neug::UpdateTimestampLease update_lease(version_manager);
  ASSERT_TRUE(
      coordinator_->PublishManualCheckpoint(std::move(update_lease)).ok());

  EXPECT_EQ(activation_calls, 1u);
  const auto current_checkpoint = checkpoint_manager_.CurrentCheckpoint();
  ASSERT_NE(current_checkpoint, nullptr);
  EXPECT_EQ(observed_wal_uri, current_checkpoint->wal_dir());
  ASSERT_EQ(handler_calls_.size(), 2u);
  EXPECT_EQ(handler_calls_[1], current_checkpoint->allocator_dir());
  ASSERT_EQ(wal_rotation_calls_.size(), 2u);
  EXPECT_EQ(wal_rotation_calls_[1], current_checkpoint->wal_dir());
}

TEST_F(CheckpointActivationHandlerTest,
       PublishedCheckpointClearsUnloggedMutationBarrier) {
  EXPECT_FALSE(coordinator_->UnloggedMutationPending());
  coordinator_->MarkUnloggedMutation();
  EXPECT_TRUE(coordinator_->UnloggedMutationPending());

  neug::VersionManager version_manager;
  version_manager.init_ts({0, 0}, 1);
  ASSERT_TRUE(
      coordinator_
          ->PublishManualCheckpoint(neug::UpdateTimestampLease(version_manager))
          .ok());

  EXPECT_FALSE(coordinator_->UnloggedMutationPending());
}

TEST_F(CheckpointActivationHandlerTest,
       IncrementalCheckpointKeepsGraphAllocatorAndTimestampTimeline) {
  neug::VersionManager version_manager;
  version_manager.init_ts({40, 0}, 1);
  neug::UpdateTimestampLease update_lease(version_manager);
  const auto update_ts = update_lease.Timestamp();
  update_lease.MakeUpdateExclusive();
  const neug::PropertyGraph* graph_before = nullptr;
  {
    neug::SnapshotGuard snapshot(*snapshot_store_);
    graph_before = &snapshot.get().graph();
  }

  coordinator_->MarkUnloggedMutation();
  ASSERT_TRUE(
      coordinator_->PublishIncrementalCheckpoint(std::move(update_lease)).ok());

  {
    neug::SnapshotGuard snapshot(*snapshot_store_);
    EXPECT_EQ(&snapshot.get().graph(), graph_before);
  }
  EXPECT_TRUE(handler_calls_.empty());
  ASSERT_EQ(wal_rotation_calls_.size(), 1u);
  EXPECT_EQ(wal_rotation_calls_[0], graph_->checkpoint().wal_dir());
  EXPECT_FALSE(coordinator_->UnloggedMutationPending());
  {
    auto read = version_manager.acquire_read_operation();
    EXPECT_EQ(read.published_view.visibility_ts, update_ts);
  }
  const auto next_insert_ts = version_manager.acquire_insert_timestamp();
  EXPECT_EQ(next_insert_ts, update_ts + 1);
  version_manager.release_insert_timestamp(next_insert_ts);
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
      },
      [](const std::string&) {});

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
  auto conflicting_staging = checkpoint_manager.CreateStagingCheckpoint();
  neug::VersionManager version_manager;
  version_manager.init_ts({40, 0}, 1);
  neug::UpdateTimestampLease update_lease(version_manager);
  const auto update_ts = update_lease.Timestamp();
  coordinator.SetActivationHandler([&](const std::string& wal_uri) {
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
  coordinator.ClearActivationHandler();
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
          << "the schema DDL and TP insert share one WAL timeline";
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

TEST_F(WalReplayTest, EmbeddedOrdinaryCowWritesReplayWithoutCheckpoint) {
  create_checkpointed_base_graph(db_dir_);

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    uint32_t generation_before = 0;
    {
      neug::SnapshotGuard snapshot(db.graph_snapshot_store());
      generation_before = snapshot.get().snapshot_generation();
    }
    auto conn = db.Connect();
    assert_query_ok(*conn, "CREATE (:person {id: 2, name: 'inserted'});");
    assert_query_ok(
        *conn,
        "MATCH (n:person {id: 2}) SET n.name = 'updated' RETURN n.name;");
    conn->Close();
    {
      neug::SnapshotGuard snapshot(db.graph_snapshot_store());
      EXPECT_EQ(snapshot.get().snapshot_generation(), generation_before)
          << "AP current-slot replacement must not publish a TP generation";
    }
    db.Close();
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    expect_embedded_name(db, 2, "updated");
    db.Close();
  }
}

TEST_F(WalReplayTest, DefaultVertexAndEdgeCopyPublishCheckpointWithoutWal) {
  const auto person_csv = std::filesystem::path(db_dir_) / "bulk-person.csv";
  const auto edge_csv = std::filesystem::path(db_dir_) / "bulk-knows.csv";
  write_person_csv(person_csv, "1|seed\n2|bulk\n");
  std::ofstream(edge_csv) << "from|to|since\n1|2|2026\n";

  {
    neug::NeugDB db;
    auto config = make_config(db_dir_);
    ASSERT_FALSE(config.checkpoint_on_close);
    ASSERT_TRUE(db.Open(config));
    create_person_schema(db);
    auto conn = db.Connect();
    assert_query_ok(
        *conn, "CREATE REL TABLE knows(FROM person TO person, since INT64);");
    assert_query_ok(*conn, "COPY person FROM \"" + person_csv.string() + "\";");
    EXPECT_TRUE(std::filesystem::exists(std::filesystem::path(db_dir_) /
                                        "checkpoint-1"));
    assert_query_ok(*conn, "COPY knows FROM \"" + edge_csv.string() +
                               "\" (from=\"person\", to=\"person\");");
    EXPECT_TRUE(std::filesystem::exists(std::filesystem::path(db_dir_) /
                                        "checkpoint-2"));
    auto parser = neug::WalParserFactory::CreateWalParser(
        db.graph().checkpoint().wal_dir());
    EXPECT_EQ(parser->last_ts(), 0U);
    EXPECT_TRUE(parser->get_update_wals().empty());
    conn->Close();
    db.Close();
  }

  {
    neug::NeugDB db;
    ASSERT_TRUE(db.Open(make_config(db_dir_)));
    EXPECT_TRUE(replayed_graph_matches(db));
    db.Close();
  }
}

TEST_F(WalReplayTest, ReadOnlyBatchDoesNotPublishCheckpointOrLeaveBarrier) {
  const auto input = std::filesystem::path(db_dir_) / "input.csv";
  const auto output = std::filesystem::path(db_dir_) / "output.csv";
  write_person_csv(input, "2|loaded\n");

  neug::NeugDB db;
  ASSERT_TRUE(db.Open(make_config(db_dir_)));
  create_person_schema(db);
  auto planning_generation = [&]() {
    neug::SnapshotGuard snapshot(db.graph_snapshot_store());
    return snapshot.get().planning_generation();
  };
  const auto generation_before = planning_generation();

  auto conn = db.Connect();
  assert_query_ok(*conn, "COPY (MATCH (n:person) RETURN n.*) TO '" +
                             output.string() + "' (header=true);");
  assert_query_ok(*conn, "LOAD FROM '" + input.string() +
                             "' (header=true, delim='|') RETURN *;");
  EXPECT_EQ(planning_generation(), generation_before);
  EXPECT_FALSE(
      std::filesystem::exists(std::filesystem::path(db_dir_) / "checkpoint-1"));

  assert_query_ok(*conn, "CREATE (:person {id: 3, name: 'wal'});");
  EXPECT_FALSE(
      std::filesystem::exists(std::filesystem::path(db_dir_) / "checkpoint-1"));
  conn->Close();
  db.Close();
}

TEST_F(WalReplayTest, ConsecutiveCopiesPublishIndependentCheckpoints) {
  const auto first = std::filesystem::path(db_dir_) / "first.csv";
  const auto second = std::filesystem::path(db_dir_) / "second.csv";
  write_person_csv(first, "2|first\n");
  write_person_csv(second, "3|second\n");

  neug::NeugDB db;
  ASSERT_TRUE(db.Open(make_config(db_dir_)));
  create_person_schema(db);
  auto conn = db.Connect();
  assert_query_ok(*conn, "COPY person FROM \"" + first.string() + "\";");
  EXPECT_TRUE(
      std::filesystem::exists(std::filesystem::path(db_dir_) / "checkpoint-1"));
  assert_query_ok(*conn, "COPY person FROM \"" + second.string() + "\";");
  EXPECT_TRUE(
      std::filesystem::exists(std::filesystem::path(db_dir_) / "checkpoint-2"));

  assert_query_ok(*conn, "CREATE (:person {id: 4, name: 'wal'});");
  EXPECT_FALSE(
      std::filesystem::exists(std::filesystem::path(db_dir_) / "checkpoint-3"));
  conn->Close();
  db.Close();

  neug::NeugDB reopened;
  ASSERT_TRUE(reopened.Open(make_config(db_dir_)));
  expect_embedded_name(reopened, 2, "first");
  expect_embedded_name(reopened, 3, "second");
  expect_embedded_name(reopened, 4, "wal");
  reopened.Close();
}

TEST_F(WalReplayTest, FailedCopyAllowsReadsAndAnotherCopy) {
  const auto failed_csv = std::filesystem::path(db_dir_) / "failed.csv";
  const auto next_csv = std::filesystem::path(db_dir_) / "next.csv";
  write_person_csv(failed_csv, "2|partial\ninvalid|bad\n");
  write_person_csv(next_csv, "3|next\n");

  neug::NeugDB db;
  ASSERT_TRUE(db.Open(make_config(db_dir_)));
  create_person_schema(db);
  auto conn = db.Connect();
  auto failed =
      conn->Query("COPY person FROM \"" + failed_csv.string() + "\";");
  ASSERT_FALSE(failed);
  auto read = conn->Query("MATCH (n:person) RETURN count(n);");
  ASSERT_TRUE(read) << read.error().ToString();
  assert_query_ok(*conn, "COPY person FROM \"" + next_csv.string() + "\";");
  conn->Close();
  expect_embedded_name(db, 3, "next");
  db.Close();
}

TEST_F(WalReplayTest, FailedCopyCloseCheckpointFailureIsReported) {
  const auto csv_path = std::filesystem::path(db_dir_) / "failed-person.csv";
  write_person_csv(csv_path, "2|partial\ninvalid|bad\n");

  neug::NeugDB db;
  auto config = make_config(db_dir_);
  ASSERT_FALSE(config.checkpoint_on_close);
  ASSERT_TRUE(db.Open(config));
  create_person_schema(db);
  auto conn = db.Connect();
  EXPECT_FALSE(conn->Query("COPY person FROM \"" + csv_path.string() + "\";"));
  conn->Close();

  const auto checkpoint_blocker =
      std::filesystem::path(db_dir_) / "checkpoint-1";
  std::filesystem::create_directories(checkpoint_blocker);
  std::ofstream(checkpoint_blocker / "blocker") << "not a checkpoint";

  EXPECT_THROW(db.Close(), std::exception);
  EXPECT_TRUE(db.IsClosed());
}

TEST_F(WalReplayTest, FailedEmbeddedCowStatementKeepsCurrentGenerationAndData) {
  create_checkpointed_base_graph(db_dir_);

  neug::NeugDB db;
  ASSERT_TRUE(db.Open(make_config(db_dir_)));
  uint32_t generation_before = 0;
  const neug::PropertyGraph* graph_before = nullptr;
  {
    neug::SnapshotGuard snapshot(db.graph_snapshot_store());
    generation_before = snapshot.get().snapshot_generation();
    graph_before = &snapshot.get().graph();
  }

  auto conn = db.Connect();
  auto failed = conn->Query("CREATE (:person {id: 1, name: 'duplicate'});");
  ASSERT_FALSE(failed);
  conn->Close();

  {
    neug::SnapshotGuard snapshot(db.graph_snapshot_store());
    EXPECT_EQ(snapshot.get().snapshot_generation(), generation_before);
    EXPECT_EQ(&snapshot.get().graph(), graph_before)
        << "a failed private-COW statement must not replace the current graph";
  }
  expect_embedded_name(db, 1, "seed");

  {
    neug::NeugDBService service(db);
    auto parser = neug::WalParserFactory::CreateWalParser(
        db.graph().checkpoint().wal_dir());
    EXPECT_TRUE(parser->get_update_wals().empty())
        << "an aborted AP COW statement must not append logical redo";
    EXPECT_EQ(insert_person_and_return_ts(service, 2, "after-abort"), 2)
        << "abort must complete its reserved timestamp and reopen admission";
    EXPECT_EQ(read_person_count(service), 2);
  }
  db.Close();
}

TEST_F(WalReplayTest, EmbeddedCowSharesOneWalAndVisibilityTimestamp) {
  neug::NeugDB db;
  ASSERT_TRUE(db.Open(make_config(db_dir_)));
  create_person_schema(db);

  auto conn = db.Connect();
  assert_query_ok(*conn, "CREATE (:person {id: 1, name: 'ap'});");
  conn->Close();

  {
    neug::NeugDBService service(db);
    auto parser = neug::WalParserFactory::CreateWalParser(
        db.graph().checkpoint().wal_dir());
    const auto& update_wals = parser->get_update_wals();
    ASSERT_EQ(update_wals.size(), 2U);
    const auto ap_timestamp = update_wals.back().timestamp;
    EXPECT_EQ(ap_timestamp, 2U);

    {
      neug::SnapshotGuard snapshot(db.graph_snapshot_store());
      const auto person_label =
          snapshot.get().view().schema().get_vertex_label_id("person");
      neug::vid_t vid = 0;
      neug::StorageReadInterface before(snapshot.get().view(),
                                        ap_timestamp - 1);
      EXPECT_FALSE(before.GetVertexIndex(person_label, Value::INT64(1), vid));
      neug::StorageReadInterface at_commit(snapshot.get().view(), ap_timestamp);
      EXPECT_TRUE(at_commit.GetVertexIndex(person_label, Value::INT64(1), vid));
    }

    auto slot = service.AcquireExecutionSlot();
    auto read_txn = slot->BeginReadTransaction();
    EXPECT_EQ(read_txn.timestamp(), ap_timestamp)
        << "TP read admission must publish the same timestamp serialized in "
           "the AP WAL header";
    neug::StorageReadInterface graph(read_txn.view(), read_txn.timestamp());
    const auto person_label = graph.schema().get_vertex_label_id("person");
    neug::vid_t vid = 0;
    EXPECT_TRUE(graph.GetVertexIndex(person_label, Value::INT64(1), vid));
    EXPECT_TRUE(read_txn.Commit());
  }
  db.Close();
}

TEST_F(WalReplayTest, ServiceBoundaryPreservesApTpWalTimeline) {
  neug::NeugDB db;
  ASSERT_TRUE(db.Open(make_config(db_dir_)));
  create_person_schema(db);

  {
    neug::NeugDBService service(db);
    EXPECT_EQ(insert_person_and_return_ts(service, 1, "tp"), 2);
  }

  auto conn = db.Connect();
  assert_query_ok(*conn,
                  "MATCH (n:person {id: 1}) SET n.name = 'ap' RETURN n.name;");
  conn->Close();

  {
    neug::NeugDBService service(db);
    EXPECT_EQ(insert_person_and_return_ts(service, 2, "tp-again"), 4)
        << "schema, TP insert, AP update, and the next TP insert must use one "
           "monotonic WAL timeline";
  }
  db.Close();

  neug::NeugDB reopened;
  ASSERT_TRUE(reopened.Open(make_config(db_dir_)));
  expect_embedded_name(reopened, 1, "ap");
  expect_embedded_name(reopened, 2, "tp-again");
  reopened.Close();
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
