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

#include <chrono>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "gtest/gtest.h"

#include "neug/common/types/value.h"
#include "neug/storages/allocators.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/graph/operation_params.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/compact_transaction.h"
#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/dummy_wal_writer.h"
#include "unittest/utils.h"

namespace neug {
namespace {

enum class TransactionKind { kRead, kInsert, kCompact };

struct ReleasePath {
  const char* name;
  TransactionKind transaction_kind;
  bool commit;
  bool add_vertex;
};

class ReleaseOrderVersionManager : public IVersionManager {
 public:
  explicit ReleaseOrderVersionManager(GraphSnapshotStore& store)
      : store_(store) {}

  void init_ts(uint32_t, int) override {}
  bool try_set_runtime_wait_if_quiescent(RuntimeWaitFn) noexcept override {
    return true;
  }
  uint32_t acquire_read_timestamp() override { return 1; }
  uint32_t acquire_insert_timestamp() override { return 1; }
  uint32_t acquire_update_timestamp() override { return 1; }
  void begin_update_commit(uint32_t) override {}
  void drain_readers() override {}
  void release_update_timestamp(uint32_t) override {}
  uint32_t acquire_compact_timestamp() override { return 1; }

  void release_read_timestamp() override { record_release(); }
  void release_insert_timestamp(uint32_t) override { record_release(); }
  void release_compact_timestamp(uint32_t) override { record_release(); }
  void revert_compact_timestamp(uint32_t) override { record_release(); }

  bool snapshot_was_released_first() const {
    return snapshot_was_released_first_;
  }

  int release_count() const { return release_count_; }

 private:
  void record_release() {
    ++release_count_;
    if (release_count_ == 1) {
      snapshot_was_released_first_ = store_.HasFreeSlot();
    }
  }

  GraphSnapshotStore& store_;
  bool snapshot_was_released_first_{false};
  int release_count_{0};
};

class TransactionReleaseOrderTest
    : public ::testing::TestWithParam<ReleasePath> {
 protected:
  void SetUp() override {
    work_dir_ =
        (std::filesystem::temp_directory_path() /
         ("test_transaction_release_order_" + std::string(GetParam().name)))
            .string();
    std::filesystem::remove_all(work_dir_);
    std::filesystem::create_directories(work_dir_);

    checkpoint_manager_.Open(work_dir_);
    initial_graph_ = std::make_shared<PropertyGraph>();
    initial_graph_->Open(make_checkpoint(checkpoint_manager_),
                         MemoryLevel::kInMemory);

    CreateVertexTypeParamBuilder person_builder;
    ASSERT_TRUE(initial_graph_
                    ->CreateVertexType(person_builder.VertexLabel("person")
                                           .AddProperty("id", Value::INT64(0))
                                           .AddPrimaryKeyName("id")
                                           .Build())
                    .ok());

    store_ = std::make_unique<GraphSnapshotStore>(2, initial_graph_);
  }

  void TearDown() override {
    store_.reset();
    initial_graph_.reset();
    std::filesystem::remove_all(work_dir_);
  }

  void publish_replacement_snapshot() {
    ASSERT_TRUE(store_->PublishSnapshot(initial_graph_->Clone()).ok());
    ASSERT_FALSE(store_->HasFreeSlot())
        << "The transaction-owned pin must keep the stale slot occupied";
  }

  std::string work_dir_;
  CheckpointManager checkpoint_manager_;
  std::shared_ptr<PropertyGraph> initial_graph_;
  std::unique_ptr<GraphSnapshotStore> store_;
  Allocator allocator_{MemoryLevel::kInMemory, ""};
  DummyWalWriter wal_writer_;
};

TEST_P(TransactionReleaseOrderTest, ReleasesSnapshotBeforeTimestamp) {
  ReleaseOrderVersionManager version_manager(*store_);
  const auto& path = GetParam();
  switch (path.transaction_kind) {
  case TransactionKind::kRead: {
    SnapshotGuard guard(*store_);
    ReadTransaction transaction(std::move(guard), version_manager, 1);
    publish_replacement_snapshot();
    ASSERT_TRUE(transaction.Commit());
    break;
  }
  case TransactionKind::kInsert: {
    SnapshotGuard guard(*store_);
    InsertTransaction transaction(std::move(guard), allocator_, wal_writer_,
                                  version_manager, 1);
    if (path.add_vertex) {
      vid_t vertex_id;
      ASSERT_TRUE(
          transaction.AddVertex(0, Value::INT64(1), {}, vertex_id).ok());
    }
    publish_replacement_snapshot();
    if (path.commit) {
      ASSERT_TRUE(transaction.Commit());
    } else {
      transaction.Abort();
    }
    break;
  }
  case TransactionKind::kCompact: {
    CompactTransaction transaction(*store_, wal_writer_, version_manager, 1);
    publish_replacement_snapshot();
    if (path.commit) {
      ASSERT_TRUE(transaction.Commit());
    } else {
      transaction.Abort();
    }
    break;
  }
  }

  EXPECT_EQ(version_manager.release_count(), 1);
  EXPECT_TRUE(version_manager.snapshot_was_released_first())
      << "The snapshot pin must be released before the timestamp lease";
}

TEST(APInPlaceConcurrencyTest, ExistingReaderBlocksWriterMutationPhase) {
  VersionManager version_manager;
  version_manager.init_ts(0, 2);

  version_manager.acquire_read_timestamp();

  std::promise<void> entered_commit;
  std::promise<void> drained;
  auto entered_commit_future = entered_commit.get_future();
  auto drained_future = drained.get_future();
  std::thread writer([&]() {
    const auto timestamp = version_manager.acquire_update_timestamp();
    version_manager.begin_update_commit(timestamp);
    entered_commit.set_value();
    version_manager.drain_readers();
    drained.set_value();
    version_manager.release_update_timestamp(timestamp);
  });

  entered_commit_future.wait();
  EXPECT_EQ(drained_future.wait_for(std::chrono::milliseconds(20)),
            std::future_status::timeout);

  version_manager.release_read_timestamp();
  EXPECT_EQ(drained_future.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
  writer.join();
}

TEST(APInPlaceConcurrencyTest, WriterBlocksNewReadersUntilReleased) {
  VersionManager version_manager;
  version_manager.init_ts(0, 2);

  const auto writer_timestamp = version_manager.acquire_update_timestamp();
  version_manager.begin_update_commit(writer_timestamp);

  std::promise<void> attempting_read;
  std::promise<timestamp_t> acquired_read;
  auto attempting_read_future = attempting_read.get_future();
  auto acquired_read_future = acquired_read.get_future();
  std::thread reader([&]() {
    attempting_read.set_value();
    const auto timestamp = version_manager.acquire_read_timestamp();
    acquired_read.set_value(timestamp);
    version_manager.release_read_timestamp();
  });

  attempting_read_future.wait();
  EXPECT_EQ(acquired_read_future.wait_for(std::chrono::milliseconds(20)),
            std::future_status::timeout);

  version_manager.release_update_timestamp(writer_timestamp);
  EXPECT_EQ(acquired_read_future.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
  reader.join();
}

TEST(APInPlaceConcurrencyTest, WritersAreSerialized) {
  VersionManager version_manager;
  version_manager.init_ts(0, 2);

  const auto first_timestamp = version_manager.acquire_update_timestamp();

  std::promise<void> attempting_update;
  std::promise<timestamp_t> acquired_update;
  auto attempting_update_future = attempting_update.get_future();
  auto acquired_update_future = acquired_update.get_future();
  std::thread second_writer([&]() {
    attempting_update.set_value();
    const auto timestamp = version_manager.acquire_update_timestamp();
    acquired_update.set_value(timestamp);
    version_manager.release_update_timestamp(timestamp);
  });

  attempting_update_future.wait();
  EXPECT_EQ(acquired_update_future.wait_for(std::chrono::milliseconds(20)),
            std::future_status::timeout);

  version_manager.release_update_timestamp(first_timestamp);
  EXPECT_EQ(acquired_update_future.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
  second_writer.join();
  EXPECT_GT(acquired_update_future.get(), first_timestamp);
}

std::string release_path_name(
    const ::testing::TestParamInfo<ReleasePath>& info) {
  return info.param.name;
}

INSTANTIATE_TEST_SUITE_P(
    TransactionPaths, TransactionReleaseOrderTest,
    ::testing::Values(
        ReleasePath{"Read", TransactionKind::kRead, true, false},
        ReleasePath{"EmptyInsertCommit", TransactionKind::kInsert, true, false},
        ReleasePath{"InsertCommit", TransactionKind::kInsert, true, true},
        ReleasePath{"InsertAbort", TransactionKind::kInsert, false, false},
        ReleasePath{"CompactCommit", TransactionKind::kCompact, true, false},
        ReleasePath{"CompactAbort", TransactionKind::kCompact, false, false}),
    release_path_name);

}  // namespace
}  // namespace neug
