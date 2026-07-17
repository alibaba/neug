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

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/graph/operation_params.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/read_snapshot_lease.h"
#include "neug/transaction/timestamp_lease.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/transaction/version_manager.h"
#include "unittest/utils.h"

namespace neug {
namespace {

constexpr uint64_t kReplacementPlanningGeneration = 1;

uint64_t ReadPlanningGeneration(GraphSnapshotStore& store) {
  SnapshotGuard current(store);
  return current.get().planning_generation();
}

result<uint32_t> PrepareAndPublishSnapshot(
    GraphSnapshotStore& store, const std::shared_ptr<PropertyGraph>& snapshot,
    uint64_t planning_generation) {
  auto prepared_result = store.PrepareSnapshot(snapshot, planning_generation);
  if (!prepared_result) {
    return tl::unexpected(prepared_result.error());
  }
  auto prepared = std::move(prepared_result).value();
  return std::move(prepared).Publish();
}

std::atomic<int> runtime_wait_calls{0};

void CountRuntimeWait(RuntimeWaitAction) noexcept {
  runtime_wait_calls.fetch_add(1, std::memory_order_relaxed);
}

class ScriptedVersionManager : public IVersionManager {
 public:
  explicit ScriptedVersionManager(PublishedReadView initial)
      : published_(PackPublishedReadView(initial)) {}

  void set_acquire_hook(std::function<void(int)> hook) {
    acquire_hook_ = std::move(hook);
  }

  void set_runtime_wait(RuntimeWaitFn runtime_wait) {
    runtime_wait_ = runtime_wait;
  }

  void publish(PublishedReadView view) {
    published_.store(PackPublishedReadView(view), std::memory_order_release);
  }

  int acquire_count() const { return acquire_count_.load(); }
  int release_count() const { return release_count_.load(); }

  void init_ts(PublishedReadView, int) override {}
  bool try_set_runtime_wait_if_quiescent(RuntimeWaitFn) noexcept override {
    return true;
  }

  PublishedReadView acquire_read_view() override {
    const auto captured =
        UnpackPublishedReadView(published_.load(std::memory_order_acquire));
    const int acquire_count = acquire_count_.fetch_add(1) + 1;
    if (acquire_hook_) {
      acquire_hook_(acquire_count);
    }
    return captured;
  }

  void release_read_view() override { release_count_.fetch_add(1); }
  uint32_t acquire_insert_timestamp() override { return 1; }
  void release_insert_timestamp(uint32_t) override {}
  uint32_t acquire_update_timestamp() override { return 1; }
  void begin_update_commit(uint32_t) override {}
  void drain_readers() override {}
  void finish_update_timestamp(uint32_t,
                               std::optional<uint32_t>) noexcept override {}
  void finish_update_and_reset_timeline(uint32_t) noexcept override {}
  uint32_t acquire_compact_timestamp() override { return 1; }
  void release_compact_timestamp(uint32_t) override {}
  void revert_compact_timestamp(uint32_t) override {}

 private:
  RuntimeWaitFn runtime_wait_impl() const noexcept override {
    return runtime_wait_;
  }

  std::atomic<uint64_t> published_;
  std::atomic<int> acquire_count_{0};
  std::atomic<int> release_count_{0};
  std::function<void(int)> acquire_hook_;
  RuntimeWaitFn runtime_wait_{&NativeRuntimeWait};
};

class ReadViewPublicationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    work_dir_ =
        (std::filesystem::temp_directory_path() /
         ("test_read_view_publication_" +
          std::string(
              ::testing::UnitTest::GetInstance()->current_test_info()->name())))
            .string();
    std::filesystem::remove_all(work_dir_);
    std::filesystem::create_directories(work_dir_);

    checkpoint_manager_.Open(work_dir_);
    initial_graph_ = std::make_shared<PropertyGraph>();
    initial_graph_->Open(make_checkpoint(checkpoint_manager_),
                         MemoryLevel::kInMemory);
    CreateVertexTypeParamBuilder person;
    ASSERT_TRUE(initial_graph_
                    ->CreateVertexType(person.VertexLabel("person")
                                           .AddProperty("id", Value::INT64(0))
                                           .AddPrimaryKeyName("id")
                                           .Build())
                    .ok());
    store_ = std::make_unique<GraphSnapshotStore>(4, initial_graph_);
  }

  void TearDown() override {
    store_.reset();
    initial_graph_.reset();
    std::filesystem::remove_all(work_dir_);
  }

  std::shared_ptr<PropertyGraph> MakeReplacement() {
    auto replacement = initial_graph_->Clone();
    CreateVertexTypeParamBuilder company;
    auto status =
        replacement->CreateVertexType(company.VertexLabel("company")
                                          .AddProperty("id", Value::INT64(0))
                                          .AddPrimaryKeyName("id")
                                          .Build());
    if (!status.ok()) {
      ADD_FAILURE() << "Failed to create replacement marker schema: "
                    << status.error_message();
    }
    return replacement;
  }

  uint32_t PublishReplacement(VersionManager& version_manager) {
    const uint32_t timestamp = version_manager.acquire_update_timestamp();
    auto replacement = MakeReplacement();
    auto prepared_result =
        store_->PrepareSnapshot(replacement, kReplacementPlanningGeneration);
    EXPECT_TRUE(prepared_result.has_value());
    auto prepared = std::move(prepared_result).value();
    version_manager.begin_update_commit(timestamp);
    const uint32_t snapshot_generation = std::move(prepared).Publish();
    version_manager.finish_update_timestamp(timestamp, snapshot_generation);
    return timestamp;
  }

  std::string work_dir_;
  CheckpointManager checkpoint_manager_;
  std::shared_ptr<PropertyGraph> initial_graph_;
  std::unique_ptr<GraphSnapshotStore> store_;
};

TEST_F(ReadViewPublicationTest, SplitAcquisitionExposesGenerationMismatch) {
  VersionManager version_manager;
  version_manager.init_ts({1, 0}, 2);

  const PublishedReadView old_view = version_manager.acquire_read_view();
  PublishReplacement(version_manager);

  SnapshotGuard current(*store_);
  EXPECT_EQ(old_view.visibility_ts, 1u);
  EXPECT_EQ(old_view.snapshot_generation, 0u);
  EXPECT_TRUE(current.get().view().schema().is_vertex_label_valid("company"));
  EXPECT_NE(current.get().snapshot_generation(), old_view.snapshot_generation);

  current.release();
  version_manager.release_read_view();
}

TEST_F(ReadViewPublicationTest, ValidatedReaderKeepsPinnedOldSnapshot) {
  VersionManager version_manager;
  version_manager.init_ts({1, 0}, 2);

  auto lease = ReadSnapshotLease::Acquire(version_manager, *store_);
  const auto timestamp = PublishReplacement(version_manager);

  EXPECT_EQ(lease.timestamp(), 1u);
  EXPECT_FALSE(lease.view().schema().is_vertex_label_valid("company"));

  lease.release();
  auto next = ReadSnapshotLease::Acquire(version_manager, *store_);
  EXPECT_EQ(next.timestamp(), timestamp);
  EXPECT_TRUE(next.view().schema().is_vertex_label_valid("company"));
}

TEST_F(ReadViewPublicationTest,
       InitialReadViewMatchesNonzeroSnapshotGeneration) {
  constexpr uint32_t kInitialTimestamp = 7;
  constexpr uint32_t kInitialSnapshotGeneration = 19;
  store_ = std::make_unique<GraphSnapshotStore>(4, initial_graph_,
                                                kInitialSnapshotGeneration);

  VersionManager version_manager;
  version_manager.init_ts({kInitialTimestamp, kInitialSnapshotGeneration}, 2);

  const PublishedReadView initialized = version_manager.acquire_read_view();
  version_manager.release_read_view();
  ASSERT_EQ(initialized.visibility_ts, kInitialTimestamp);
  ASSERT_EQ(initialized.snapshot_generation, kInitialSnapshotGeneration);

  auto lease = ReadSnapshotLease::Acquire(version_manager, *store_);
  EXPECT_EQ(lease.timestamp(), kInitialTimestamp);
  EXPECT_FALSE(lease.view().schema().is_vertex_label_valid("company"));
}

TEST_F(ReadViewPublicationTest,
       InPlaceWriteScopePublishesPlanningGenerationOnCurrentSnapshot) {
  VersionManager version_manager;
  version_manager.init_ts({1, 0}, 2);

  const auto initial_planning_generation = ReadPlanningGeneration(*store_);
  uint32_t initial_snapshot_generation = 0;
  {
    SnapshotGuard current(*store_);
    initial_snapshot_generation = current.get().snapshot_generation();
  }

  uint32_t committed_timestamp = 0;
  {
    InPlaceWriteScope write_scope(version_manager, *store_);
    committed_timestamp = write_scope.Timestamp();
    write_scope.MarkPlanningChanged();
  }

  {
    SnapshotGuard current(*store_);
    EXPECT_EQ(current.get().snapshot_generation(), initial_snapshot_generation);
    EXPECT_EQ(current.get().planning_generation(),
              initial_planning_generation + 1);
  }

  auto reader = ReadSnapshotLease::Acquire(version_manager, *store_);
  EXPECT_EQ(reader.timestamp(), committed_timestamp);
  EXPECT_EQ(reader.planning_generation(), initial_planning_generation + 1);
}

TEST_F(ReadViewPublicationTest,
       InPlaceWriteScopeWithoutPlanningChangeKeepsGeneration) {
  VersionManager version_manager;
  version_manager.init_ts({1, 0}, 2);

  const auto initial_planning_generation = ReadPlanningGeneration(*store_);
  uint32_t committed_timestamp = 0;
  {
    InPlaceWriteScope write_scope(version_manager, *store_);
    committed_timestamp = write_scope.Timestamp();
  }

  auto reader = ReadSnapshotLease::Acquire(version_manager, *store_);
  EXPECT_EQ(reader.timestamp(), committed_timestamp);
  EXPECT_EQ(reader.planning_generation(), initial_planning_generation);
}

TEST_F(ReadViewPublicationTest,
       InPlaceWriteScopePublishesDuringStackUnwinding) {
  VersionManager version_manager;
  version_manager.init_ts({1, 0}, 2);

  const auto initial_planning_generation = ReadPlanningGeneration(*store_);
  uint32_t committed_timestamp = 0;
  EXPECT_THROW(
      [&]() {
        InPlaceWriteScope write_scope(version_manager, *store_);
        committed_timestamp = write_scope.Timestamp();
        write_scope.MarkPlanningChanged();
        throw std::runtime_error("in-place write failed after mutation");
      }(),
      std::runtime_error);

  auto reader = ReadSnapshotLease::Acquire(version_manager, *store_);
  EXPECT_EQ(reader.timestamp(), committed_timestamp);
  EXPECT_EQ(reader.planning_generation(), initial_planning_generation + 1);
}

TEST_F(ReadViewPublicationTest, LeaseRetriesAfterBlockedOpenCycle) {
  ScriptedVersionManager version_manager({1, 0});
  auto replacement = MakeReplacement();
  bool publish_succeeded = false;
  version_manager.set_acquire_hook([&](int acquire_count) {
    if (acquire_count == 1) {
      auto published = PrepareAndPublishSnapshot(
          *store_, replacement, kReplacementPlanningGeneration);
      publish_succeeded = published.has_value();
      if (published) {
        version_manager.publish({2, published.value()});
      }
    }
  });

  auto lease = ReadSnapshotLease::Acquire(version_manager, *store_);

  EXPECT_TRUE(publish_succeeded);
  EXPECT_EQ(version_manager.acquire_count(), 2);
  EXPECT_EQ(version_manager.release_count(), 1);
  EXPECT_EQ(lease.timestamp(), 2u);
  EXPECT_TRUE(lease.view().schema().is_vertex_label_valid("company"));

  lease.release();
  EXPECT_EQ(version_manager.release_count(), 2);
}

TEST_F(ReadViewPublicationTest, LeaseRetryUsesConfiguredRuntimeWait) {
  ScriptedVersionManager version_manager({1, 0});
  version_manager.set_runtime_wait(&CountRuntimeWait);
  runtime_wait_calls.store(0, std::memory_order_relaxed);

  auto replacement = MakeReplacement();
  bool publish_succeeded = true;
  constexpr int mismatch_count = kRuntimeWaitSpinIterations + 1;
  version_manager.set_acquire_hook([&](int acquire_count) {
    if (acquire_count <= mismatch_count) {
      auto published = PrepareAndPublishSnapshot(
          *store_, replacement, kReplacementPlanningGeneration);
      publish_succeeded &= published.has_value();
      if (published) {
        version_manager.publish(
            {static_cast<uint32_t>(acquire_count + 1), published.value()});
      }
    }
  });

  auto lease = ReadSnapshotLease::Acquire(version_manager, *store_);

  EXPECT_TRUE(publish_succeeded);
  EXPECT_EQ(version_manager.acquire_count(), mismatch_count + 1);
  EXPECT_EQ(version_manager.release_count(), mismatch_count);
  EXPECT_EQ(runtime_wait_calls.load(std::memory_order_relaxed), 1);
  EXPECT_EQ(lease.timestamp(), static_cast<uint32_t>(mismatch_count + 1));
  EXPECT_TRUE(lease.view().schema().is_vertex_label_valid("company"));
}

}  // namespace
}  // namespace neug
