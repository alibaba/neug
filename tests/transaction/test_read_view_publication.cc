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
#include <string>
#include <utility>

#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/graph/operation_params.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/transaction/read_snapshot_lease.h"
#include "neug/transaction/version_manager.h"
#include "unittest/utils.h"

namespace neug {
namespace {

class ScriptedVersionManager : public IVersionManager {
 public:
  explicit ScriptedVersionManager(PublishedReadView initial)
      : published_(PackPublishedReadView(initial)) {}

  void set_first_acquire_hook(std::function<void()> hook) {
    first_acquire_hook_ = std::move(hook);
  }

  void publish(PublishedReadView view) {
    published_.store(PackPublishedReadView(view), std::memory_order_release);
  }

  int acquire_count() const { return acquire_count_.load(); }
  int release_count() const { return release_count_.load(); }

  void init_ts(uint32_t, int) override {}
  bool try_set_runtime_wait_if_quiescent(RuntimeWaitFn) noexcept override {
    return true;
  }

  PublishedReadView acquire_read_view() override {
    const auto captured =
        UnpackPublishedReadView(published_.load(std::memory_order_acquire));
    if (acquire_count_.fetch_add(1) == 0 && first_acquire_hook_) {
      first_acquire_hook_();
    }
    return captured;
  }

  void release_read_view() override { release_count_.fetch_add(1); }
  uint32_t acquire_insert_timestamp() override { return 1; }
  void release_insert_timestamp(uint32_t) override {}
  uint32_t acquire_update_timestamp() override { return 1; }
  uint32_t reserve_view_generation() override { return 0; }
  void begin_update_commit(uint32_t) override {}
  void drain_readers() override {}
  void release_update_timestamp(uint32_t) override {}
  void release_update_timestamp_with_view(uint32_t, uint32_t) override {}
  uint32_t acquire_compact_timestamp() override { return 1; }
  void release_compact_timestamp(uint32_t) override {}
  void revert_compact_timestamp(uint32_t) override {}

 private:
  std::atomic<uint64_t> published_;
  std::atomic<int> acquire_count_{0};
  std::atomic<int> release_count_{0};
  std::function<void()> first_acquire_hook_;
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

  std::pair<std::shared_ptr<PropertyGraph>, uint32_t> PublishReplacement(
      VersionManager& version_manager) {
    const uint32_t timestamp = version_manager.acquire_update_timestamp();
    const uint32_t generation = version_manager.reserve_view_generation();
    version_manager.begin_update_commit(timestamp);
    auto replacement = initial_graph_->Clone();
    EXPECT_TRUE(store_->PublishSnapshot(replacement, generation).ok());
    version_manager.release_update_timestamp_with_view(timestamp, generation);
    return {std::move(replacement), timestamp};
  }

  std::string work_dir_;
  CheckpointManager checkpoint_manager_;
  std::shared_ptr<PropertyGraph> initial_graph_;
  std::unique_ptr<GraphSnapshotStore> store_;
};

TEST_F(ReadViewPublicationTest, SplitAcquisitionExposesGenerationMismatch) {
  VersionManager version_manager;
  version_manager.init_ts(1, 2);

  const PublishedReadView old_view = version_manager.acquire_read_view();
  const auto publication = PublishReplacement(version_manager);
  const auto& replacement = publication.first;

  SnapshotGuard current(*store_);
  EXPECT_EQ(old_view.visibility_ts, 1u);
  EXPECT_EQ(old_view.view_generation, 0u);
  EXPECT_EQ(current.get().mutable_graph(), replacement.get());
  EXPECT_NE(current.get().view_generation(), old_view.view_generation);

  current.release();
  version_manager.release_read_view();
}

TEST_F(ReadViewPublicationTest, ValidatedReaderKeepsPinnedOldSnapshot) {
  VersionManager version_manager;
  version_manager.init_ts(1, 2);

  auto lease = ReadSnapshotLease::Acquire(version_manager, *store_);
  const auto [replacement, timestamp] = PublishReplacement(version_manager);

  EXPECT_EQ(lease.timestamp(), 1u);
  EXPECT_EQ(lease.view_generation(), 0u);
  EXPECT_EQ(lease.graph(), initial_graph_.get());
  EXPECT_NE(lease.graph(), replacement.get());

  lease.release();
  auto next = ReadSnapshotLease::Acquire(version_manager, *store_);
  EXPECT_EQ(next.timestamp(), timestamp);
  EXPECT_EQ(next.view_generation(), 1u);
  EXPECT_EQ(next.graph(), replacement.get());
}

TEST_F(ReadViewPublicationTest, LeaseRetriesAfterBlockedOpenCycle) {
  ScriptedVersionManager version_manager({1, 0});
  auto replacement = initial_graph_->Clone();
  bool publish_succeeded = false;
  version_manager.set_first_acquire_hook([&] {
    publish_succeeded = store_->PublishSnapshot(replacement, 1).ok();
    version_manager.publish({2, 1});
  });

  auto lease = ReadSnapshotLease::Acquire(version_manager, *store_);

  EXPECT_TRUE(publish_succeeded);
  EXPECT_EQ(version_manager.acquire_count(), 2);
  EXPECT_EQ(version_manager.release_count(), 1);
  EXPECT_EQ(lease.timestamp(), 2u);
  EXPECT_EQ(lease.view_generation(), 1u);
  EXPECT_EQ(lease.graph(), replacement.get());

  lease.release();
  EXPECT_EQ(version_manager.release_count(), 2);
}

}  // namespace
}  // namespace neug
