/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>

#include <filesystem>
#include <functional>
#include <memory>
#include <string>

#include "neug/storages/graph/operation_params.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/transaction/mvcc_insert_transaction.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/wal.h"
#include "unittest/utils.h"

namespace neug {
namespace {

class VisibilityWalWriter : public IWalWriter {
 public:
  std::string type() const override { return "visibility-test"; }
  void open(const std::string&) override {}
  void close() override {}
  bool append(const char*, size_t) override {
    return on_append ? on_append() : true;
  }
  std::function<bool()> on_append;
};

class InsertVisibilityTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ =
        std::filesystem::temp_directory_path() /
        ("neug_insert_visibility_" +
         std::string(
             ::testing::UnitTest::GetInstance()->current_test_info()->name()));
    std::filesystem::remove_all(path_);
    std::filesystem::create_directories(path_);
    checkpoints_.Open(path_.string());
    graph_ = std::make_shared<PropertyGraph>();
    graph_->Open(make_checkpoint(checkpoints_), MemoryLevel::kInMemory);
    CreateVertexTypeParamBuilder person;
    ASSERT_TRUE(graph_
                    ->CreateVertexType(person.VertexLabel("person")
                                           .AddProperty("id", Value::INT64(0))
                                           .AddPrimaryKeyName("id")
                                           .Build())
                    .ok());
    CreateEdgeTypeParamBuilder edge;
    ASSERT_TRUE(graph_
                    ->CreateEdgeType(edge.SrcLabel("person")
                                         .DstLabel("person")
                                         .EdgeLabel("knows")
                                         .Build())
                    .ok());
    store_ = std::make_unique<GraphSnapshotStore>(4, graph_);
    vm_.init_ts({0, 0}, 4);
  }

  void TearDown() override {
    store_.reset();
    graph_.reset();
    std::filesystem::remove_all(path_);
  }

  MvccInsertTransaction Begin(VisibilityWalWriter& writer) {
    return MvccInsertTransaction(SnapshotGuard(*store_), alloc_, writer, vm_,
                                 vm_.acquire_insert_timestamp());
  }

  void AddPair(MvccInsertTransaction& txn) {
    vid_t lid;
    ASSERT_TRUE(txn.AddVertex(0, Value::INT64(1), {}, lid).ok());
    ASSERT_TRUE(txn.AddVertex(0, Value::INT64(2), {}, lid).ok());
  }

  void AddRelationship(MvccInsertTransaction& txn) {
    vid_t src, dst;
    ASSERT_TRUE(txn.GetVertexIndex(0, Value::INT64(1), src));
    ASSERT_TRUE(txn.GetVertexIndex(0, Value::INT64(2), dst));
    const void* prop = nullptr;
    ASSERT_TRUE(txn.AddEdge(0, src, 0, dst, 0, {}, prop).ok());
  }

  std::filesystem::path path_;
  CheckpointManager checkpoints_;
  std::shared_ptr<PropertyGraph> graph_;
  std::unique_ptr<GraphSnapshotStore> store_;
  VersionManager vm_;
  Allocator alloc_{MemoryLevel::kInMemory, ""};
};

TEST_F(InsertVisibilityTest, CommittedEndpointsAboveReaderWatermark) {
  VisibilityWalWriter writer;
  auto gap = Begin(writer);
  auto vertices = Begin(writer);
  AddPair(vertices);
  ASSERT_TRUE(vertices.Commit());
  ASSERT_EQ(vm_.insert_read_timestamp(), 0u);

  auto edge = Begin(writer);
  AddRelationship(edge);
  ASSERT_TRUE(edge.Commit());
  // Ordinary readers still see a coherent prefix, including neither vertex
  // nor edge until the older transaction finishes.
  SnapshotGuard snapshot(*store_);
  const auto& view = snapshot.get().view();
  vid_t lid;
  EXPECT_FALSE(view.get_lid(0, Value::INT64(1), lid, 0));
  EXPECT_EQ(view.EdgeNum(0, 0, 0), 1u);
  auto hidden = view.GetGenericOutgoingView(0, 0, 0, 0).get_edges(0);
  EXPECT_EQ(hidden.begin(), hidden.end());
  gap.Abort();
  EXPECT_TRUE(
      view.get_lid(0, Value::INT64(1), lid, vm_.insert_read_timestamp()));
  auto neighbors =
      view.GetGenericOutgoingView(0, 0, 0, vm_.insert_read_timestamp())
          .get_edges(lid);
  ASSERT_NE(neighbors.begin(), neighbors.end());
  vid_t dst;
  ASSERT_TRUE(
      view.get_lid(0, Value::INT64(2), dst, vm_.insert_read_timestamp()));
  EXPECT_EQ(neighbors.begin().get_vertex(), dst);
  auto incoming =
      view.GetGenericIncomingView(0, 0, 0, vm_.insert_read_timestamp())
          .get_edges(dst);
  ASSERT_NE(incoming.begin(), incoming.end());
  EXPECT_EQ(incoming.begin().get_vertex(), lid);
}

TEST_F(InsertVisibilityTest, PendingAndRolledBackEndpointsStayInvisible) {
  VisibilityWalWriter writer;
  auto pending = Begin(writer);
  AddPair(pending);
  writer.on_append = [&] {
    VisibilityWalWriter other_writer;
    auto edge = Begin(other_writer);
    vid_t lid;
    EXPECT_FALSE(edge.GetVertexIndex(0, Value::INT64(1), lid));
    const void* prop = nullptr;
    EXPECT_THROW(edge.AddEdge(0, 0, 0, 1, 0, {}, prop), std::exception);
    return false;
  };
  EXPECT_FALSE(pending.Commit());
  VisibilityWalWriter other_writer;
  auto next = Begin(other_writer);
  vid_t lid;
  EXPECT_FALSE(next.GetVertexIndex(0, Value::INT64(1), lid));
  EXPECT_EQ(graph_->EdgeNum(0, 0, 0), 0u);
}

TEST_F(InsertVisibilityTest, SameTransactionEndpointsRemainUsable) {
  VisibilityWalWriter writer;
  auto txn = Begin(writer);
  AddPair(txn);
  AddRelationship(txn);
  EXPECT_TRUE(txn.Commit());
  EXPECT_EQ(graph_->EdgeNum(0, 0, 0), 1u);
}

TEST_F(InsertVisibilityTest, LaterTimestampDoesNotEnterEarlierInsert) {
  VisibilityWalWriter writer;
  auto earlier = Begin(writer);
  auto vertices = Begin(writer);
  AddPair(vertices);
  ASSERT_TRUE(vertices.Commit());
  vid_t lid;
  EXPECT_FALSE(earlier.GetVertexIndex(0, Value::INT64(1), lid));
  // Timestamp 2 is completed despite the gap; after the gap is released the
  // same query must succeed after its completion-window entry has been cleared.
  EXPECT_TRUE(vm_.is_insert_timestamp_completed(2));
  earlier.Abort();
  EXPECT_TRUE(vm_.is_insert_timestamp_completed(2));
  auto next = Begin(writer);
  EXPECT_TRUE(next.GetVertexIndex(0, Value::INT64(1), lid));
}

TEST_F(InsertVisibilityTest, BufferedVertexDoesNotAliasEarlierCommit) {
  VisibilityWalWriter writer;
  auto gap = Begin(writer);
  auto earlier = Begin(writer);
  AddPair(earlier);

  auto mixed = Begin(writer);
  vid_t temporary;
  ASSERT_TRUE(mixed.AddVertex(0, Value::INT64(3), {}, temporary).ok());
  // The earlier insert acquires real VIDs only after mixed has allocated its
  // transaction-local VID. Its commit is still above the reader watermark.
  ASSERT_TRUE(earlier.Commit());
  ASSERT_EQ(vm_.insert_read_timestamp(), 0u);
  vid_t src, dst, buffered;
  ASSERT_TRUE(mixed.GetVertexIndex(0, Value::INT64(1), src));
  ASSERT_TRUE(mixed.GetVertexIndex(0, Value::INT64(2), dst));
  ASSERT_TRUE(mixed.GetVertexIndex(0, Value::INT64(3), buffered));
  EXPECT_EQ(buffered, temporary);
  EXPECT_NE(src, temporary);
  EXPECT_NE(dst, temporary);
  const void* prop = nullptr;
  EXPECT_NO_THROW(
      EXPECT_TRUE(mixed.AddEdge(0, src, 0, temporary, 0, {}, prop).ok()));
  EXPECT_NO_THROW(
      EXPECT_TRUE(mixed.AddEdge(0, temporary, 0, dst, 0, {}, prop).ok()));
  ASSERT_TRUE(mixed.Commit());
  gap.Abort();

  SnapshotGuard snapshot(*store_);
  const auto& view = snapshot.get().view();
  const auto ts = vm_.insert_read_timestamp();
  ASSERT_TRUE(view.get_lid(0, Value::INT64(3), buffered, ts));
  auto outgoing = view.GetGenericOutgoingView(0, 0, 0, ts).get_edges(src);
  ASSERT_NE(outgoing.begin(), outgoing.end());
  EXPECT_EQ(outgoing.begin().get_vertex(), buffered);
  auto incoming = view.GetGenericIncomingView(0, 0, 0, ts).get_edges(dst);
  ASSERT_NE(incoming.begin(), incoming.end());
  EXPECT_EQ(incoming.begin().get_vertex(), buffered);
  EXPECT_EQ(view.EdgeNum(0, 0, 0), 2u);
}

}  // namespace
}  // namespace neug
