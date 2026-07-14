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

#include <algorithm>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "neug/compiler/planner/graph_planner.h"
#include "neug/common/columns/value_columns.h"
#include "neug/common/types/data_chunk.h"
#include "neug/common/types/value.h"
#include "neug/execution/execute/query_cache.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/container/i_container.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph_snapshot_store.h"
#include "neug/storages/index/storage_index.h"
#include "neug/storages/index/storage_index_manager.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/storages/module/module_factory.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/update_transaction.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/wal.h"
#include "test_index_common.h"

namespace neug {
namespace {

class CapturingWalWriter : public IWalWriter {
 public:
  std::string type() const override { return "capturing"; }
  void open() override {}
  void close() override {}

  bool append(const char* data, size_t length) override {
    records.emplace_back(data, data + length);
    return true;
  }

  std::vector<std::vector<char>> records;
};

class StubPlanner : public IGraphPlanner {
 public:
  std::string type() const override { return "stub"; }

  result<std::pair<physical::PhysicalPlan, std::string>> compilePlan(
      const std::string&, const Schema*, const GraphStats&) override {
    RETURN_STATUS_ERROR(StatusCode::ERR_NOT_SUPPORTED,
                        "StubPlanner does not compile plans");
  }

  AccessMode analyzeMode(const std::string&) const override {
    return AccessMode::kRead;
  }
};

class TPIndexTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    ModuleFactory::instance().Register(
        kExampleIndexType, [] { return std::make_unique<ExampleIndex>(); });
  }

  void SetUp() override {
    work_dir_ = std::string("/tmp/test_tp_index_") +
                ::testing::UnitTest::GetInstance()->current_test_info()->name();
    std::filesystem::remove_all(work_dir_);
    std::filesystem::create_directories(work_dir_);
    OpenFreshGraph();
  }

  void TearDown() override {
    snapshot_store_.reset();
    ap_.reset();
    view_.reset();
    graph_.reset();
    checkpoint_mgr_.Close();
    std::filesystem::remove_all(work_dir_);
  }

  void OpenFreshGraph() {
    checkpoint_mgr_.Open(work_dir_);
    auto ckp =
        checkpoint_mgr_.GetCheckpoint(checkpoint_mgr_.CreateCheckpoint());
    graph_ = std::make_shared<PropertyGraph>();
    graph_->Open(ckp, MemoryLevel::kInMemory);
    view_ = std::make_unique<GraphView>(*graph_);
    ap_ = std::make_unique<StorageAPUpdateInterface>(*graph_, *view_, 0,
                                                     allocator_);
    version_manager_.init_ts(0, 1);
    wal_writer_.records.clear();
    auto global_cache = std::make_shared<execution::GlobalQueryCache>(
        std::make_shared<StubPlanner>());
    local_cache_ = std::make_unique<execution::LocalQueryCache>(global_cache);
  }

  void StartSnapshotStore() {
    ap_.reset();
    view_.reset();
    snapshot_store_ = std::make_unique<GraphSnapshotStore>(16, graph_);
  }

  UpdateTransaction NewUpdateTransaction() {
    auto ts = version_manager_.acquire_update_timestamp();
    auto cow_graph = snapshot_store_->CurrentSnapshot().Clone();
    return UpdateTransaction(std::move(cow_graph), allocator_, wal_writer_,
                             version_manager_, *snapshot_store_, *local_cache_,
                             ts);
  }

  ReadTransaction NewReadTransaction() {
    auto ts = version_manager_.acquire_read_timestamp();
    SnapshotGuard guard(*snapshot_store_);
    return ReadTransaction(std::move(guard), version_manager_, ts);
  }

  void Commit(UpdateTransaction& txn) { ASSERT_TRUE(txn.Commit()); }

  void CreatePersonTableAP() {
    CreateVertexTypeParamBuilder builder;
    auto status = ap_->CreateVertexType(
        builder.VertexLabel("Person")
            .AddProperty("id", Value::INT64(0))
            .AddProperty("name", Value::STRING(""))
            .AddProperty("age", Value::INT32(0))
            .AddPrimaryKeyName("id")
            .Build());
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  void CreateReplacementTableAP() {
    CreateVertexTypeParamBuilder builder;
    auto status = ap_->CreateVertexType(
        builder.VertexLabel("Replacement")
            .AddProperty("id", Value::INT64(0))
            .AddProperty("value", Value::INT32(0))
            .AddPrimaryKeyName("id")
            .Build());
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  void CreatePersonTableTP() {
    StartSnapshotStore();
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    CreateVertexTypeParamBuilder builder;
    auto status = tp.CreateVertexType(
        builder.VertexLabel("Person")
            .AddProperty("id", Value::INT64(0))
            .AddProperty("name", Value::STRING(""))
            .AddProperty("age", Value::INT32(0))
            .AddPrimaryKeyName("id")
            .Build());
    ASSERT_TRUE(status.ok()) << status.ToString();
    Commit(txn);
  }

  result<StorageIndex*> CreateIndexOnGraph(PropertyGraph& graph,
                                           const std::string& name,
                                           const std::string& label_name,
                                           const std::string& property_name) {
    auto label = graph.schema().get_vertex_label_id(label_name);
    auto schema = graph.schema().get_vertex_schema(label);
    auto prop_it = std::find(schema->property_names.begin(),
                             schema->property_names.end(), property_name);
    if (prop_it == schema->property_names.end()) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                          "Property does not exist: " + property_name);
    }
    auto prop_id = static_cast<size_t>(
        std::distance(schema->property_names.begin(), prop_it));
    auto meta = std::make_unique<IndexMeta>();
    meta->name = name;
    meta->type = "example";
    meta->schema.label_id = label;
    meta->schema.property_name = property_name;
    meta->schema.property_type = schema->property_types[prop_id];
    return graph.mutable_index_manager().CreateIndex(name, std::move(meta));
  }

  result<StorageIndex*> CreateIndex(const std::string& name,
                                    const std::string& label_name,
                                    const std::string& property_name) {
    return CreateIndexOnGraph(*graph_, name, label_name, property_name);
  }

  result<StorageIndex*> CreateIndexOnCurrentSnapshot(
      const std::string& name, const std::string& label_name,
      const std::string& property_name) {
    SnapshotGuard guard(*snapshot_store_);
    return CreateIndexOnGraph(*guard.get().mutable_graph(), name, label_name,
                              property_name);
  }

  StorageIndex* GetIndexByName(const std::string& name) const {
    return snapshot_store_->CurrentSnapshot().index_manager().GetIndexByName(
        name);
  }

  std::vector<StorageIndex*> GetIndexes(
      label_t label, const std::string& property_name) const {
    auto indexes = snapshot_store_->CurrentSnapshot().index_manager().GetIndex(
        label, property_name);
    EXPECT_TRUE(indexes) << indexes.error().ToString();
    if (!indexes) {
      return {};
    }
    return indexes.value();
  }

  void AddPersonTP(StorageTPUpdateInterface& tp, int64_t id,
                   const std::string& name, int32_t age, vid_t* out = nullptr) {
    auto label = tp.schema().get_vertex_label_id("Person");
    vid_t vid = 0;
    auto status = tp.AddVertex(
        label, Value::INT64(id),
        {Value::STRING(name), Value::INT32(age)}, vid);
    ASSERT_TRUE(status.ok()) << status.ToString();
    if (out) {
      *out = vid;
    }
  }

  std::vector<std::string> SearchPersonNames(
      const StorageReadInterface& reader, int32_t age,
      const std::string& index_name = "idx_person_age") const {
    auto* index = reader.index_manager().GetIndexByName(index_name);
    EXPECT_NE(index, nullptr);
    if (!index) {
      return {};
    }
    ExampleIndexQueryParams params(age);
    IndexFilterParams filter;
    auto result = index->Search(params, filter, reader);
    EXPECT_TRUE(result) << result.error().ToString();
    if (!result) {
      return {};
    }
    auto label = reader.schema().get_vertex_label_id("Person");
    auto name_col = reader.GetVertexPropColumn(label, "name");
    std::vector<std::string> names;
    for (auto vid : result.value()) {
      names.push_back(name_col->get_any(vid).GetValue<std::string>());
    }
    std::sort(names.begin(), names.end());
    return names;
  }

  std::vector<std::string> SearchPersonNamesInCurrent(int32_t age) {
    auto txn = NewReadTransaction();
    StorageReadInterface reader(txn.view(), txn.timestamp());
    auto names = SearchPersonNames(reader, age);
    txn.Commit();
    return names;
  }

  std::string work_dir_;
  CheckpointManager checkpoint_mgr_;
  std::shared_ptr<PropertyGraph> graph_;
  std::unique_ptr<GraphView> view_;
  Allocator allocator_{MemoryLevel::kInMemory, ""};
  std::unique_ptr<StorageAPUpdateInterface> ap_;
  std::unique_ptr<GraphSnapshotStore> snapshot_store_;
  VersionManager version_manager_;
  CapturingWalWriter wal_writer_;
  std::unique_ptr<execution::LocalQueryCache> local_cache_;
};

TEST_F(TPIndexTest, CreateIndexEmptyGraphAndDuplicateName) {
  CreatePersonTableTP();

  auto created =
      CreateIndexOnCurrentSnapshot("idx_person_age", "Person", "age");
  ASSERT_TRUE(created) << created.error().ToString();
  EXPECT_NE(created.value(), nullptr);

  auto duplicate =
      CreateIndexOnCurrentSnapshot("idx_person_age", "Person", "age");
  EXPECT_FALSE(duplicate);
  EXPECT_EQ(duplicate.error().error_code(), StatusCode::ERR_SCHEMA_MISMATCH);
}

TEST_F(TPIndexTest, DropVertexTypeDeletesBoundIndex) {
  CreatePersonTableAP();
  CreateReplacementTableAP();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));
  StartSnapshotStore();

  auto person_label =
      snapshot_store_->CurrentSnapshot().schema().get_vertex_label_id("Person");
  ASSERT_EQ(GetIndexes(person_label, "age").size(), 1);

  auto txn = NewUpdateTransaction();
  StorageTPUpdateInterface tp(txn);
  auto status = tp.DeleteVertexType("Person");
  ASSERT_TRUE(status.ok()) << status.ToString();
  Commit(txn);

  EXPECT_TRUE(GetIndexes(person_label, "age").empty());
}

TEST_F(TPIndexTest, DropAndRenameVertexPropertyDeleteBoundIndex) {
  CreatePersonTableAP();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));
  StartSnapshotStore();
  auto person_label =
      snapshot_store_->CurrentSnapshot().schema().get_vertex_label_id("Person");
  ASSERT_EQ(GetIndexes(person_label, "age").size(), 1);

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    DeleteVertexPropertiesParamBuilder delete_builder;
    auto status = tp.DeleteVertexProperties(
        delete_builder.VertexLabel("Person").AddDeleteProperty("age").Build());
    ASSERT_TRUE(status.ok()) << status.ToString();
    Commit(txn);
  }
  EXPECT_TRUE(GetIndexes(person_label, "age").empty());

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    AddVertexPropertiesParamBuilder add_builder;
    auto status = tp.AddVertexProperties(
        add_builder.VertexLabel("Person")
            .AddProperty("score", Value::INT32(0))
            .Build());
    ASSERT_TRUE(status.ok()) << status.ToString();
    Commit(txn);
  }

  ASSERT_TRUE(
      CreateIndexOnCurrentSnapshot("idx_person_score", "Person", "score"));
  ASSERT_EQ(GetIndexes(person_label, "score").size(), 1);

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    RenameVertexPropertiesParamBuilder rename_builder;
    auto status =
        tp.RenameVertexProperties(rename_builder.VertexLabel("Person")
                                      .AddRenameProperty("score", "years")
                                      .Build());
    ASSERT_TRUE(status.ok()) << status.ToString();
    Commit(txn);
  }

  EXPECT_TRUE(GetIndexes(person_label, "score").empty());
}

TEST_F(TPIndexTest, InsertDeleteAndUpdateMaintainIndex) {
  CreatePersonTableAP();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));
  StartSnapshotStore();

  vid_t alice = 0;
  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    AddPersonTP(tp, 1, "Alice", 30, &alice);
    AddPersonTP(tp, 2, "Bob", 25);
    AddPersonTP(tp, 3, "Charlie", 30);
    EXPECT_EQ(SearchPersonNames(tp, 30),
              (std::vector<std::string>{"Alice", "Charlie"}));

    auto label = tp.schema().get_vertex_label_id("Person");
    auto delete_status = tp.DeleteVertex(label, alice);
    ASSERT_TRUE(delete_status.ok()) << delete_status.ToString();
    EXPECT_EQ(SearchPersonNames(tp, 30), (std::vector<std::string>{"Charlie"}));

    vid_t bob = 0;
    ASSERT_TRUE(tp.GetVertexIndex(label, Value::INT64(2), bob));
    auto schema = tp.schema().get_vertex_schema(label);
    auto age_it = std::find(schema->property_names.begin(),
                            schema->property_names.end(), "age");
    ASSERT_NE(age_it, schema->property_names.end());
    auto age_col =
        static_cast<int>(std::distance(schema->property_names.begin(), age_it));
    auto update_status = tp.UpdateVertexProperty(label, bob, age_col,
                                                 Value::INT32(30));
    ASSERT_TRUE(update_status.ok()) << update_status.ToString();
    EXPECT_EQ(SearchPersonNames(tp, 25), (std::vector<std::string>{}));
    EXPECT_EQ(SearchPersonNames(tp, 30),
              (std::vector<std::string>{"Bob", "Charlie"}));
    Commit(txn);
  }

  EXPECT_EQ(SearchPersonNamesInCurrent(30),
            (std::vector<std::string>{"Bob", "Charlie"}));
}

TEST_F(TPIndexTest, BatchAddVerticesIsNotSupportedInTPMode) {
  CreatePersonTableAP();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));
  StartSnapshotStore();

  auto txn = NewUpdateTransaction();
  StorageTPUpdateInterface tp(txn);
  auto status =
      tp.BatchAddVertices(tp.schema().get_vertex_label_id("Person"), nullptr);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), StatusCode::ERR_NOT_SUPPORTED);
  txn.Abort();
}

TEST_F(TPIndexTest, IndexPersistsAfterCheckpointReopen) {
  CreatePersonTableAP();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));
  StartSnapshotStore();

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    for (const auto& person : kPersons) {
      AddPersonTP(tp, person.id, person.name, person.age);
    }
    EXPECT_EQ(SearchPersonNames(tp, 30),
              (std::vector<std::string>{"Alice", "Charlie"}));
    Commit(txn);
  }

  {
    SnapshotGuard guard(*snapshot_store_);
    auto ckp_id = checkpoint_mgr_.CreateCheckpoint();
    auto ckp = checkpoint_mgr_.GetCheckpoint(ckp_id);
    guard.get().mutable_graph()->Dump(ckp, false);
  }

  auto reopened = std::make_shared<PropertyGraph>();
  reopened->Open(checkpoint_mgr_.GetCheckpoint(checkpoint_mgr_.HeadId()),
                 MemoryLevel::kInMemory);
  GraphView reopened_view(*reopened);
  StorageReadInterface reader(reopened_view, MAX_TIMESTAMP);

  EXPECT_NE(reader.index_manager().GetIndexByName("idx_person_age"), nullptr);
  EXPECT_EQ(SearchPersonNames(reader, 30),
            (std::vector<std::string>{"Alice", "Charlie"}));
  EXPECT_EQ(SearchPersonNames(reader, 25),
            (std::vector<std::string>{"Bob", "Eve"}));
  EXPECT_EQ(SearchPersonNames(reader, 40), (std::vector<std::string>{"Diana"}));
}

TEST_F(TPIndexTest, AutomaticallyDeletedIndexStaysDeletedAfterReopen) {
  CreatePersonTableAP();
  CreateReplacementTableAP();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));
  StartSnapshotStore();
  auto person_label =
      snapshot_store_->CurrentSnapshot().schema().get_vertex_label_id("Person");
  ASSERT_EQ(GetIndexes(person_label, "age").size(), 1);

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    auto status = tp.DeleteVertexType("Person");
    ASSERT_TRUE(status.ok()) << status.ToString();
    Commit(txn);
  }
  EXPECT_TRUE(GetIndexes(person_label, "age").empty());

  {
    SnapshotGuard guard(*snapshot_store_);
    auto ckp_id = checkpoint_mgr_.CreateCheckpoint();
    auto ckp = checkpoint_mgr_.GetCheckpoint(ckp_id);
    guard.get().mutable_graph()->Dump(ckp, false);
  }

  auto reopened = std::make_shared<PropertyGraph>();
  reopened->Open(checkpoint_mgr_.GetCheckpoint(checkpoint_mgr_.HeadId()),
                 MemoryLevel::kInMemory);
  auto indexes = reopened->index_manager().GetIndex(person_label, "age");
  ASSERT_TRUE(indexes) << indexes.error().ToString();
  EXPECT_TRUE(indexes->empty());
}

TEST_F(TPIndexTest, DropIndexRemovesIndexInCommittedTransaction) {
  CreatePersonTableAP();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));
  StartSnapshotStore();
  auto person_label =
      snapshot_store_->CurrentSnapshot().schema().get_vertex_label_id("Person");
  ASSERT_EQ(GetIndexes(person_label, "age").size(), 1);

  auto txn = NewUpdateTransaction();
  StorageTPUpdateInterface tp(txn);
  auto status = tp.DropIndex("idx_person_age");
  ASSERT_TRUE(status.ok()) << status.ToString();
  Commit(txn);

  EXPECT_EQ(GetIndexByName("idx_person_age"), nullptr);
  EXPECT_TRUE(GetIndexes(person_label, "age").empty());
}

TEST_F(TPIndexTest, WalReplayRestoresIndexData) {
  CreatePersonTableAP();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));
  StartSnapshotStore();
  auto replay_graph = snapshot_store_->CurrentSnapshot().Clone();

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    AddPersonTP(tp, 1, "Alice", 30);
    AddPersonTP(tp, 2, "Bob", 25);
    AddPersonTP(tp, 3, "Charlie", 30);
    Commit(txn);
  }
  ASSERT_EQ(wal_writer_.records.size(), 1);
  const auto& wal = wal_writer_.records.back();
  ASSERT_GT(wal.size(), sizeof(WalHeader));
  const auto* header = reinterpret_cast<const WalHeader*>(wal.data());
  ASSERT_EQ(static_cast<size_t>(header->length),
            wal.size() - sizeof(WalHeader));

  {
    GraphView before_replay_view(*replay_graph);
    StorageReadInterface before_replay_reader(before_replay_view,
                                              header->timestamp);
    EXPECT_EQ(SearchPersonNames(before_replay_reader, 30),
              (std::vector<std::string>{}));
    EXPECT_EQ(SearchPersonNames(before_replay_reader, 25),
              (std::vector<std::string>{}));
  }

  UpdateTransaction::IngestWal(
      *replay_graph, header->timestamp,
      const_cast<char*>(wal.data() + sizeof(WalHeader)), header->length,
      allocator_);
  GraphView replay_view(*replay_graph);
  StorageReadInterface replay_reader(replay_view, header->timestamp);

  EXPECT_EQ(SearchPersonNames(replay_reader, 30),
            (std::vector<std::string>{"Alice", "Charlie"}));
  EXPECT_EQ(SearchPersonNames(replay_reader, 25),
            (std::vector<std::string>{"Bob"}));
}

TEST_F(TPIndexTest, AbortDiscardsIndexMutations) {
  CreatePersonTableAP();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));
  StartSnapshotStore();

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    AddPersonTP(tp, 1, "Alice", 30);
    Commit(txn);
  }

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    auto label = tp.schema().get_vertex_label_id("Person");
    vid_t alice = 0;
    ASSERT_TRUE(tp.GetVertexIndex(label, Value::INT64(1), alice));
    auto schema = tp.schema().get_vertex_schema(label);
    auto age_it = std::find(schema->property_names.begin(),
                            schema->property_names.end(), "age");
    ASSERT_NE(age_it, schema->property_names.end());
    auto age_col =
        static_cast<int>(std::distance(schema->property_names.begin(), age_it));
    ASSERT_TRUE(tp.UpdateVertexProperty(label, alice, age_col,
                                        Value::INT32(40)));
    AddPersonTP(tp, 2, "Bob", 25);
    EXPECT_EQ(SearchPersonNames(tp, 40), (std::vector<std::string>{"Alice"}));
    EXPECT_EQ(SearchPersonNames(tp, 25), (std::vector<std::string>{"Bob"}));
    txn.Abort();
  }

  EXPECT_EQ(SearchPersonNamesInCurrent(30),
            (std::vector<std::string>{"Alice"}));
  EXPECT_EQ(SearchPersonNamesInCurrent(40), (std::vector<std::string>{}));
  EXPECT_EQ(SearchPersonNamesInCurrent(25), (std::vector<std::string>{}));
}

TEST_F(TPIndexTest, ReadTransactionIsolationFromUpdateTransaction) {
  CreatePersonTableAP();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));
  StartSnapshotStore();

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    AddPersonTP(tp, 1, "Alice", 30);
    Commit(txn);
  }

  auto read_txn = NewReadTransaction();
  StorageReadInterface read_reader(read_txn.view(), read_txn.timestamp());

  auto update_txn = NewUpdateTransaction();
  StorageTPUpdateInterface tp(update_txn);
  auto label = tp.schema().get_vertex_label_id("Person");
  vid_t alice = 0;
  ASSERT_TRUE(tp.GetVertexIndex(label, Value::INT64(1), alice));
  auto schema = tp.schema().get_vertex_schema(label);
  auto age_it = std::find(schema->property_names.begin(),
                          schema->property_names.end(), "age");
  ASSERT_NE(age_it, schema->property_names.end());
  auto age_col =
      static_cast<int>(std::distance(schema->property_names.begin(), age_it));
  ASSERT_TRUE(tp.UpdateVertexProperty(label, alice, age_col,
                                      Value::INT32(40)));
  AddPersonTP(tp, 2, "Bob", 25);

  EXPECT_EQ(SearchPersonNames(tp, 40), (std::vector<std::string>{"Alice"}));
  EXPECT_EQ(SearchPersonNames(tp, 25), (std::vector<std::string>{"Bob"}));
  EXPECT_EQ(SearchPersonNames(read_reader, 30),
            (std::vector<std::string>{"Alice"}));
  EXPECT_EQ(SearchPersonNames(read_reader, 40), (std::vector<std::string>{}));
  EXPECT_EQ(SearchPersonNames(read_reader, 25), (std::vector<std::string>{}));

  Commit(update_txn);

  EXPECT_EQ(SearchPersonNames(read_reader, 30),
            (std::vector<std::string>{"Alice"}));
  EXPECT_EQ(SearchPersonNames(read_reader, 40), (std::vector<std::string>{}));
  EXPECT_EQ(SearchPersonNames(read_reader, 25), (std::vector<std::string>{}));
  read_txn.Commit();

  EXPECT_EQ(SearchPersonNamesInCurrent(30), (std::vector<std::string>{}));
  EXPECT_EQ(SearchPersonNamesInCurrent(40),
            (std::vector<std::string>{"Alice"}));
  EXPECT_EQ(SearchPersonNamesInCurrent(25), (std::vector<std::string>{"Bob"}));
}

}  // namespace
}  // namespace neug
