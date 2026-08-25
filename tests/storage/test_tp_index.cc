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

#include "neug/common/columns/value_columns.h"
#include "neug/common/types/data_chunk.h"
#include "neug/common/types/value.h"
#include "neug/compiler/planner/graph_planner.h"
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
  void open(const std::string&) override {}
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

  QueryAnalysis analyzeQuery(const std::string&) const override { return {}; }
};

class TPIndexTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    ModuleFactory::instance().Register(
        kExampleIndexType, [] { return std::make_unique<ExampleIndex>(); });
    ModuleFactory::instance().Register(
        kVecIndexType, [] { return std::make_unique<VecIndex>(); });
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
    auto staging = checkpoint_mgr_.CreateStaging();
    CheckpointManifest meta;
    meta.SetSchema(Schema());
    staging.checkpoint()->SetManifest(std::move(meta));
    auto ckp = staging.Publish();
    graph_ = std::make_shared<PropertyGraph>();
    graph_->Open(ckp, MemoryLevel::kInMemory);
    view_ = std::make_unique<GraphView>(*graph_);
    ap_ = std::make_unique<StorageAPUpdateInterface>(*graph_, *view_, 0,
                                                     allocator_);
    version_manager_.init_ts({0, 0}, 1);
    wal_writer_.records.clear();
    auto global_cache = std::make_shared<execution::GlobalQueryCache>(
        std::make_shared<StubPlanner>());
  }

  void StartSnapshotStore() {
    ap_.reset();
    view_.reset();
    snapshot_store_ = std::make_unique<GraphSnapshotStore>(16, graph_);
  }

  UpdateTransaction NewUpdateTransaction() {
    UpdateTimestampLease timestamp_lease(version_manager_);
    auto [cow_graph, planning_generation] =
        snapshot_store_->CloneCurrentForUpdate();
    return UpdateTransaction(std::move(cow_graph), planning_generation,
                             allocator_, wal_writer_, *snapshot_store_,
                             std::move(timestamp_lease));
  }

  ReadTransaction NewReadTransaction() {
    return ReadTransaction(
        ReadSnapshotLease::Acquire(version_manager_, *snapshot_store_));
  }

  void Commit(UpdateTransaction& txn) { ASSERT_TRUE(txn.Commit()); }

  void CreatePersonTableAP() {
    CreateVertexTypeParamBuilder builder;
    auto status =
        ap_->CreateVertexType(builder.VertexLabel("Person")
                                  .AddProperty("id", Value::INT64(0))
                                  .AddProperty("name", Value::STRING(""))
                                  .AddProperty("age", Value::INT32(0))
                                  .AddPrimaryKeyName("id")
                                  .Build());
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  void CreateReplacementTableAP() {
    CreateVertexTypeParamBuilder builder;
    auto status =
        ap_->CreateVertexType(builder.VertexLabel("Replacement")
                                  .AddProperty("id", Value::INT64(0))
                                  .AddProperty("value", Value::INT32(0))
                                  .AddPrimaryKeyName("id")
                                  .Build());
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  void CreateItemTableAP() {
    CreateVertexTypeParamBuilder builder;
    auto status =
        ap_->CreateVertexType(builder.VertexLabel("Item")
                                  .AddProperty("id", Value::INT32(0))
                                  .AddProperty("value", Value::INT32(0))
                                  .AddPrimaryKeyName("id")
                                  .Build());
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  void CreateVectorTableAP() {
    auto vector_type = DataType::Array(DataType::FLOAT, 2);
    auto default_vector =
        Value::ARRAY(vector_type, {Value::FLOAT(0.0f), Value::FLOAT(0.0f)});
    CreateVertexTypeParamBuilder builder;
    auto status =
        ap_->CreateVertexType(builder.VertexLabel("Vector")
                                  .AddProperty("id", Value::INT64(0))
                                  .AddProperty("embedding", default_vector)
                                  .AddPrimaryKeyName("id")
                                  .Build());
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  vid_t AddVectorAP(int64_t id, float first, float second) {
    auto label = graph_->schema().get_vertex_label_id("Vector");
    auto vector_type = DataType::Array(DataType::FLOAT, 2);
    auto vector =
        Value::ARRAY(vector_type, {Value::FLOAT(first), Value::FLOAT(second)});
    vid_t vid = 0;
    auto status = ap_->AddVertex(label, Value::INT64(id), {vector}, vid);
    EXPECT_TRUE(status.ok()) << status.ToString();
    return vid;
  }

  result<StorageIndex*> CreateVecIndexAP(const std::string& name) {
    auto label = graph_->schema().get_vertex_label_id("Vector");
    auto meta = std::make_unique<IndexMeta>();
    meta->name = name;
    meta->type = "hnsw";
    meta->schema.label_id = label;
    meta->schema.property_name = "embedding";
    meta->schema.property_type = DataType::Array(DataType::FLOAT, 2);
    GS_AUTO(created, ap_->CreateIndex(std::move(meta)));
    return std::get<StorageIndex*>(created);
  }

  void CreatePersonTableTP() {
    StartSnapshotStore();
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    CreateVertexTypeParamBuilder builder;
    auto status =
        tp.CreateVertexType(builder.VertexLabel("Person")
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
    DataType property_type;
    if (property_name == std::get<1>(schema->primary_keys[0])) {
      property_type = std::get<0>(schema->primary_keys[0]);
    } else {
      auto prop_it = std::find(schema->property_names.begin(),
                               schema->property_names.end(), property_name);
      if (prop_it == schema->property_names.end()) {
        RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                            "Property does not exist: " + property_name);
      }
      auto prop_id = static_cast<size_t>(
          std::distance(schema->property_names.begin(), prop_it));
      property_type = schema->property_types[prop_id];
    }
    auto meta = std::make_unique<IndexMeta>();
    meta->name = name;
    meta->type = "example";
    meta->schema.label_id = label;
    meta->schema.property_name = property_name;
    meta->schema.property_type = property_type;
    const auto& vertex_table = graph.get_vertex_table(label);
    auto* column = vertex_table.GetPropertyColumnBase(property_name);
    if (!column) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                          "Property column does not exist: " + property_name);
    }
    auto created = graph.mutable_index_manager().CreateIndex(
        std::move(meta), std::make_unique<DefaultIndexIDAccessor>(), column,
        graph.GetVertexSet(label));
    if (!created) {
      return tl::unexpected(created.error());
    }
    return std::get<StorageIndex*>(created.value());
  }

  result<StorageIndex*> CreateIndex(const std::string& name,
                                    const std::string& label_name,
                                    const std::string& property_name) {
    return CreateIndexOnGraph(*graph_, name, label_name, property_name);
  }

  std::unique_ptr<IndexMeta> PersonAgeIndexMeta(
      const StorageReadInterface& storage,
      const std::string& name = "idx_person_age") {
    auto meta = std::make_unique<IndexMeta>();
    meta->name = name;
    meta->type = "example";
    meta->schema.label_id = storage.schema().get_vertex_label_id("Person");
    meta->schema.property_name = "age";
    meta->schema.property_type = DataType::INT32;
    return meta;
  }

  result<StorageIndex*> CreateIndexOnCurrentSnapshot(
      const std::string& name, const std::string& label_name,
      const std::string& property_name) {
    SnapshotGuard guard(*snapshot_store_);
    return CreateIndexOnGraph(*guard.get().mutable_graph(), name, label_name,
                              property_name);
  }

  StorageIndex* GetIndexByName(const std::string& name) const {
    return snapshot_store_->CurrentSnapshot()
        .index_manager()
        .GetIndexByName(name)
        .value_or(nullptr);
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
    auto status = tp.AddVertex(label, Value::INT64(id),
                               {Value::STRING(name), Value::INT32(age)}, vid);
    ASSERT_TRUE(status.ok()) << status.ToString();
    if (out) {
      *out = vid;
    }
  }

  std::vector<std::string> SearchPersonNames(
      const StorageReadInterface& reader, int32_t age,
      const std::string& index_name = "idx_person_age") const {
    ExampleIndexQueryParams params(age);
    auto result = reader.IndexSearch(index_name, params);
    EXPECT_TRUE(result) << result.error().ToString();
    if (!result) {
      return {};
    }
    auto label = reader.schema().get_vertex_label_id("Person");
    auto name_col = reader.GetVertexPropColumn(label, "name");
    std::vector<std::string> names;
    for (const auto& entry : result.value()) {
      names.push_back(name_col->get_any(entry.vid).GetValue<std::string>());
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

  std::vector<SearchResult> SearchVector(const StorageReadInterface& reader,
                                         std::vector<float> query) const {
    VecIndexQueryParams params(std::move(query));
    auto result = reader.IndexSearch("idx_vector_embedding", params);
    EXPECT_TRUE(result) << result.error().ToString();
    return result ? std::move(result.value()) : std::vector<SearchResult>{};
  }

  std::vector<SearchResult> SearchVectorInCurrent(std::vector<float> query) {
    auto txn = NewReadTransaction();
    StorageReadInterface reader(txn.view(), txn.timestamp());
    auto result = SearchVector(reader, std::move(query));
    txn.Commit();
    return result;
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
  EXPECT_EQ(duplicate.error().error_code(), StatusCode::ERR_ILLEGAL_OPERATION);
}

TEST_F(TPIndexTest, IndexAdminInterfaceIsAvailableInAPAndTP) {
  EXPECT_NE(dynamic_cast<StorageIndexDDLInterface*>(ap_.get()), nullptr);

  CreatePersonTableTP();
  auto txn = NewUpdateTransaction();
  StorageTPUpdateInterface tp(txn);
  EXPECT_NE(dynamic_cast<StorageIndexDDLInterface*>(&tp), nullptr);
  txn.Abort();
}

TEST_F(TPIndexTest, CreateAndDropIndexCommitThroughUpdateTransaction) {
  CreatePersonTableTP();
  wal_writer_.records.clear();

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    auto created = tp.CreateIndex(PersonAgeIndexMeta(tp));
    ASSERT_TRUE(created) << created.error().ToString();
    ASSERT_TRUE(std::holds_alternative<StorageIndex*>(created.value()));
    EXPECT_NE(std::get<StorageIndex*>(created.value()), nullptr);
    Commit(txn);
  }
  EXPECT_NE(GetIndexByName("idx_person_age"), nullptr);
  ASSERT_EQ(wal_writer_.records.size(), 1);

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    ASSERT_TRUE(tp.DropIndex("idx_person_age").ok());
    Commit(txn);
  }
  EXPECT_EQ(GetIndexByName("idx_person_age"), nullptr);
  ASSERT_EQ(wal_writer_.records.size(), 2);
}

TEST_F(TPIndexTest, AbortedCreateAndDropIndexDoNotAffectSnapshot) {
  CreatePersonTableTP();

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    ASSERT_TRUE(tp.CreateIndex(PersonAgeIndexMeta(tp)));
    txn.Abort();
  }
  EXPECT_EQ(GetIndexByName("idx_person_age"), nullptr);

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    ASSERT_TRUE(tp.CreateIndex(PersonAgeIndexMeta(tp)));
    Commit(txn);
  }
  ASSERT_NE(GetIndexByName("idx_person_age"), nullptr);

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    ASSERT_TRUE(tp.DropIndex("idx_person_age").ok());
    txn.Abort();
  }
  EXPECT_NE(GetIndexByName("idx_person_age"), nullptr);
}

TEST_F(TPIndexTest, ActivateIndexesWithoutPendingIndexIsNoOp) {
  CreatePersonTableTP();
  wal_writer_.records.clear();

  SnapshotGuard before(*snapshot_store_);
  const auto* before_slot = &before.get();
  const auto snapshot_generation = before.get().snapshot_generation();
  const auto planning_generation = before.get().planning_generation();

  auto txn = NewUpdateTransaction();
  StorageTPUpdateInterface tp(txn);
  auto activated = tp.ActivateIndexes();
  ASSERT_TRUE(activated) << activated.error().ToString();
  EXPECT_EQ(activated.value(), 0u);
  Commit(txn);

  EXPECT_TRUE(wal_writer_.records.empty());
  SnapshotGuard after(*snapshot_store_);
  EXPECT_EQ(&after.get(), before_slot);
  EXPECT_EQ(after.get().snapshot_generation(), snapshot_generation);
  EXPECT_EQ(after.get().planning_generation(), planning_generation);
}

TEST_F(TPIndexTest, WalReplayRestoresCreateDropAndActivateIndexOperations) {
  CreatePersonTableTP();
  auto replay_graph = snapshot_store_->CurrentSnapshot().Clone();
  wal_writer_.records.clear();

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    ASSERT_TRUE(tp.CreateIndex(PersonAgeIndexMeta(tp)));
    Commit(txn);
  }
  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    auto activated = tp.ActivateIndexes();
    ASSERT_TRUE(activated);
    EXPECT_EQ(activated.value(), 0u);
    Commit(txn);
  }
  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    ASSERT_TRUE(tp.DropIndex("idx_person_age").ok());
    Commit(txn);
  }
  // No pending indexes exist here, so LOAD-style activation is a no-op and
  // must not create a schema WAL record.
  ASSERT_EQ(wal_writer_.records.size(), 2);

  for (const auto& wal : wal_writer_.records) {
    ASSERT_GT(wal.size(), sizeof(WalHeader));
    const auto* header = reinterpret_cast<const WalHeader*>(wal.data());
    UpdateTransaction::IngestWal(*replay_graph, header->timestamp,
                                 wal.data() + sizeof(WalHeader), header->length,
                                 allocator_);
  }
  EXPECT_FALSE(replay_graph->index_manager().GetIndexByName("idx_person_age"));
}

TEST_F(TPIndexTest, IndexConflictPreflightDoesNotWriteWal) {
  CreatePersonTableTP();
  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    ASSERT_TRUE(tp.CreateIndex(PersonAgeIndexMeta(tp)));
    Commit(txn);
  }
  wal_writer_.records.clear();

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    auto duplicate = tp.CreateIndex(PersonAgeIndexMeta(tp));
    ASSERT_FALSE(duplicate);
    EXPECT_EQ(duplicate.error().error_code(),
              StatusCode::ERR_ILLEGAL_OPERATION);
    EXPECT_TRUE(txn.Commit());
  }
  EXPECT_TRUE(wal_writer_.records.empty());

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    auto missing = tp.DropIndex("missing_index");
    EXPECT_EQ(missing.error_code(), StatusCode::ERR_NOT_FOUND);
    EXPECT_TRUE(txn.Commit());
  }
  EXPECT_TRUE(wal_writer_.records.empty());
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
  auto status = tp.DeleteVertexType(person_label);
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
        person_label, delete_builder.AddDeleteProperty("age").Build());
    ASSERT_TRUE(status.ok()) << status.ToString();
    Commit(txn);
  }
  EXPECT_TRUE(GetIndexes(person_label, "age").empty());

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    AddVertexPropertiesParamBuilder add_builder;
    auto status = tp.AddVertexProperties(
        person_label,
        add_builder.AddProperty("score", Value::INT32(0)).Build());
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
    auto status = tp.RenameVertexProperties(
        person_label,
        rename_builder.AddRenameProperty("score", "years").Build());
    ASSERT_TRUE(status.ok()) << status.ToString();

    // The published snapshot must retain the old metadata until commit.
    EXPECT_EQ(GetIndexes(person_label, "score").size(), 1);
    EXPECT_TRUE(GetIndexes(person_label, "years").empty());
    Commit(txn);
  }

  EXPECT_TRUE(GetIndexes(person_label, "score").empty());
  auto renamed_indexes = GetIndexes(person_label, "years");
  ASSERT_EQ(renamed_indexes.size(), 1);
  EXPECT_EQ(renamed_indexes.front()->GetMeta().name, "idx_person_score");
  EXPECT_EQ(renamed_indexes.front()->GetMeta().schema.property_name, "years");
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
    auto update_status =
        tp.UpdateVertexProperty(label, bob, age_col, Value::INT32(30));
    ASSERT_TRUE(update_status.ok()) << update_status.ToString();
    EXPECT_EQ(SearchPersonNames(tp, 25), (std::vector<std::string>{}));
    EXPECT_EQ(SearchPersonNames(tp, 30),
              (std::vector<std::string>{"Bob", "Charlie"}));
    Commit(txn);
  }

  EXPECT_EQ(SearchPersonNamesInCurrent(30),
            (std::vector<std::string>{"Bob", "Charlie"}));
}

TEST_F(TPIndexTest, APCreateVecColumnAndTPUpdateMaintainsVecIndexSearch) {
  CreateVectorTableAP();
  auto first_vid = AddVectorAP(1, 1.0f, 1.0f);
  AddVectorAP(2, 5.0f, 5.0f);
  AddVectorAP(3, 9.0f, 9.0f);

  auto created = CreateVecIndexAP("idx_vector_embedding");
  ASSERT_TRUE(created) << created.error().ToString();
  auto label = graph_->schema().get_vertex_label_id("Vector");
  ASSERT_NE(
      dynamic_cast<const VecColumn*>(
          graph_->get_vertex_table(label).GetPropertyColumnBase("embedding")),
      nullptr);

  StartSnapshotStore();
  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    auto vector_type = DataType::Array(DataType::FLOAT, 2);
    auto updated_vector =
        Value::ARRAY(vector_type, {Value::FLOAT(12.0f), Value::FLOAT(12.0f)});
    auto status = tp.UpdateVertexProperty(label, first_vid, 0, updated_vector);
    ASSERT_TRUE(status.ok()) << status.ToString();

    auto result = SearchVector(tp, {11.5f, 12.5f});
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result.front().vid, first_vid);
    Commit(txn);
  }

  auto committed_result = SearchVectorInCurrent({11.5f, 12.5f});
  ASSERT_EQ(committed_result.size(), 1);
  EXPECT_EQ(committed_result.front().vid, first_vid);
}

TEST_F(TPIndexTest, AbortedVectorVertexDeletePreservesReadSnapshot) {
  CreateVectorTableAP();
  auto first_vid = AddVectorAP(1, 1.0f, 1.0f);
  AddVectorAP(2, 5.0f, 5.0f);

  auto created = CreateVecIndexAP("idx_vector_embedding");
  ASSERT_TRUE(created) << created.error().ToString();
  StartSnapshotStore();

  auto read_txn = NewReadTransaction();
  StorageReadInterface read_reader(read_txn.view(), read_txn.timestamp());
  auto before_delete = SearchVector(read_reader, {1.0f, 1.0f});
  ASSERT_EQ(before_delete.size(), 1);
  EXPECT_EQ(before_delete.front().vid, first_vid);

  auto update_txn = NewUpdateTransaction();
  StorageTPUpdateInterface tp(update_txn);
  auto label = tp.schema().get_vertex_label_id("Vector");
  ASSERT_TRUE(tp.DeleteVertex(label, first_vid));
  auto update_results = SearchVector(tp, {1.0f, 1.0f});
  EXPECT_TRUE(std::none_of(update_results.begin(), update_results.end(),
                           [first_vid](const SearchResult& result) {
                             return result.vid == first_vid;
                           }));

  auto during_delete = SearchVector(read_reader, {1.0f, 1.0f});
  ASSERT_EQ(during_delete.size(), 1);
  EXPECT_EQ(during_delete.front().vid, first_vid);

  update_txn.Abort();

  auto after_abort = SearchVector(read_reader, {1.0f, 1.0f});
  ASSERT_EQ(after_abort.size(), 1);
  EXPECT_EQ(after_abort.front().vid, first_vid);
  read_txn.Commit();

  auto current = SearchVectorInCurrent({1.0f, 1.0f});
  ASSERT_EQ(current.size(), 1);
  EXPECT_EQ(current.front().vid, first_vid);
}

TEST_F(TPIndexTest, PrimaryKeyIndexMaintainedAcrossVertexLifecycle) {
  CreateItemTableAP();
  ASSERT_TRUE(CreateIndex("idx_item_id", "Item", "id"));
  StartSnapshotStore();

  auto label =
      snapshot_store_->CurrentSnapshot().schema().get_vertex_label_id("Item");
  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    vid_t vid = 0;
    ASSERT_TRUE(
        tp.AddVertex(label, Value::INT32(1), {Value::INT32(10)}, vid).ok());

    ExampleIndexQueryParams query(1);
    ASSERT_EQ(tp.IndexSearch("idx_item_id", query).value(),
              (std::vector<SearchResult>{{vid}}));

    ASSERT_TRUE(tp.DeleteVertex(label, vid).ok());
    EXPECT_TRUE(tp.IndexSearch("idx_item_id", query).value().empty());

    DeleteVertexPropertiesParamBuilder delete_builder;
    EXPECT_THROW(tp.DeleteVertexProperties(
                     label, delete_builder.AddDeleteProperty("id").Build()),
                 exception::RuntimeError);
    EXPECT_TRUE(tp.IndexSearch("idx_item_id", query));
    Commit(txn);
  }

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    ASSERT_TRUE(tp.DeleteVertexType(label).ok());
    Commit(txn);
  }
  EXPECT_EQ(GetIndexByName("idx_item_id"), nullptr);
}

TEST_F(TPIndexTest, BatchAddVerticesIsNotSupportedInTPMode) {
  CreatePersonTableAP();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));
  StartSnapshotStore();

  auto txn = NewUpdateTransaction();
  StorageTPUpdateInterface tp(txn);
  auto result =
      tp.BatchAddVertices(tp.schema().get_vertex_label_id("Person"), nullptr);
  EXPECT_FALSE(result);
  EXPECT_EQ(result.error().error_code(), StatusCode::ERR_NOT_SUPPORTED);
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

  std::shared_ptr<Checkpoint> published_checkpoint;
  {
    SnapshotGuard guard(*snapshot_store_);
    auto staging = checkpoint_mgr_.CreateStaging();
    guard.get().mutable_graph()->Compact();
    guard.get().mutable_graph()->DumpAndClear(staging.checkpoint());
    published_checkpoint = staging.Publish();
  }

  auto reopened = std::make_shared<PropertyGraph>();
  reopened->Open(published_checkpoint, MemoryLevel::kInMemory);
  GraphView reopened_view(*reopened);
  StorageReadInterface reader(reopened_view, MAX_TIMESTAMP);

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
    auto status = tp.DeleteVertexType(person_label);
    ASSERT_TRUE(status.ok()) << status.ToString();
    Commit(txn);
  }
  EXPECT_TRUE(GetIndexes(person_label, "age").empty());

  std::shared_ptr<Checkpoint> published_checkpoint;
  {
    SnapshotGuard guard(*snapshot_store_);
    auto staging = checkpoint_mgr_.CreateStaging();
    guard.get().mutable_graph()->Compact();
    guard.get().mutable_graph()->DumpAndClear(staging.checkpoint());
    published_checkpoint = staging.Publish();
  }

  auto reopened = std::make_shared<PropertyGraph>();
  reopened->Open(published_checkpoint, MemoryLevel::kInMemory);
  auto indexes = reopened->index_manager().GetIndex(person_label, "age");
  ASSERT_TRUE(indexes) << indexes.error().ToString();
  EXPECT_TRUE(indexes->empty());
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

  UpdateTransaction::IngestWal(*replay_graph, header->timestamp,
                               wal.data() + sizeof(WalHeader), header->length,
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
    ASSERT_TRUE(
        tp.UpdateVertexProperty(label, alice, age_col, Value::INT32(40)));
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

TEST_F(TPIndexTest, AbortDoesNotReuseAllocatedIndexID) {
  CreatePersonTableAP();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));
  StartSnapshotStore();

  index_id_t next_index_id_after_abort = 0;
  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    vid_t bob = 0;
    AddPersonTP(tp, 2, "Bob", 25, &bob);
    auto* index = dynamic_cast<ExampleIndex*>(GetIndexByName("idx_person_age"));
    ASSERT_NE(index, nullptr);
    next_index_id_after_abort = index->GetNextIndexID();
    ASSERT_GT(next_index_id_after_abort, 0);
    txn.Abort();
  }

  {
    auto txn = NewUpdateTransaction();
    StorageTPUpdateInterface tp(txn);
    vid_t charlie = 0;
    AddPersonTP(tp, 3, "Charlie", 30, &charlie);
    auto* index = dynamic_cast<ExampleIndex*>(GetIndexByName("idx_person_age"));
    ASSERT_NE(index, nullptr);
    EXPECT_GT(index->GetNextIndexID(), next_index_id_after_abort);
    Commit(txn);
  }

  EXPECT_EQ(SearchPersonNamesInCurrent(25), (std::vector<std::string>{}));
  EXPECT_EQ(SearchPersonNamesInCurrent(30),
            (std::vector<std::string>{"Charlie"}));
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
  ASSERT_TRUE(tp.UpdateVertexProperty(label, alice, age_col, Value::INT32(40)));
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
