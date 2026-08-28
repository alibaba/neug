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
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "neug/common/columns/value_columns.h"
#include "neug/common/types/data_chunk.h"
#include "neug/common/types/value.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/container/i_container.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/index/storage_index.h"
#include "neug/storages/index/storage_index_manager.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/storages/module/module_factory.h"
#include "neug/transaction/cow_graph_storage.h"
#include "neug/transaction/cow_graph_workspace.h"
#include "neug/utils/exception/exception.h"
#include "test_index_common.h"
#include "unittest/utils.h"

namespace neug {
namespace {

class FailingIndex : public ExampleIndex {
 public:
  enum class FailurePoint { kNone, kUpsert, kDelete };

  static void SetFailurePoint(FailurePoint failure_point) {
    failure_point_ = failure_point;
  }

  Status Delete(vid_t vid) override {
    if (failure_point_ == FailurePoint::kDelete) {
      return Status::InternalError("injected index delete failure");
    }
    return ExampleIndex::Delete(vid);
  }

 protected:
  std::unique_ptr<ExampleIndex> CreateClone() const override {
    return std::make_unique<FailingIndex>();
  }

  Status AppendImpl(index_id_t index_id, const IndexValues& values) override {
    if (failure_point_ == FailurePoint::kUpsert) {
      return Status::InternalError("injected index upsert failure");
    }
    return ExampleIndex::AppendImpl(index_id, values);
  }

 private:
  static inline FailurePoint failure_point_{FailurePoint::kNone};
};

TEST(ModuleDescriptorTest, RequiredDefaultsTrueAndRoundTripsFalse) {
  ModuleDescriptor required;
  EXPECT_TRUE(required.required);

  ModuleDescriptor optional;
  optional.module_type = "extension_module";
  optional.required = false;
  rapidjson::Document document;
  document.Parse(optional.ToJsonString().c_str());
  auto restored = ModuleDescriptor::FromJson(document);
  EXPECT_FALSE(restored.required);
}

TEST(IndexMetaTest, PreservesDetailedPropertyType) {
  IndexMeta meta;
  meta.name = "array_index";
  meta.type = "example";
  meta.schema.label_id = 7;
  meta.schema.label_name = "Array";
  meta.schema.columns.push_back(
      {"embedding", DataType::Array(DataType::FLOAT, 3)});

  auto json = meta.ToJsonString();
  rapidjson::Document document;
  document.Parse(json.c_str());
  ASSERT_TRUE(document.HasMember("schema"));
  ASSERT_TRUE(document["schema"].HasMember("columns"));
  ASSERT_EQ(document["schema"]["columns"].Size(), 1);
  const auto& column = document["schema"]["columns"][0];
  EXPECT_FALSE(column.HasMember("property_type"));
  ASSERT_TRUE(column.HasMember("property_type_detail"));
  EXPECT_TRUE(column["property_type_detail"].IsString());

  auto restored = IndexMeta::FromJsonString(json);
  EXPECT_EQ(restored.schema.label_name, meta.schema.label_name);
  ASSERT_EQ(restored.schema.columns.size(), 1);
  EXPECT_EQ(restored.schema.columns[0].property_type,
            meta.schema.columns[0].property_type);
}

TEST(IndexMetaTest, RejectsInvalidJson) {
  EXPECT_THROW(IndexMeta::FromJsonString("{invalid"), exception::RuntimeError);
  EXPECT_THROW(IndexMeta::FromJsonString("[]"), exception::RuntimeError);
}

class VectorChunkSupplier : public IDataChunkSupplier {
 public:
  explicit VectorChunkSupplier(std::vector<std::shared_ptr<DataChunk>> chunks)
      : chunks_(std::move(chunks)) {}

  std::shared_ptr<DataChunk> GetNextChunk() override {
    if (index_ >= chunks_.size()) {
      return nullptr;
    }
    return chunks_[index_++];
  }

  int64_t RowNum() const override {
    int64_t total = 0;
    for (const auto& chunk : chunks_) {
      total += static_cast<int64_t>(chunk->row_num());
    }
    return total;
  }

 private:
  std::vector<std::shared_ptr<DataChunk>> chunks_;
  size_t index_{0};
};

class FailingAfterFirstChunkSupplier : public IDataChunkSupplier {
 public:
  explicit FailingAfterFirstChunkSupplier(std::shared_ptr<DataChunk> chunk)
      : chunk_(std::move(chunk)) {}

  std::shared_ptr<DataChunk> GetNextChunk() override {
    if (chunk_) {
      return std::exchange(chunk_, nullptr);
    }
    THROW_RUNTIME_ERROR("injected supplier failure after first chunk");
  }

  int64_t RowNum() const override { return -1; }

 private:
  std::shared_ptr<DataChunk> chunk_;
};

template <typename T>
std::shared_ptr<IContextColumn> MakeValueColumn(const std::vector<T>& values) {
  ValueColumnBuilder<T> builder;
  builder.reserve(values.size());
  for (const auto& value : values) {
    builder.push_back_opt(value);
  }
  return builder.finish();
}

std::shared_ptr<IDataChunkSupplier> MakePersonSupplier(
    const std::vector<PersonRow>& rows) {
  std::vector<int64_t> ids;
  std::vector<std::string> names;
  std::vector<int32_t> ages;
  ids.reserve(rows.size());
  names.reserve(rows.size());
  ages.reserve(rows.size());
  for (const auto& row : rows) {
    ids.push_back(row.id);
    names.push_back(row.name);
    ages.push_back(row.age);
  }

  auto chunk = std::make_shared<DataChunk>();
  chunk->set(0, MakeValueColumn(ids));
  chunk->set(1, MakeValueColumn(names));
  chunk->set(2, MakeValueColumn(ages));
  std::vector<std::shared_ptr<DataChunk>> chunks;
  chunks.push_back(std::move(chunk));
  return std::make_shared<VectorChunkSupplier>(std::move(chunks));
}

std::shared_ptr<IDataChunkSupplier> MakeItemSupplier(
    const std::vector<std::pair<int32_t, int32_t>>& rows) {
  std::vector<int32_t> ids;
  std::vector<int32_t> values;
  for (const auto& [id, value] : rows) {
    ids.push_back(id);
    values.push_back(value);
  }
  auto chunk = std::make_shared<DataChunk>();
  chunk->set(0, MakeValueColumn(ids));
  chunk->set(1, MakeValueColumn(values));
  return std::make_shared<VectorChunkSupplier>(
      std::vector<std::shared_ptr<DataChunk>>{std::move(chunk)});
}

class APIndexTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    ModuleFactory::instance().Register(
        kExampleIndexType, [] { return std::make_unique<ExampleIndex>(); });
    ModuleFactory::instance().Register(
        "failing_index", [] { return std::make_unique<FailingIndex>(); });
    ModuleFactory::instance().Register(
        kVecIndexType, [] { return std::make_unique<VecIndex>(); });
  }

  void SetUp() override {
    work_dir_ = std::string("/tmp/test_ap_index_") +
                ::testing::UnitTest::GetInstance()->current_test_info()->name();
    std::filesystem::remove_all(work_dir_);
    std::filesystem::create_directories(work_dir_);
    OpenFreshGraph();
  }

  void TearDown() override {
    FailingIndex::SetFailurePoint(FailingIndex::FailurePoint::kNone);
    ap_.reset();
    workspace_.reset();
    view_.reset();
    graph_.reset();
    published_graph_.reset();
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
    published_graph_ = graph_;
    view_ = std::make_unique<GraphView>(*graph_);
    ResetStorageAdapter();
  }

  void ReopenGraph() {
    ap_.reset();
    workspace_.reset();
    view_.reset();
    graph_.reset();
    published_graph_.reset();
    checkpoint_mgr_.Close();
    checkpoint_mgr_.Open(work_dir_);
    ASSERT_NE(checkpoint_mgr_.Current(), nullptr);
    graph_ = std::make_shared<PropertyGraph>();
    graph_->Open(checkpoint_mgr_.Current(), MemoryLevel::kInMemory);
    published_graph_ = graph_;
    view_ = std::make_unique<GraphView>(*graph_);
    ResetStorageAdapter();
  }

  void ResetStorageAdapter() {
    if (workspace_ && workspace_->graph()) {
      published_graph_ = workspace_->graph();
    }
    StartPrivateWorkspace();
  }

  void AbortStorageAdapter() { StartPrivateWorkspace(); }

  void StartPrivateWorkspace() {
    ap_.reset();
    workspace_.reset();
    graph_ = published_graph_->Clone();
    view_ = std::make_unique<GraphView>(*graph_);
    workspace_.emplace(graph_, 0);
    ap_ = std::make_unique<BulkCowGraphStorage>(*workspace_, 0, 0, allocator_);
  }

  void CheckpointGraph() {
    auto staging = checkpoint_mgr_.CreateStaging();
    graph_->Compact();
    graph_->DumpAndClear(staging.checkpoint());
    staging.Publish();
  }

  void CheckpointDirtyAndReopen() {
    auto staging = checkpoint_mgr_.CreateStaging();
    graph_->DumpDirtyAndReopen(staging.checkpoint(), 1);
    staging.Publish();
    view_->Rebuild(*graph_);
    ReopenGraph();
  }

  void CreatePersonTable() {
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

  void CreateReplacementTable() {
    CreateVertexTypeParamBuilder builder;
    auto status =
        ap_->CreateVertexType(builder.VertexLabel("Replacement")
                                  .AddProperty("id", Value::INT64(0))
                                  .AddProperty("value", Value::INT32(0))
                                  .AddPrimaryKeyName("id")
                                  .Build());
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  void CreateItemTable() {
    CreateVertexTypeParamBuilder builder;
    auto status =
        ap_->CreateVertexType(builder.VertexLabel("Item")
                                  .AddProperty("id", Value::INT32(0))
                                  .AddProperty("value", Value::INT32(0))
                                  .AddPrimaryKeyName("id")
                                  .Build());
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  void CreateVectorTable() {
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

  result<StorageIndex*> CreateIndex(const std::string& name,
                                    const std::string& label_name,
                                    const std::string& property_name,
                                    const std::string& type = "example") {
    auto label = graph_->schema().get_vertex_label_id(label_name);
    auto schema = graph_->schema().get_vertex_schema(label);
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
    meta->type = type;
    meta->schema.label_id = label;
    meta->schema.columns.push_back({property_name, property_type});
    GS_AUTO(created, ap_->CreateIndex(std::move(meta)));
    return std::get<StorageIndex*>(created);
  }

  StorageIndex* GetIndex(const std::string& name) const {
    return graph_->index_manager().GetIndexByName(name).value_or(nullptr);
  }

  std::vector<StorageIndex*> GetIndexes(
      label_t label, const std::string& property_name) const {
    auto indexes = graph_->index_manager().GetIndex(label, {property_name});
    EXPECT_TRUE(indexes) << indexes.error().ToString();
    if (!indexes) {
      return {};
    }
    return indexes.value();
  }

  void AddPerson(int64_t id, const std::string& name, int32_t age,
                 vid_t* out = nullptr) {
    auto label = graph_->schema().get_vertex_label_id("Person");
    vid_t vid = 0;
    auto status = ap_->AddVertex(label, Value::INT64(id),
                                 {Value::STRING(name), Value::INT32(age)}, vid);
    ASSERT_TRUE(status.ok()) << status.ToString();
    if (out) {
      *out = vid;
    }
  }

  void AddReplacement(int64_t id, int32_t value) {
    auto label = graph_->schema().get_vertex_label_id("Replacement");
    vid_t vid = 0;
    auto status =
        ap_->AddVertex(label, Value::INT64(id), {Value::INT32(value)}, vid);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  vid_t AddVector(int64_t id, float first, float second) {
    auto label = graph_->schema().get_vertex_label_id("Vector");
    auto vector_type = DataType::Array(DataType::FLOAT, 2);
    auto vector =
        Value::ARRAY(vector_type, {Value::FLOAT(first), Value::FLOAT(second)});
    vid_t vid = 0;
    auto status = ap_->AddVertex(label, Value::INT64(id), {vector}, vid);
    EXPECT_TRUE(status.ok()) << status.ToString();
    return vid;
  }

  result<StorageIndex*> CreateVecIndex(
      const std::string& name,
      common::case_insensitive_map_t<std::string> options = {}) {
    auto label = graph_->schema().get_vertex_label_id("Vector");
    auto meta = std::make_unique<IndexMeta>();
    meta->name = name;
    meta->type = "hnsw";
    meta->schema.label_id = label;
    meta->schema.columns.push_back(
        {"embedding", DataType::Array(DataType::FLOAT, 2)});
    meta->options = std::move(options);
    GS_AUTO(created, ap_->CreateIndex(std::move(meta)));
    return std::get<StorageIndex*>(created);
  }

  std::vector<std::string> SearchPersonNames(int32_t age) const {
    auto* index = GetIndex("idx_person_age");
    EXPECT_NE(index, nullptr);
    if (!index) {
      return {};
    }
    ExampleIndexQueryParams params(age);
    auto result = index->Search(params);
    EXPECT_TRUE(result) << result.error().ToString();
    if (!result) {
      return {};
    }
    auto label = graph_->schema().get_vertex_label_id("Person");
    auto name_col = graph_->GetVertexPropertyColumn(label, "name");
    std::vector<std::string> names;
    for (const auto& entry : result.value()) {
      names.push_back(name_col->get_any(entry.vid).GetValue<std::string>());
    }
    std::sort(names.begin(), names.end());
    return names;
  }

  std::string work_dir_;
  CheckpointManager checkpoint_mgr_;
  std::shared_ptr<PropertyGraph> published_graph_;
  std::shared_ptr<PropertyGraph> graph_;
  std::unique_ptr<GraphView> view_;
  Allocator allocator_{MemoryLevel::kInMemory, ""};
  std::optional<CowGraphWorkspace> workspace_;
  std::unique_ptr<BulkCowGraphStorage> ap_;
};

TEST_F(APIndexTest, CreateIndexEmptyGraphAndDuplicateName) {
  CreatePersonTable();
  ResetStorageAdapter();

  auto created = CreateIndex("idx_person_age", "Person", "age");
  ASSERT_TRUE(created) << created.error().ToString();
  EXPECT_NE(created.value(), nullptr);
  EXPECT_TRUE(workspace_->PlanningChanged());

  ResetStorageAdapter();
  auto duplicate = CreateIndex("idx_person_age", "Person", "age");
  EXPECT_FALSE(duplicate);
  EXPECT_EQ(duplicate.error().error_code(), StatusCode::ERR_ILLEGAL_OPERATION);
  EXPECT_FALSE(workspace_->PlanningChanged());

  ASSERT_TRUE(ap_->DropIndex("idx_person_age").ok());
  EXPECT_TRUE(workspace_->PlanningChanged());
}

TEST_F(APIndexTest, DropMissingIndexReturnsNotFound) {
  auto status = ap_->DropIndex("missing_index");
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), StatusCode::ERR_NOT_FOUND);
  EXPECT_EQ(status.error_message(), "Index not found: missing_index");
}

TEST_F(APIndexTest, IndexMutationMarksWorkspacePlanningChanged) {
  CreateVectorTable();
  const auto label = graph_->schema().get_vertex_label_id("Vector");
  ResetStorageAdapter();

  auto created = CreateVecIndex("idx_vector_embedding");
  ASSERT_TRUE(created) << created.error().ToString();
  EXPECT_TRUE(workspace_->PlanningChanged());
  EXPECT_NE(GetIndex("idx_vector_embedding"), nullptr);
  EXPECT_NE(
      dynamic_cast<const VecColumn*>(
          graph_->get_vertex_table(label).GetPropertyColumnBase("embedding")),
      nullptr);

  ResetStorageAdapter();
  ASSERT_TRUE(ap_->DropIndex("idx_vector_embedding").ok());
  EXPECT_TRUE(workspace_->PlanningChanged());
  EXPECT_EQ(GetIndex("idx_vector_embedding"), nullptr);
  EXPECT_NE(
      dynamic_cast<const ArrayColumn*>(
          graph_->get_vertex_table(label).GetPropertyColumnBase("embedding")),
      nullptr);
}

TEST_F(APIndexTest, BulkBuildIndexesExistingVertices) {
  CreatePersonTable();
  for (const auto& person : kPersons) {
    AddPerson(person.id, person.name, person.age);
  }

  auto created = CreateIndex("idx_person_age", "Person", "age");
  ASSERT_TRUE(created) << created.error().ToString();

  EXPECT_EQ(SearchPersonNames(30),
            (std::vector<std::string>{"Alice", "Charlie"}));
  EXPECT_EQ(SearchPersonNames(25), (std::vector<std::string>{"Bob", "Eve"}));
  EXPECT_EQ(SearchPersonNames(40), (std::vector<std::string>{"Diana"}));
}

TEST_F(APIndexTest, VecIndexCreateSearchUpdateAndDrop) {
  CreateVectorTable();
  auto first_vid = AddVector(1, 1.0f, 1.0f);
  AddVector(2, 5.0f, 5.0f);
  auto third_vid = AddVector(3, 9.0f, 9.0f);

  auto label = graph_->schema().get_vertex_label_id("Vector");
  const auto& vertex_table = graph_->get_vertex_table(label);
  const auto* array = dynamic_cast<const ArrayColumn*>(
      vertex_table.GetPropertyColumnBase("embedding"));
  ASSERT_NE(array, nullptr);
  const void* original_buffer = array->shared_buffer<float>()->GetData();

  auto created = CreateVecIndex("idx_vector_embedding");
  ASSERT_TRUE(created) << created.error().ToString();
  auto* index = dynamic_cast<VecIndex*>(created.value());
  ASSERT_NE(index, nullptr);

  const auto* vec = dynamic_cast<const VecColumn*>(
      vertex_table.GetPropertyColumnBase("embedding"));
  ASSERT_NE(vec, nullptr);
  EXPECT_EQ(vec->get_buffer_ptr(), original_buffer);

  VecIndexQueryParams near_first({1.2f, 0.8f});
  auto first_result = index->Search(near_first);
  ASSERT_TRUE(first_result) << first_result.error().ToString();
  ASSERT_EQ(first_result->size(), 1);
  EXPECT_EQ(first_result->front().vid, first_vid);

  auto fourth_vid = AddVector(4, 12.0f, 12.0f);
  VecIndexQueryParams near_fourth({11.5f, 12.5f});
  auto fourth_result = index->Search(near_fourth);
  ASSERT_TRUE(fourth_result) << fourth_result.error().ToString();
  ASSERT_EQ(fourth_result->size(), 1);
  EXPECT_EQ(fourth_result->front().vid, fourth_vid);

  ASSERT_TRUE(ap_->DropIndex("idx_vector_embedding").ok());
  EXPECT_EQ(GetIndex("idx_vector_embedding"), nullptr);
  const auto* restored = dynamic_cast<const ArrayColumn*>(
      vertex_table.GetPropertyColumnBase("embedding"));
  ASSERT_NE(restored, nullptr);
  auto third_value = restored->get_any(third_vid);
  const auto& third_vector = ArrayValue::GetChildren(third_value);
  EXPECT_FLOAT_EQ(third_vector[0].GetValue<float>(), 9.0f);
  EXPECT_FLOAT_EQ(third_vector[1].GetValue<float>(), 9.0f);
}

TEST_F(APIndexTest, HnswNormalizeTransformsAndRestoresArrayColumnOnDrop) {
  CreateVectorTable();
  auto first_vid = AddVector(1, 3.0f, 4.0f);
  auto created =
      CreateVecIndex("idx_vector_normalized", {{"metric", "cosine"}});
  ASSERT_TRUE(created) << created.error().ToString();

  auto label = graph_->schema().get_vertex_label_id("Vector");
  auto& vertex_table = graph_->get_vertex_table(label);
  auto* vec = dynamic_cast<const VecColumn*>(
      vertex_table.GetPropertyColumnBase("embedding"));
  ASSERT_NE(vec, nullptr);
  EXPECT_TRUE(vec->is_l2_normalized());
  auto first = ArrayValue::GetChildren(vec->get_any(first_vid));
  EXPECT_NEAR(first[0].GetValue<float>(), 0.6f, 1e-6f);
  EXPECT_NEAR(first[1].GetValue<float>(), 0.8f, 1e-6f);

  auto second_vid = AddVector(2, 5.0f, 12.0f);
  auto second = ArrayValue::GetChildren(vec->get_any(second_vid));
  EXPECT_NEAR(second[0].GetValue<float>(), 5.0f / 13.0f, 1e-6f);
  EXPECT_NEAR(second[1].GetValue<float>(), 12.0f / 13.0f, 1e-6f);

  ASSERT_TRUE(ap_->DropIndex("idx_vector_normalized").ok());
  auto* restored = dynamic_cast<const ArrayColumn*>(
      vertex_table.GetPropertyColumnBase("embedding"));
  ASSERT_NE(restored, nullptr);
  auto retained_first = ArrayValue::GetChildren(restored->get_any(first_vid));
  EXPECT_NEAR(retained_first[0].GetValue<float>(), 0.6f, 1e-6f);
  EXPECT_NEAR(retained_first[1].GetValue<float>(), 0.8f, 1e-6f);

  auto third_vid = AddVector(3, 8.0f, 15.0f);
  auto third = ArrayValue::GetChildren(restored->get_any(third_vid));
  EXPECT_FLOAT_EQ(third[0].GetValue<float>(), 8.0f);
  EXPECT_FLOAT_EQ(third[1].GetValue<float>(), 15.0f);
}

TEST_F(APIndexTest, HnswNormalizeAcceptsZeroVector) {
  CreateVectorTable();
  auto zero_vid = AddVector(1, 0.0f, 0.0f);
  auto created =
      CreateVecIndex("idx_vector_normalized", {{"metric", "cosine"}});
  ASSERT_TRUE(created) << created.error().ToString();

  auto label = graph_->schema().get_vertex_label_id("Vector");
  const auto& vertex_table = graph_->get_vertex_table(label);
  const auto* vec = dynamic_cast<const VecColumn*>(
      vertex_table.GetPropertyColumnBase("embedding"));
  ASSERT_NE(vec, nullptr);
  EXPECT_TRUE(vec->is_l2_normalized());
  auto zero = ArrayValue::GetChildren(vec->get_any(zero_vid));
  EXPECT_FLOAT_EQ(zero[0].GetValue<float>(), 0.0f);
  EXPECT_FLOAT_EQ(zero[1].GetValue<float>(), 0.0f);
}

TEST_F(APIndexTest, HnswNormalizeRejectsNonFiniteVectorsAtomically) {
  CreateVectorTable();
  AddVector(1, std::numeric_limits<float>::infinity(), 0.0f);
  auto created =
      CreateVecIndex("idx_vector_normalized", {{"metric", "cosine"}});
  ASSERT_FALSE(created);
  EXPECT_EQ(created.error().error_code(), StatusCode::ERR_INVALID_ARGUMENT);
}

TEST_F(APIndexTest, CosineHnswWithNormalizationDisabledRequiresUnitSamples) {
  CreateVectorTable();
  AddVector(1, 3.0f, 4.0f);
  auto created =
      CreateVecIndex("idx_vector_cosine",
                     {{"metric", "cosine"}, {"cosine_normalize", "false"}});
  ASSERT_FALSE(created);
  EXPECT_EQ(created.error().error_code(), StatusCode::ERR_INVALID_ARGUMENT);
}

TEST_F(APIndexTest, CosineNormalizeIsIgnoredForL2) {
  CreateVectorTable();
  auto vid = AddVector(1, 3.0f, 4.0f);
  auto created = CreateVecIndex(
      "idx_vector_l2", {{"metric", "l2"}, {"cosine_normalize", "true"}});
  ASSERT_TRUE(created) << created.error().ToString();

  auto label = graph_->schema().get_vertex_label_id("Vector");
  const auto& vertex_table = graph_->get_vertex_table(label);
  const auto* vec = dynamic_cast<const VecColumn*>(
      vertex_table.GetPropertyColumnBase("embedding"));
  ASSERT_NE(vec, nullptr);
  EXPECT_FALSE(vec->is_l2_normalized());
  auto stored = ArrayValue::GetChildren(vec->get_any(vid));
  EXPECT_FLOAT_EQ(stored[0].GetValue<float>(), 3.0f);
  EXPECT_FLOAT_EQ(stored[1].GetValue<float>(), 4.0f);
}

TEST_F(APIndexTest, CosineNormalizeIsIgnoredForInnerProduct) {
  CreateVectorTable();
  auto vid = AddVector(1, 3.0f, 4.0f);
  auto created = CreateVecIndex(
      "idx_vector_ip", {{"metric", "ip"}, {"cosine_normalize", "true"}});
  ASSERT_TRUE(created) << created.error().ToString();

  auto label = graph_->schema().get_vertex_label_id("Vector");
  const auto& vertex_table = graph_->get_vertex_table(label);
  const auto* vec = dynamic_cast<const VecColumn*>(
      vertex_table.GetPropertyColumnBase("embedding"));
  ASSERT_NE(vec, nullptr);
  EXPECT_FALSE(vec->is_l2_normalized());
  auto stored = ArrayValue::GetChildren(vec->get_any(vid));
  EXPECT_FLOAT_EQ(stored[0].GetValue<float>(), 3.0f);
  EXPECT_FLOAT_EQ(stored[1].GetValue<float>(), 4.0f);
}

TEST_F(APIndexTest, HnswIndexRejectsPropertyWithNonHnswIndex) {
  CreateVectorTable();
  auto regular = CreateIndex("idx_vector_regular", "Vector", "embedding");
  ASSERT_TRUE(regular) << regular.error().ToString();

  auto hnsw = CreateVecIndex("idx_vector_hnsw");
  ASSERT_FALSE(hnsw);
  EXPECT_EQ(hnsw.error().error_code(), StatusCode::ERR_INVALID_ARGUMENT);
}

TEST_F(APIndexTest, NonHnswIndexRejectsPropertyWithHnswIndex) {
  CreateVectorTable();
  auto hnsw = CreateVecIndex("idx_vector_hnsw");
  ASSERT_TRUE(hnsw) << hnsw.error().ToString();

  auto regular = CreateIndex("idx_vector_regular", "Vector", "embedding");
  ASSERT_FALSE(regular);
  EXPECT_EQ(regular.error().error_code(), StatusCode::ERR_INVALID_ARGUMENT);

  ASSERT_TRUE(ap_->DropIndex("idx_vector_hnsw").ok());
  auto label = graph_->schema().get_vertex_label_id("Vector");
  const auto& vertex_table = graph_->get_vertex_table(label);
  EXPECT_NE(dynamic_cast<const ArrayColumn*>(
                vertex_table.GetPropertyColumnBase("embedding")),
            nullptr);
}

TEST_F(APIndexTest, CloneRebindsIndexToClonedPropertyColumn) {
  CreatePersonTable();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));

  auto* original = dynamic_cast<ExampleIndex*>(GetIndex("idx_person_age"));
  ASSERT_NE(original, nullptr);
  auto clone = graph_->Clone();
  auto cloned_index = clone->index_manager().GetIndexByName("idx_person_age");
  ASSERT_TRUE(cloned_index);
  auto* cloned = dynamic_cast<ExampleIndex*>(cloned_index.value());
  ASSERT_NE(cloned, nullptr);
  EXPECT_TRUE(cloned->IsBound());
  EXPECT_NE(cloned->BoundColumn(), original->BoundColumn());
}

TEST_F(APIndexTest, DropVertexTypeDeletesBoundIndex) {
  CreatePersonTable();
  CreateReplacementTable();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));

  auto person_label = graph_->schema().get_vertex_label_id("Person");
  ASSERT_EQ(GetIndexes(person_label, "age").size(), 1);

  auto status = ap_->DeleteVertexType(person_label);
  ASSERT_TRUE(status.ok()) << status.ToString();

  EXPECT_TRUE(GetIndexes(person_label, "age").empty());
}

TEST_F(APIndexTest, DropDeletesAndRenamePreservesBoundIndex) {
  CreatePersonTable();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));
  auto person_label = graph_->schema().get_vertex_label_id("Person");
  ASSERT_EQ(GetIndexes(person_label, "age").size(), 1);

  DeleteVertexPropertiesParamBuilder delete_builder;
  auto drop_status = ap_->DeleteVertexProperties(
      person_label, delete_builder.AddDeleteProperty("age").Build());
  ASSERT_TRUE(drop_status.ok()) << drop_status.ToString();
  EXPECT_TRUE(GetIndexes(person_label, "age").empty());

  AddVertexPropertiesParamBuilder add_builder;
  auto add_status = ap_->AddVertexProperties(
      person_label, add_builder.AddProperty("score", Value::INT32(0)).Build());
  ASSERT_TRUE(add_status.ok()) << add_status.ToString();

  ASSERT_TRUE(CreateIndex("idx_person_score", "Person", "score"));
  ASSERT_EQ(GetIndexes(person_label, "score").size(), 1);

  RenameVertexPropertiesParamBuilder rename_builder;
  auto rename_status = ap_->RenameVertexProperties(
      person_label, rename_builder.AddRenameProperty("score", "years").Build());
  ASSERT_TRUE(rename_status.ok()) << rename_status.ToString();

  EXPECT_TRUE(GetIndexes(person_label, "score").empty());
  auto renamed_indexes = GetIndexes(person_label, "years");
  ASSERT_EQ(renamed_indexes.size(), 1);
  EXPECT_EQ(renamed_indexes.front()->GetMeta().name, "idx_person_score");
  ASSERT_EQ(renamed_indexes.front()->GetMeta().schema.columns.size(), 1);
  EXPECT_EQ(renamed_indexes.front()->GetMeta().schema.columns[0].property_name,
            "years");
}

TEST_F(APIndexTest, InsertDeleteAndUpdateMaintainIndex) {
  CreatePersonTable();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));

  vid_t alice = 0;
  AddPerson(1, "Alice", 30, &alice);
  AddPerson(2, "Bob", 25);
  AddPerson(3, "Charlie", 30);
  EXPECT_EQ(SearchPersonNames(30),
            (std::vector<std::string>{"Alice", "Charlie"}));

  auto label = graph_->schema().get_vertex_label_id("Person");
  auto delete_status = ap_->DeleteVertex(label, alice);
  ASSERT_TRUE(delete_status.ok()) << delete_status.ToString();
  EXPECT_EQ(SearchPersonNames(30), (std::vector<std::string>{"Charlie"}));

  vid_t bob = 0;
  ASSERT_TRUE(ap_->GetVertexIndex(label, Value::INT64(2), bob));
  auto schema = graph_->schema().get_vertex_schema(label);
  auto age_it = std::find(schema->property_names.begin(),
                          schema->property_names.end(), "age");
  ASSERT_NE(age_it, schema->property_names.end());
  auto age_col =
      static_cast<int>(std::distance(schema->property_names.begin(), age_it));
  auto update_status =
      ap_->UpdateVertexProperty(label, bob, age_col, Value::INT32(30));
  ASSERT_TRUE(update_status.ok()) << update_status.ToString();
  EXPECT_EQ(SearchPersonNames(25), (std::vector<std::string>{}));
  EXPECT_EQ(SearchPersonNames(30),
            (std::vector<std::string>{"Bob", "Charlie"}));
}

TEST_F(APIndexTest, PrimaryKeyIndexMaintainedAcrossVertexLifecycle) {
  CreateItemTable();
  ASSERT_TRUE(CreateIndex("idx_item_id", "Item", "id"));
  auto label = graph_->schema().get_vertex_label_id("Item");
  auto* index = GetIndex("idx_item_id");
  ASSERT_NE(index, nullptr);

  vid_t first_vid = 0;
  ASSERT_TRUE(
      ap_->AddVertex(label, Value::INT32(1), {Value::INT32(10)}, first_vid)
          .ok());
  ExampleIndexQueryParams first_query(1);
  ASSERT_EQ(index->Search(first_query).value(),
            (std::vector<SearchResult>{{first_vid}}));

  auto batch_result =
      ap_->BatchAddVertices(label, MakeItemSupplier({{2, 20}, {3, 30}}));
  ASSERT_TRUE(batch_result) << batch_result.error().ToString();
  ExampleIndexQueryParams batch_query(2);
  ASSERT_EQ(index->Search(batch_query).value().size(), 1);

  ASSERT_TRUE(ap_->DeleteVertex(label, first_vid).ok());
  EXPECT_TRUE(index->Search(first_query).value().empty());

  DeleteVertexPropertiesParamBuilder delete_builder;
  EXPECT_THROW(ap_->DeleteVertexProperties(
                   label, delete_builder.AddDeleteProperty("id").Build()),
               exception::RuntimeError);
  EXPECT_NE(GetIndex("idx_item_id"), nullptr);

  ASSERT_TRUE(ap_->DeleteVertexType(label).ok());
  EXPECT_EQ(GetIndex("idx_item_id"), nullptr);
}

TEST_F(APIndexTest, BatchAddVerticesMaintainsIndexAndSkipsDuplicatePk) {
  CreatePersonTable();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));

  auto result = ap_->BatchAddVertices(
      graph_->schema().get_vertex_label_id("Person"),
      MakePersonSupplier({{1, "Alice", 30}, {2, "Bob", 25}}));
  ASSERT_TRUE(result) << result.error().ToString();
  EXPECT_EQ(SearchPersonNames(30), (std::vector<std::string>{"Alice"}));

  result = ap_->BatchAddVertices(
      graph_->schema().get_vertex_label_id("Person"),
      MakePersonSupplier(
          {{1, "Alice", 40}, {3, "Charlie", 30}, {4, "Diana", 40}}));
  ASSERT_TRUE(result) << result.error().ToString();

  EXPECT_EQ(SearchPersonNames(30),
            (std::vector<std::string>{"Alice", "Charlie"}));
  EXPECT_EQ(SearchPersonNames(25), (std::vector<std::string>{"Bob"}));
  EXPECT_EQ(SearchPersonNames(40), (std::vector<std::string>{"Diana"}));
}

TEST_F(APIndexTest, BatchLoadFinalizesVertexTimestampAndEdgeOrder) {
  CreateItemTable();
  const auto item = graph_->schema().get_vertex_label_id("Item");
  auto vertices = ap_->BatchAddVertices(
      item, MakeItemSupplier({{1, 10}, {2, 20}, {3, 30}}));
  ASSERT_TRUE(vertices) << vertices.error().ToString();

  CreateEdgeTypeParamBuilder edge_builder;
  auto create_edge =
      ap_->CreateEdgeType(edge_builder.SrcLabel("Item")
                              .DstLabel("Item")
                              .EdgeLabel("weighted")
                              .AddProperty("weight", Value::INT32(0))
                              .SortKeyForNbr("weight")
                              .Build());
  ASSERT_TRUE(create_edge.ok()) << create_edge.ToString();
  const auto weighted = graph_->schema().get_edge_label_id("weighted");

  auto edges = std::make_shared<DataChunk>();
  edges->set(0, MakeValueColumn(std::vector<int32_t>{1, 1}));
  edges->set(1, MakeValueColumn(std::vector<int32_t>{2, 3}));
  edges->set(2, MakeValueColumn(std::vector<int32_t>{20, 10}));
  auto add_edges = ap_->BatchAddEdges(
      item, item, weighted,
      std::make_shared<VectorChunkSupplier>(
          std::vector<std::shared_ptr<DataChunk>>{std::move(edges)}));
  ASSERT_TRUE(add_edges.ok()) << add_edges.ToString();

  CreateEdgeTypeParamBuilder plain_edge_builder;
  create_edge = ap_->CreateEdgeType(plain_edge_builder.SrcLabel("Item")
                                        .DstLabel("Item")
                                        .EdgeLabel("plain")
                                        .Build());
  ASSERT_TRUE(create_edge.ok()) << create_edge.ToString();
  const auto plain = graph_->schema().get_edge_label_id("plain");

  vid_t source = 0;
  vid_t second = 0;
  ASSERT_TRUE(ap_->GetVertexIndex(item, Value::INT32(1), source));
  ASSERT_TRUE(ap_->GetVertexIndex(item, Value::INT32(2), second));

  CowGraphStorage dml(*workspace_, 0, 7, allocator_);
  const void* property = nullptr;
  ASSERT_TRUE(
      dml.AddEdge(item, source, item, second, plain, {}, property).ok());

  auto plain_edges = std::make_shared<DataChunk>();
  plain_edges->set(0, MakeValueColumn(std::vector<int32_t>{1}));
  plain_edges->set(1, MakeValueColumn(std::vector<int32_t>{3}));
  add_edges = ap_->BatchAddEdges(
      item, item, plain,
      std::make_shared<VectorChunkSupplier>(
          std::vector<std::shared_ptr<DataChunk>>{std::move(plain_edges)}));
  ASSERT_TRUE(add_edges.ok()) << add_edges.ToString();

  const auto plain_edge_count = [&](timestamp_t timestamp) {
    const auto& edge_table = graph_->get_edge_table(item, item, plain);
    auto outgoing = edge_table.get_outgoing_view(timestamp).get_edges(source);
    size_t count = 0;
    for (auto it = outgoing.begin(); it != outgoing.end(); ++it) {
      ++count;
    }
    return count;
  };

  const auto expect_finalized = [&] {
    const auto& vertex_table = graph_->get_vertex_table(item);
    EXPECT_EQ(vertex_table.get_vertex_timestamp().InitVertexNum(),
              vertex_table.LidNum());

    const auto& edge_table = graph_->get_edge_table(item, item, weighted);
    auto edge_data = edge_table.get_edge_data_accessor("weight");
    auto outgoing =
        edge_table.get_outgoing_view(MAX_TIMESTAMP).get_edges(source);
    std::vector<int32_t> weights;
    for (auto it = outgoing.begin(); it != outgoing.end(); ++it) {
      weights.push_back(edge_data.get_typed_data<int32_t>(it));
    }
    EXPECT_EQ(weights, (std::vector<int32_t>{10, 20}));
    EXPECT_EQ(plain_edge_count(0), 2);
  };

  // Commit-time finalization has not run yet: the batch loader leaves a
  // timestamp-zero tail on the vertex table.
  EXPECT_EQ(
      graph_->get_vertex_table(item).get_vertex_timestamp().InitVertexNum(), 0);
  EXPECT_EQ(plain_edge_count(0), 1);

  // CommitCowWrite finalizes the recorded COPY targets right before the
  // checkpoint consumes the private graph; drive the same code path here.
  workspace_->FinalizeBulkTablesForCheckpoint();
  expect_finalized();
  CheckpointDirtyAndReopen();
  expect_finalized();
}

TEST_F(APIndexTest, PartialBatchFailureIsDiscardedWithPrivateWorkspace) {
  CreateItemTable();
  ResetStorageAdapter();
  const auto label = graph_->schema().get_vertex_label_id("Item");

  auto chunk = std::make_shared<DataChunk>();
  chunk->set(0, MakeValueColumn(std::vector<int32_t>{7}));
  chunk->set(1, MakeValueColumn(std::vector<int32_t>{70}));
  auto supplier = std::make_shared<FailingAfterFirstChunkSupplier>(chunk);

  EXPECT_THROW(
      {
        auto result = ap_->BatchAddVertices(label, std::move(supplier));
        (void) result;
      },
      exception::RuntimeError);
  AbortStorageAdapter();

  vid_t vid = 0;
  EXPECT_FALSE(ap_->GetVertexIndex(label, Value::INT32(7), vid));
}

TEST_F(APIndexTest, UpdateIndexFailureAbortsPrivateWorkspace) {
  CreateItemTable();
  const auto label = graph_->schema().get_vertex_label_id("Item");
  vid_t original_vid = 0;
  ASSERT_TRUE(
      ap_->AddVertex(label, Value::INT32(1), {Value::INT32(10)}, original_vid)
          .ok());
  ASSERT_TRUE(CreateIndex("idx_failing_value", "Item", "value", "failing"));
  CheckpointGraph();
  ReopenGraph();
  const auto checkpoint_id = checkpoint_mgr_.Current()->id();

  ASSERT_TRUE(ap_->GetVertexIndex(label, Value::INT32(1), original_vid));
  FailingIndex::SetFailurePoint(FailingIndex::FailurePoint::kUpsert);
  const auto status =
      ap_->UpdateVertexProperty(label, original_vid, 0, Value::INT32(20));
  EXPECT_FALSE(status.ok());
  FailingIndex::SetFailurePoint(FailingIndex::FailurePoint::kNone);
  AbortStorageAdapter();

  EXPECT_EQ(checkpoint_mgr_.Current()->id(), checkpoint_id);
  vid_t published_vid = 0;
  ASSERT_TRUE(ap_->GetVertexIndex(label, Value::INT32(1), published_vid));
  EXPECT_EQ(ap_->GetVertexProperty(label, published_vid, 0).GetValue<int32_t>(),
            10);
}

TEST_F(APIndexTest, DeleteIndexFailureAbortsPrivateWorkspace) {
  CreateItemTable();
  const auto label = graph_->schema().get_vertex_label_id("Item");
  vid_t original_vid = 0;
  ASSERT_TRUE(
      ap_->AddVertex(label, Value::INT32(1), {Value::INT32(10)}, original_vid)
          .ok());
  ASSERT_TRUE(CreateIndex("idx_failing_value", "Item", "value", "failing"));
  CheckpointGraph();
  ReopenGraph();
  const auto checkpoint_id = checkpoint_mgr_.Current()->id();

  ASSERT_TRUE(ap_->GetVertexIndex(label, Value::INT32(1), original_vid));
  FailingIndex::SetFailurePoint(FailingIndex::FailurePoint::kDelete);
  const auto status = ap_->DeleteVertex(label, original_vid);
  EXPECT_FALSE(status.ok());
  FailingIndex::SetFailurePoint(FailingIndex::FailurePoint::kNone);
  AbortStorageAdapter();

  EXPECT_EQ(checkpoint_mgr_.Current()->id(), checkpoint_id);
  vid_t published_vid = 0;
  ASSERT_TRUE(ap_->GetVertexIndex(label, Value::INT32(1), published_vid));
  EXPECT_EQ(ap_->GetVertexProperty(label, published_vid, 0).GetValue<int32_t>(),
            10);
}

TEST_F(APIndexTest, IndexPersistsAfterCheckpointReopen) {
  CreatePersonTable();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));
  auto* created = dynamic_cast<ExampleIndex*>(GetIndex("idx_person_age"));
  ASSERT_NE(created, nullptr);
  EXPECT_TRUE(created->IsBound());
  for (const auto& person : kPersons) {
    AddPerson(person.id, person.name, person.age);
  }
  EXPECT_EQ(SearchPersonNames(30),
            (std::vector<std::string>{"Alice", "Charlie"}));

  CheckpointGraph();
  ReopenGraph();

  EXPECT_NE(GetIndex("idx_person_age"), nullptr);
  auto* reopened = dynamic_cast<ExampleIndex*>(GetIndex("idx_person_age"));
  ASSERT_NE(reopened, nullptr);
  EXPECT_TRUE(reopened->IsBound());
  EXPECT_EQ(SearchPersonNames(30),
            (std::vector<std::string>{"Alice", "Charlie"}));
  EXPECT_EQ(SearchPersonNames(25), (std::vector<std::string>{"Bob", "Eve"}));
  EXPECT_EQ(SearchPersonNames(40), (std::vector<std::string>{"Diana"}));
}

TEST_F(APIndexTest, CheckpointRemapsIndexAfterTemporaryLabelIsStripped) {
  CreateVertexTypeParamBuilder builder;
  const auto status =
      ap_->CreateVertexType(builder.VertexLabel("Temporary")
                                .AddProperty("id", Value::INT64(0))
                                .AddPrimaryKeyName("id")
                                .Temporary(true)
                                .Build());
  ASSERT_TRUE(status.ok()) << status.ToString();
  CreatePersonTable();
  AddPerson(1, "Alice", 30);
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));

  ASSERT_EQ(graph_->schema().get_vertex_label_id("Person"), 1);
  auto first_staging = checkpoint_mgr_.CreateStaging();
  ASSERT_TRUE(graph_->DumpDirtyAndReopen(first_staging.checkpoint(), 1));
  first_staging.Publish();
  EXPECT_EQ(SearchPersonNames(30), (std::vector<std::string>{"Alice"}));
  auto second_staging = checkpoint_mgr_.CreateStaging();
  EXPECT_FALSE(graph_->DumpDirtyAndReopen(second_staging.checkpoint(), 1));
  second_staging.Publish();
  view_->Rebuild(*graph_);
  ReopenGraph();

  EXPECT_EQ(graph_->schema().get_vertex_label_id("Person"), 0);
  EXPECT_EQ(SearchPersonNames(30), (std::vector<std::string>{"Alice"}));
}

TEST_F(APIndexTest, IncrementalCheckpointRewritesOnlyMutatedIndex) {
  CreateItemTable();
  const auto label = graph_->schema().get_vertex_label_id("Item");
  vid_t item_vid = 0;
  ASSERT_TRUE(
      ap_->AddVertex(label, Value::INT32(1), {Value::INT32(10)}, item_vid)
          .ok());
  ASSERT_TRUE(CreateIndex("idx_item_id", "Item", "id"));
  ASSERT_TRUE(CreateIndex("idx_item_value", "Item", "value"));
  CheckpointGraph();
  ReopenGraph();

  auto base_checkpoint = checkpoint_mgr_.Current();
  ASSERT_NE(base_checkpoint, nullptr);
  const auto index_path = [](const Checkpoint& checkpoint,
                             const std::string& name) {
    const auto* descriptor =
        checkpoint.manifest().FindModule(StorageIndexManager::GetKey(name));
    EXPECT_NE(descriptor, nullptr);
    return descriptor == nullptr ? std::optional<std::string>()
                                 : descriptor->get_path(kIndexBufferPath);
  };
  const auto old_id_path = index_path(*base_checkpoint, "idx_item_id");
  const auto old_value_path = index_path(*base_checkpoint, "idx_item_value");
  ASSERT_TRUE(old_id_path.has_value());
  ASSERT_TRUE(old_value_path.has_value());
  auto* clean_index = GetIndex("idx_item_id");
  auto* dirty_index = GetIndex("idx_item_value");
  ASSERT_NE(clean_index, nullptr);
  ASSERT_NE(dirty_index, nullptr);

  ASSERT_TRUE(
      ap_->UpdateVertexProperty(label, item_vid, 0, Value::INT32(11)).ok());
  EXPECT_TRUE(graph_->IsModified());

  auto staging = checkpoint_mgr_.CreateStaging();
  EXPECT_FALSE(graph_->DumpDirtyAndReopen(staging.checkpoint(), 7));
  auto incremental_checkpoint = staging.Publish();
  view_->Rebuild(*graph_);

  EXPECT_EQ(index_path(*incremental_checkpoint, "idx_item_id"), old_id_path);
  EXPECT_NE(index_path(*incremental_checkpoint, "idx_item_value"),
            old_value_path);
  EXPECT_EQ(GetIndex("idx_item_id"), clean_index);
  EXPECT_NE(GetIndex("idx_item_value"), dirty_index);
  EXPECT_FALSE(graph_->IsModified());

  auto* value_index = GetIndex("idx_item_value");
  ASSERT_NE(value_index, nullptr);
  ExampleIndexQueryParams old_value(10);
  ExampleIndexQueryParams new_value(11);
  EXPECT_TRUE(value_index->Search(old_value).value().empty());
  EXPECT_EQ(value_index->Search(new_value).value(),
            (std::vector<SearchResult>{{item_vid}}));
}

TEST_F(APIndexTest, AutomaticallyDeletedIndexStaysDeletedAfterReopen) {
  CreatePersonTable();
  CreateReplacementTable();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));
  auto person_label = graph_->schema().get_vertex_label_id("Person");
  ASSERT_EQ(GetIndexes(person_label, "age").size(), 1);

  auto status = ap_->DeleteVertexType(person_label);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_TRUE(GetIndexes(person_label, "age").empty());
  CheckpointGraph();
  ReopenGraph();

  EXPECT_TRUE(GetIndexes(person_label, "age").empty());
  AddReplacement(1, 10);
}

}  // namespace
}  // namespace neug
