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
#include <functional>
#include <memory>
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
#include "neug/utils/exception/exception.h"
#include "test_index_common.h"
#include "unittest/utils.h"

namespace neug {
namespace {

TEST(IndexMetaTest, PreservesDetailedPropertyType) {
  IndexMeta meta;
  meta.name = "array_index";
  meta.type = "example";
  meta.schema.label_id = 7;
  meta.schema.property_name = "embedding";
  meta.schema.property_type = DataType::Array(DataType::FLOAT, 3);

  auto json = meta.ToJsonString();
  rapidjson::Document document;
  document.Parse(json.c_str());
  ASSERT_TRUE(document.HasMember("schema"));
  EXPECT_FALSE(document["schema"].HasMember("property_type"));
  ASSERT_TRUE(document["schema"].HasMember("property_type_detail"));
  EXPECT_TRUE(document["schema"]["property_type_detail"].IsString());

  auto restored = IndexMeta::FromJsonString(json);
  EXPECT_EQ(restored.schema.property_type, meta.schema.property_type);
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
    ap_.reset();
    view_.reset();
    graph_.reset();
    checkpoint_mgr_.Close();
    std::filesystem::remove_all(work_dir_);
  }

  void OpenFreshGraph() {
    checkpoint_mgr_.Open(work_dir_);
    auto staging = checkpoint_mgr_.CreateStagingCheckpoint();
    CheckpointManifest meta;
    meta.SetSchema(Schema());
    staging.checkpoint()->UpdateMeta(std::move(meta));
    auto ckp = staging.Commit();
    graph_ = std::make_unique<PropertyGraph>();
    graph_->Open(ckp, MemoryLevel::kInMemory);
    view_ = std::make_unique<GraphView>(*graph_);
    ap_ = std::make_unique<StorageAPUpdateInterface>(
        *graph_, *view_, 0, allocator_, [this]() {
          ++planning_change_count_;
          if (planning_change_hook_) {
            planning_change_hook_();
          }
        });
  }

  void ReopenGraph() {
    ap_.reset();
    view_.reset();
    graph_.reset();
    checkpoint_mgr_.Close();
    checkpoint_mgr_.Open(work_dir_);
    ASSERT_TRUE(checkpoint_mgr_.HasCurrentCheckpoint());
    graph_ = std::make_unique<PropertyGraph>();
    graph_->Open(checkpoint_mgr_.CurrentCheckpoint(), MemoryLevel::kInMemory);
    view_ = std::make_unique<GraphView>(*graph_);
    ap_ = std::make_unique<StorageAPUpdateInterface>(
        *graph_, *view_, 0, allocator_, [this]() {
          ++planning_change_count_;
          if (planning_change_hook_) {
            planning_change_hook_();
          }
        });
  }

  void CheckpointGraph() {
    auto staging = checkpoint_mgr_.CreateStagingCheckpoint();
    graph_->DumpAndClear(staging.checkpoint());
    staging.Commit();
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
                                    const std::string& property_name) {
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
    meta->type = "example";
    meta->schema.label_id = label;
    meta->schema.property_name = property_name;
    meta->schema.property_type = property_type;
    return ap_->CreateIndex(std::move(meta));
  }

  StorageIndex* GetIndex(const std::string& name) const {
    return graph_->index_manager().GetIndexByName(name).value_or(nullptr);
  }

  std::vector<StorageIndex*> GetIndexes(
      label_t label, const std::string& property_name) const {
    auto indexes = graph_->index_manager().GetIndex(label, property_name);
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

  result<StorageIndex*> CreateVecIndex(const std::string& name) {
    auto label = graph_->schema().get_vertex_label_id("Vector");
    auto meta = std::make_unique<IndexMeta>();
    meta->name = name;
    meta->type = "hnsw";
    meta->schema.label_id = label;
    meta->schema.property_name = "embedding";
    meta->schema.property_type = DataType::Array(DataType::FLOAT, 2);
    return ap_->CreateIndex(std::move(meta));
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
  std::unique_ptr<PropertyGraph> graph_;
  std::unique_ptr<GraphView> view_;
  Allocator allocator_{MemoryLevel::kInMemory, ""};
  std::unique_ptr<StorageAPUpdateInterface> ap_;
  int planning_change_count_{0};
  std::function<void()> planning_change_hook_;
};

TEST_F(APIndexTest, CreateIndexEmptyGraphAndDuplicateName) {
  CreatePersonTable();
  planning_change_count_ = 0;

  auto created = CreateIndex("idx_person_age", "Person", "age");
  ASSERT_TRUE(created) << created.error().ToString();
  EXPECT_NE(created.value(), nullptr);
  EXPECT_EQ(planning_change_count_, 1);

  auto duplicate = CreateIndex("idx_person_age", "Person", "age");
  EXPECT_FALSE(duplicate);
  EXPECT_EQ(duplicate.error().error_code(), StatusCode::ERR_ILLEGAL_OPERATION);
  EXPECT_EQ(planning_change_count_, 1);

  ASSERT_TRUE(ap_->DropIndex("idx_person_age").ok());
  EXPECT_EQ(planning_change_count_, 2);
}

TEST_F(APIndexTest, DropMissingIndexReturnsInvalidArgument) {
  auto status = ap_->DropIndex("missing_index");
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), StatusCode::ERR_INVALID_ARGUMENT);
  EXPECT_EQ(status.error_message(), "Index not found: missing_index");
}

TEST_F(APIndexTest, IndexMutationMarksPlanningChangedBeforeViewRebuild) {
  CreateVectorTable();
  const auto label = graph_->schema().get_vertex_label_id("Vector");
  const auto& vertex_table = graph_->get_vertex_table(label);
  planning_change_count_ = 0;

  bool create_callback_saw_array = false;
  planning_change_hook_ = [&]() {
    create_callback_saw_array =
        dynamic_cast<const ArrayColumn*>(
            vertex_table.GetPropertyColumnBase("embedding")) != nullptr;
    EXPECT_NE(GetIndex("idx_vector_embedding"), nullptr);
  };
  auto created = CreateVecIndex("idx_vector_embedding");
  ASSERT_TRUE(created) << created.error().ToString();
  EXPECT_TRUE(create_callback_saw_array);
  EXPECT_EQ(planning_change_count_, 1);

  bool drop_callback_saw_vec = false;
  planning_change_hook_ = [&]() {
    drop_callback_saw_vec =
        dynamic_cast<const VecColumn*>(
            vertex_table.GetPropertyColumnBase("embedding")) != nullptr;
    EXPECT_EQ(GetIndex("idx_vector_embedding"), nullptr);
  };
  ASSERT_TRUE(ap_->DropIndex("idx_vector_embedding").ok());
  EXPECT_TRUE(drop_callback_saw_vec);
  EXPECT_EQ(planning_change_count_, 2);
  planning_change_hook_ = {};
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
  EXPECT_EQ(renamed_indexes.front()->GetMeta().schema.property_name, "years");
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
