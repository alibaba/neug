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
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/container/i_container.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/index/storage_index.h"
#include "neug/storages/index/storage_index_manager.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/storages/module/module_factory.h"
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
  EXPECT_THROW(IndexMeta::FromJsonString("{invalid"), std::runtime_error);
  EXPECT_THROW(IndexMeta::FromJsonString("[]"), std::runtime_error);
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

class APIndexTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    ModuleFactory::instance().Register(
        kExampleIndexType, [] { return std::make_unique<ExampleIndex>(); });
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
    auto ckp =
        checkpoint_mgr_.GetCheckpoint(checkpoint_mgr_.CreateCheckpoint());
    graph_ = std::make_unique<PropertyGraph>();
    graph_->Open(ckp, MemoryLevel::kInMemory);
    view_ = std::make_unique<GraphView>(*graph_);
    ap_ = std::make_unique<StorageAPUpdateInterface>(*graph_, *view_, 0,
                                                     allocator_);
  }

  void ReopenGraph() {
    ap_.reset();
    view_.reset();
    graph_.reset();
    checkpoint_mgr_.Close();
    checkpoint_mgr_.Open(work_dir_);
    ASSERT_NE(checkpoint_mgr_.HeadId(), kInvalidCheckpointId);
    graph_ = std::make_unique<PropertyGraph>();
    graph_->Open(checkpoint_mgr_.GetCheckpoint(checkpoint_mgr_.HeadId()),
                 MemoryLevel::kInMemory);
    view_ = std::make_unique<GraphView>(*graph_);
    ap_ = std::make_unique<StorageAPUpdateInterface>(*graph_, *view_, 0,
                                                     allocator_);
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

  result<StorageIndex*> CreateIndex(const std::string& name,
                                    const std::string& label_name,
                                    const std::string& property_name) {
    auto label = graph_->schema().get_vertex_label_id(label_name);
    auto schema = graph_->schema().get_vertex_schema(label);
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
    return ap_->CreateIndex(std::move(meta));
  }

  StorageIndex* GetIndex(const std::string& name) const {
    return graph_->index_manager().GetIndexByName(name);
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
    for (auto vid : result.value()) {
      names.push_back(name_col->get_any(vid).GetValue<std::string>());
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
};

TEST_F(APIndexTest, CreateIndexEmptyGraphAndDuplicateName) {
  CreatePersonTable();

  auto created = CreateIndex("idx_person_age", "Person", "age");
  ASSERT_TRUE(created) << created.error().ToString();
  EXPECT_NE(created.value(), nullptr);

  auto duplicate = CreateIndex("idx_person_age", "Person", "age");
  EXPECT_FALSE(duplicate);
  EXPECT_EQ(duplicate.error().error_code(), StatusCode::ERR_SCHEMA_MISMATCH);
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

TEST_F(APIndexTest, CloneRebindsIndexToClonedPropertyColumn) {
  CreatePersonTable();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));

  auto* original = dynamic_cast<ExampleIndex*>(GetIndex("idx_person_age"));
  ASSERT_NE(original, nullptr);
  auto clone = graph_->Clone();
  auto* cloned = dynamic_cast<ExampleIndex*>(
      clone->index_manager().GetIndexByName("idx_person_age"));
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

  auto status = ap_->DeleteVertexType("Person");
  ASSERT_TRUE(status.ok()) << status.ToString();

  EXPECT_TRUE(GetIndexes(person_label, "age").empty());
}

TEST_F(APIndexTest, DropAndRenameVertexPropertyDeleteBoundIndex) {
  CreatePersonTable();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));
  auto person_label = graph_->schema().get_vertex_label_id("Person");
  ASSERT_EQ(GetIndexes(person_label, "age").size(), 1);

  DeleteVertexPropertiesParamBuilder delete_builder;
  auto drop_status = ap_->DeleteVertexProperties(
      delete_builder.VertexLabel("Person").AddDeleteProperty("age").Build());
  ASSERT_TRUE(drop_status.ok()) << drop_status.ToString();
  EXPECT_TRUE(GetIndexes(person_label, "age").empty());

  AddVertexPropertiesParamBuilder add_builder;
  auto add_status =
      ap_->AddVertexProperties(add_builder.VertexLabel("Person")
                                   .AddProperty("score", Value::INT32(0))
                                   .Build());
  ASSERT_TRUE(add_status.ok()) << add_status.ToString();

  ASSERT_TRUE(CreateIndex("idx_person_score", "Person", "score"));
  ASSERT_EQ(GetIndexes(person_label, "score").size(), 1);

  RenameVertexPropertiesParamBuilder rename_builder;
  auto rename_status =
      ap_->RenameVertexProperties(rename_builder.VertexLabel("Person")
                                      .AddRenameProperty("score", "years")
                                      .Build());
  ASSERT_TRUE(rename_status.ok()) << rename_status.ToString();

  EXPECT_TRUE(GetIndexes(person_label, "score").empty());
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

TEST_F(APIndexTest, BatchAddVerticesMaintainsIndexAndSkipsDuplicatePk) {
  CreatePersonTable();
  ASSERT_TRUE(CreateIndex("idx_person_age", "Person", "age"));

  auto status = ap_->BatchAddVertices(
      graph_->schema().get_vertex_label_id("Person"),
      MakePersonSupplier({{1, "Alice", 30}, {2, "Bob", 25}}));
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(SearchPersonNames(30), (std::vector<std::string>{"Alice"}));

  status = ap_->BatchAddVertices(
      graph_->schema().get_vertex_label_id("Person"),
      MakePersonSupplier(
          {{1, "Alice", 40}, {3, "Charlie", 30}, {4, "Diana", 40}}));
  ASSERT_TRUE(status.ok()) << status.ToString();

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

  ap_->CreateCheckpoint();
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

  auto status = ap_->DeleteVertexType("Person");
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_TRUE(GetIndexes(person_label, "age").empty());
  ap_->CreateCheckpoint();
  ReopenGraph();

  EXPECT_TRUE(GetIndexes(person_label, "age").empty());
  AddReplacement(1, 10);
}

}  // namespace
}  // namespace neug
