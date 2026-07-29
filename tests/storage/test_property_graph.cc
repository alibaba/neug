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

#include <optional>
#include <utility>
#include <vector>

#include "neug/common/types/value.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/graph/property_graph.h"
#include "unittest/utils.h"

namespace neug {

class PropertyGraphTest : public ::testing::Test {
 protected:
  std::string work_dir_;
  std::unique_ptr<PropertyGraph> graph_;
  CheckpointManager checkpoint_mgr_;

  void SetUp() override {
    work_dir_ = std::string("/tmp/test_property_graph") +
                ::testing::UnitTest::GetInstance()->current_test_info()->name();
    if (std::filesystem::exists(work_dir_)) {
      std::filesystem::remove_all(work_dir_);
    }
    std::filesystem::create_directories(work_dir_);
    graph_ = std::make_unique<PropertyGraph>();
    checkpoint_mgr_.Open(work_dir_);
    auto ckp = make_checkpoint(checkpoint_mgr_);
    graph_->Open(ckp, MemoryLevel::kInMemory);
  }

  void TearDown() override {
    graph_.reset();
    if (std::filesystem::exists(work_dir_)) {
      std::filesystem::remove_all(work_dir_);
    }
  }

  void CreateModernGraphSchema() {
    CreateVertexTypeParamBuilder person_builder;
    EXPECT_TRUE(graph_
                    ->CreateVertexType(
                        person_builder.VertexLabel("person")
                            .AddProperty("id", neug::Value::INT64(0))
                            .AddProperty("name", neug::Value::STRING(""))
                            .AddProperty("age", neug::Value::INT32(0))
                            .AddProperty("score", neug::Value::DOUBLE(0.0))
                            .AddPrimaryKeyName("id")
                            .Build())
                    .ok());
    CreateVertexTypeParamBuilder company_builder;
    EXPECT_TRUE(
        graph_
            ->CreateVertexType(company_builder.VertexLabel("company")
                                   .AddProperty("id", neug::Value::INT64(0))
                                   .AddProperty("name", neug::Value::STRING(""))
                                   .AddPrimaryKeyName("id")
                                   .Build())
            .ok());
    CreateEdgeTypeParamBuilder knows_builder;
    EXPECT_TRUE(graph_
                    ->CreateEdgeType(
                        knows_builder.SrcLabel("person")
                            .DstLabel("person")
                            .EdgeLabel("knows")
                            .AddProperty("weight", neug::Value::DOUBLE(0.0))
                            .Build())
                    .ok());
  }
};

TEST_F(PropertyGraphTest, TestOpenAndBulkInsert) {
  CreateModernGraphSchema();
  label_t person_label = graph_->schema().get_vertex_label_id("person");
  label_t knows_label = graph_->schema().get_edge_label_id("knows");

  vid_t vid1, vid2;
  EXPECT_TRUE(
      graph_
          ->AddVertex(person_label, neug::Value::INT64(1),
                      {neug::Value::STRING("Alice"), neug::Value::INT32(30),
                       neug::Value::DOUBLE(88.5)},
                      vid1, 0)
          .ok());
  EXPECT_TRUE(
      graph_
          ->AddVertex(person_label, neug::Value::INT64(2),
                      {neug::Value::STRING("Bob"), neug::Value::INT32(25),
                       neug::Value::DOUBLE(92.0)},
                      vid2, 0)
          .ok());
  auto id_column = graph_->GetVertexPropertyColumn(person_label, "id");
  EXPECT_TRUE(id_column);
  EXPECT_EQ(id_column->get_any(vid1).GetValue<int64_t>(), 1);
  EXPECT_EQ(id_column->get_any(vid2).GetValue<int64_t>(), 2);

  // By default, we will reserve 4096 slots for each vertex label.
  for (size_t i = 3; i <= 4096; ++i) {
    vid_t vid;
    graph_->AddVertex(person_label, neug::Value::INT64(i),
                      {neug::Value::STRING("User" + std::to_string(i)),
                       neug::Value::INT32(20 + (i % 10)),
                       neug::Value::DOUBLE(80.0 + (i % 20))},
                      vid, 0);
  }
  EXPECT_EQ(graph_->VertexNum(person_label), 4096);
  vid_t vid4097;
  EXPECT_FALSE(
      graph_
          ->AddVertex(person_label, neug::Value::INT64(4097),
                      {neug::Value::STRING("User4097"), neug::Value::INT32(27),
                       neug::Value::DOUBLE(85.0)},
                      vid4097, 0)
          .ok());

  Allocator allocator(MemoryLevel::kInMemory, "");
  for (vid_t i = 0; i < 4094; ++i) {
    int32_t oe_offset = 0;
    const void* prop = nullptr;
    graph_->AddEdge(person_label, i, person_label, i + 1, knows_label,
                    {neug::Value::DOUBLE(1.0)}, MAX_TIMESTAMP, allocator,
                    oe_offset, prop);
  }
  {
    int32_t oe_offset = 0;
    const void* prop = nullptr;
    EXPECT_FALSE(graph_
                     ->AddEdge(person_label, 4095, person_label, 4096,
                               knows_label, {neug::Value::DOUBLE(1.0)},
                               MAX_TIMESTAMP, allocator, oe_offset, prop)
                     .ok());
  }
}

TEST_F(PropertyGraphTest, CheckpointSortsEdgesByConfiguredKey) {
  CreateVertexTypeParamBuilder vertex_builder;
  ASSERT_TRUE(graph_
                  ->CreateVertexType(vertex_builder.VertexLabel("person")
                                         .AddProperty("id", Value::INT64(0))
                                         .AddPrimaryKeyName("id")
                                         .Build())
                  .ok());
  CreateEdgeTypeParamBuilder edge_builder;
  ASSERT_TRUE(graph_
                  ->CreateEdgeType(
                      edge_builder.SrcLabel("person")
                          .DstLabel("person")
                          .EdgeLabel("knows")
                          .AddProperty("weight", Value::INT64(0))
                          .SortKeyForNbr(std::optional<std::string>{"weight"})
                          .Build())
                  .ok());

  const label_t person = graph_->schema().get_vertex_label_id("person");
  const label_t knows = graph_->schema().get_edge_label_id("knows");
  std::vector<vid_t> vertices;
  for (int64_t id = 0; id != 4; ++id) {
    vid_t vertex;
    ASSERT_TRUE(
        graph_->AddVertex(person, Value::INT64(id), {}, vertex, 0).ok());
    vertices.push_back(vertex);
  }

  Allocator allocator(MemoryLevel::kInMemory, "");
  for (const auto& [dst, weight] : std::vector<std::pair<vid_t, int64_t>>{
           {vertices[1], 30}, {vertices[2], 10}, {vertices[3], 20}}) {
    int32_t offset;
    const void* property = nullptr;
    ASSERT_TRUE(graph_
                    ->AddEdge(person, vertices[0], person, dst, knows,
                              {Value::INT64(weight)}, 7, allocator, offset,
                              property)
                    .ok());
  }
  graph_->MarkVertexTableDirty(person);
  graph_->MarkEdgeTableDirty(person, person, knows);

  auto checkpoint = make_checkpoint(checkpoint_mgr_);
  graph_->DumpAndClear(checkpoint);

  std::vector<vid_t> neighbors;
  std::vector<int64_t> weights;
  PropertyGraph reopened;
  reopened.Open(checkpoint, MemoryLevel::kInMemory);
  auto view = reopened.get_edge_table(person, person, knows)
                  .get_outgoing_view(MAX_TIMESTAMP);
  ASSERT_EQ(view.type(), CsrViewType::kMultipleMutable);
  auto typed_view =
      view.get_typed_view<int64_t, CsrViewType::kMultipleMutable>();
  EXPECT_EQ(typed_view.unsorted_since, 1);
  auto edges = view.get_edges(vertices[0]);
  for (auto it = edges.begin(); it != edges.end(); ++it) {
    neighbors.push_back(it.get_vertex());
    weights.push_back(*static_cast<const int64_t*>(it.get_data_ptr()));
  }
  EXPECT_EQ(neighbors,
            (std::vector<vid_t>{vertices[2], vertices[3], vertices[1]}));
  EXPECT_EQ(weights, (std::vector<int64_t>{10, 20, 30}));
}

}  // namespace neug
