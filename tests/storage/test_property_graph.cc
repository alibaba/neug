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

#include "neug/execution/common/types/value.h"
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
                            .AddProperty("id", execution::Value::INT64(0))
                            .AddProperty("name", execution::Value::STRING(""))
                            .AddProperty("age", execution::Value::INT32(0))
                            .AddProperty("score", execution::Value::DOUBLE(0.0))
                            .AddPrimaryKeyName("id")
                            .Build())
                    .ok());
    CreateVertexTypeParamBuilder company_builder;
    EXPECT_TRUE(graph_
                    ->CreateVertexType(
                        company_builder.VertexLabel("company")
                            .AddProperty("id", execution::Value::INT64(0))
                            .AddProperty("name", execution::Value::STRING(""))
                            .AddPrimaryKeyName("id")
                            .Build())
                    .ok());
    CreateEdgeTypeParamBuilder knows_builder;
    EXPECT_TRUE(
        graph_
            ->CreateEdgeType(
                knows_builder.SrcLabel("person")
                    .DstLabel("person")
                    .EdgeLabel("knows")
                    .AddProperty("weight", execution::Value::DOUBLE(0.0))
                    .Build())
            .ok());
  }
};

TEST_F(PropertyGraphTest, TestOpenAndBulkInsert) {
  CreateModernGraphSchema();
  label_t person_label = graph_->schema().get_vertex_label_id("person");
  label_t knows_label = graph_->schema().get_edge_label_id("knows");

  vid_t vid1, vid2;
  EXPECT_TRUE(graph_
                  ->AddVertex(person_label, execution::Value::INT64(1),
                              {execution::Value::STRING("Alice"),
                               execution::Value::INT32(30),
                               execution::Value::DOUBLE(88.5)},
                              vid1, 0)
                  .ok());
  EXPECT_TRUE(graph_
                  ->AddVertex(person_label, execution::Value::INT64(2),
                              {execution::Value::STRING("Bob"),
                               execution::Value::INT32(25),
                               execution::Value::DOUBLE(92.0)},
                              vid2, 0)
                  .ok());
  auto id_column = graph_->GetVertexPropertyColumn(person_label, "id");
  EXPECT_TRUE(id_column);
  EXPECT_EQ(id_column->get_any(vid1).GetValue<int64_t>(), 1);
  EXPECT_EQ(id_column->get_any(vid2).GetValue<int64_t>(), 2);

  // By default, we will reserve 4096 slots for each vertex label.
  for (size_t i = 3; i <= 4096; ++i) {
    vid_t vid;
    graph_->AddVertex(person_label, execution::Value::INT64(i),
                      {execution::Value::STRING("User" + std::to_string(i)),
                       execution::Value::INT32(20 + (i % 10)),
                       execution::Value::DOUBLE(80.0 + (i % 20))},
                      vid, 0);
  }
  EXPECT_EQ(graph_->VertexNum(person_label), 4096);
  vid_t vid4097;
  EXPECT_FALSE(graph_
                   ->AddVertex(person_label, execution::Value::INT64(4097),
                               {execution::Value::STRING("User4097"),
                                execution::Value::INT32(27),
                                execution::Value::DOUBLE(85.0)},
                               vid4097, 0)
                   .ok());

  Allocator allocator(MemoryLevel::kInMemory, "");
  for (vid_t i = 0; i < 4094; ++i) {
    int32_t oe_offset = 0;
    const void* prop = nullptr;
    graph_->AddEdge(person_label, i, person_label, i + 1, knows_label,
                    {execution::Value::DOUBLE(1.0)}, MAX_TIMESTAMP, allocator,
                    oe_offset, prop);
  }
  {
    int32_t oe_offset = 0;
    const void* prop = nullptr;
    EXPECT_FALSE(graph_
                     ->AddEdge(person_label, 4095, person_label, 4096,
                               knows_label, {execution::Value::DOUBLE(1.0)},
                               MAX_TIMESTAMP, allocator, oe_offset, prop)
                     .ok());
  }
}

}  // namespace neug