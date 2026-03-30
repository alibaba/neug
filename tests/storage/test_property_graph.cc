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

#include "neug/storages/graph/property_graph.h"

namespace neug {

class PropertyGraphTest : public ::testing::Test {
 protected:
  std::string work_dir_;
  std::unique_ptr<PropertyGraph> graph_;

  void SetUp() override {
    work_dir_ = std::string("/tmp/test_property_graph") +
                ::testing::UnitTest::GetInstance()->current_test_info()->name();
    if (std::filesystem::exists(work_dir_)) {
      std::filesystem::remove_all(work_dir_);
    }
    std::filesystem::create_directories(work_dir_);
    graph_ = std::make_unique<PropertyGraph>();
    graph_->Open(work_dir_, MemoryLevel::kInMemory);
  }

  void TearDown() override {
    graph_.reset();
    if (std::filesystem::exists(work_dir_)) {
      std::filesystem::remove_all(work_dir_);
    }
  }

  void CreateModernGraphSchema() {
    EXPECT_TRUE(graph_
                    ->CreateVertexType(
                        "person",
                        {
                            std::make_tuple(DataTypeId::kInt64, "id",
                                            Property::from_int64(0)),
                            std::make_tuple(DataTypeId::kVarchar, "name",
                                            Property::from_string_view("")),
                            std::make_tuple(DataTypeId::kInt32, "age",
                                            Property::from_int32(0)),
                            std::make_tuple(DataTypeId::kDouble, "score",
                                            Property::from_double(0.0)),
                        },
                        {"id"})
                    .ok());
    EXPECT_TRUE(graph_
                    ->CreateVertexType(
                        "company",
                        {
                            std::make_tuple(DataTypeId::kInt64, "id",
                                            Property::from_int64(0)),
                            std::make_tuple(DataTypeId::kVarchar, "name",
                                            Property::from_string_view("")),
                        },
                        {"id"})
                    .ok());
    EXPECT_TRUE(
        graph_
            ->CreateEdgeType("person", "person", "knows",
                             {
                                 std::make_tuple(DataTypeId::kDouble, "weight",
                                                 Property::from_double(0.0)),
                             })
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
          ->AddVertex(person_label, Property::from_int64(1),
                      {Property::from_string_view("Alice"),
                       Property::from_int32(30), Property::from_double(88.5)},
                      vid1, 0)
          .ok());
  EXPECT_TRUE(
      graph_
          ->AddVertex(person_label, Property::from_int64(2),
                      {Property::from_string_view("Bob"),
                       Property::from_int32(25), Property::from_double(92.0)},
                      vid2, 0)
          .ok());
  auto id_column = graph_->GetVertexPropertyColumn(person_label, "id");
  EXPECT_TRUE(id_column);
  EXPECT_EQ(id_column->get(vid1).as_int64(), 1);
  EXPECT_EQ(id_column->get(vid2).as_int64(), 2);

  // By default, we will reserve 4096 slots for each vertex label.
  for (size_t i = 3; i <= 4096; ++i) {
    vid_t vid;
    graph_->AddVertex(person_label, Property::from_int64(i),
                      {Property::from_string_view("User" + std::to_string(i)),
                       Property::from_int32(20 + (i % 10)),
                       Property::from_double(80.0 + (i % 20))},
                      vid, 0);
  }
  EXPECT_EQ(graph_->VertexNum(person_label), 4096);
  vid_t vid4097;
  EXPECT_FALSE(
      graph_
          ->AddVertex(person_label, Property::from_int64(4097),
                      {Property::from_string_view("User4097"),
                       Property::from_int32(27), Property::from_double(85.0)},
                      vid4097, 0)
          .ok());

  Allocator allocator(MemoryLevel::kInMemory, "");
  for (vid_t i = 0; i < 4094; ++i) {
    graph_->AddEdge(person_label, i, person_label, i + 1, knows_label,
                    {Property::from_double(1.0)}, MAX_TIMESTAMP, allocator);
  }
  EXPECT_THROW(
      graph_->AddEdge(person_label, 4095, person_label, 4096, knows_label,
                      {Property::from_double(1.0)}, MAX_TIMESTAMP, allocator),
      exception::Exception);
}

}  // namespace neug