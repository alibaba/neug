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
#include <memory>
#include <string>

#include "neug/compiler/planner/gopt_planner.h"
#include "neug/storages/allocators.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph/operation_params.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema_view.h"
#include "unittest/utils.h"

#ifdef BUILD_HTTP_SERVER
#include "neug/neug.h"
#include "neug/server/neug_db_service.h"
#include "neug/transaction/update_transaction.h"
#endif

namespace neug {

class SchemaViewTest : public ::testing::Test {
 protected:
  void SetUp() override {
    work_dir_ = std::string("/tmp/test_schema_view_") +
                ::testing::UnitTest::GetInstance()->current_test_info()->name();
    std::filesystem::remove_all(work_dir_);
    std::filesystem::create_directories(work_dir_);

    checkpoint_manager_.Open(work_dir_);
    graph_ = std::make_unique<PropertyGraph>();
    graph_->Open(make_checkpoint(checkpoint_manager_), MemoryLevel::kInMemory);
    graph_view_ = std::make_unique<GraphView>(*graph_);
    allocator_ = std::make_unique<Allocator>(MemoryLevel::kInMemory, work_dir_);
    graph_interface_ = std::make_unique<StorageAPUpdateInterface>(
        *graph_, *graph_view_, 0, *allocator_);
  }

  void TearDown() override {
    graph_interface_.reset();
    graph_view_.reset();
    graph_.reset();
    allocator_.reset();
    std::filesystem::remove_all(work_dir_);
  }

  void CreateVertex(const std::string& label,
                    const std::string& namespace_name) {
    CreateVertexTypeParamBuilder builder;
    ASSERT_TRUE(graph_interface_
                    ->CreateVertexType(builder.VertexLabel(label)
                                           .AddProperty("id", Value::INT64(0))
                                           .AddPrimaryKeyName("id")
                                           .Namespace(namespace_name)
                                           .Build())
                    .ok());
  }

  void CreateSelfEdge(const std::string& vertex_label,
                      const std::string& edge_label,
                      const std::string& namespace_name) {
    CreateEdgeTypeParamBuilder builder;
    ASSERT_TRUE(graph_interface_
                    ->CreateEdgeType(builder.SrcLabel(vertex_label)
                                         .DstLabel(vertex_label)
                                         .EdgeLabel(edge_label)
                                         .Namespace(namespace_name)
                                         .Build())
                    .ok());
  }

  std::string work_dir_;
  CheckpointManager checkpoint_manager_;
  std::unique_ptr<PropertyGraph> graph_;
  std::unique_ptr<GraphView> graph_view_;
  std::unique_ptr<Allocator> allocator_;
  std::unique_ptr<StorageAPUpdateInterface> graph_interface_;
};

TEST_F(SchemaViewTest, IsolatesVertexAndEdgeTypesByNamespace) {
  CreateVertex("DefaultPerson", "default");
  CreateSelfEdge("DefaultPerson", "DefaultKnows", "default");
  CreateVertex("FtsDocument", "fts");
  CreateSelfEdge("FtsDocument", "FtsReferences", "fts");

  const auto& schema = graph_->schema();
  SchemaView default_view(schema, "default");
  SchemaView fts_view(schema, "fts");

  const auto default_vertex_id = schema.get_vertex_label_id("DefaultPerson");
  const auto fts_vertex_id = schema.get_vertex_label_id("FtsDocument");
  const auto default_edge_id = schema.get_edge_label_id("DefaultKnows");
  const auto fts_edge_id = schema.get_edge_label_id("FtsReferences");

  EXPECT_TRUE(default_view.ContainsVertexLabel("DefaultPerson"));
  EXPECT_TRUE(default_view.ContainsEdgeLabel("DefaultKnows"));
  EXPECT_TRUE(default_view.ContainsEdgeTriplet("DefaultPerson", "DefaultPerson",
                                               "DefaultKnows"));
  EXPECT_FALSE(default_view.ContainsVertexLabel("FtsDocument"));
  EXPECT_FALSE(default_view.ContainsEdgeLabel("FtsReferences"));
  EXPECT_FALSE(default_view.ContainsEdgeTriplet("FtsDocument", "FtsDocument",
                                                "FtsReferences"));

  auto default_vertex_by_id = default_view.GetVertexSchema(default_vertex_id);
  ASSERT_TRUE(default_vertex_by_id);
  EXPECT_EQ(default_vertex_by_id.value()->label_name, "DefaultPerson");
  auto default_vertex_by_name = default_view.GetVertexSchema("DefaultPerson");
  ASSERT_TRUE(default_vertex_by_name);
  EXPECT_EQ(default_vertex_by_name.value()->label_id, default_vertex_id);
  auto default_edge_by_id = default_view.GetEdgeSchema(
      default_vertex_id, default_vertex_id, default_edge_id);
  ASSERT_TRUE(default_edge_by_id);
  EXPECT_EQ(default_edge_by_id.value()->edge_label_name, "DefaultKnows");
  auto default_edge_by_name = default_view.GetEdgeSchema(
      "DefaultPerson", "DefaultPerson", "DefaultKnows");
  ASSERT_TRUE(default_edge_by_name);
  EXPECT_EQ(default_edge_by_name.value()->edge_label_id, default_edge_id);

  auto hidden_vertex_by_id = default_view.GetVertexSchema(fts_vertex_id);
  ASSERT_FALSE(hidden_vertex_by_id);
  EXPECT_EQ(hidden_vertex_by_id.error().error_code(),
            StatusCode::ERR_NOT_FOUND);
  auto hidden_vertex_by_name = default_view.GetVertexSchema("FtsDocument");
  ASSERT_FALSE(hidden_vertex_by_name);
  EXPECT_EQ(hidden_vertex_by_name.error().error_code(),
            StatusCode::ERR_NOT_FOUND);
  auto hidden_edge_by_id =
      default_view.GetEdgeSchema(fts_vertex_id, fts_vertex_id, fts_edge_id);
  ASSERT_FALSE(hidden_edge_by_id);
  EXPECT_EQ(hidden_edge_by_id.error().error_code(), StatusCode::ERR_NOT_FOUND);
  auto hidden_edge_by_name =
      default_view.GetEdgeSchema("FtsDocument", "FtsDocument", "FtsReferences");
  ASSERT_FALSE(hidden_edge_by_name);
  EXPECT_EQ(hidden_edge_by_name.error().error_code(),
            StatusCode::ERR_NOT_FOUND);

  EXPECT_TRUE(fts_view.ContainsVertexLabel("FtsDocument"));
  EXPECT_TRUE(fts_view.ContainsEdgeLabel("FtsReferences"));
  EXPECT_TRUE(fts_view.ContainsEdgeTriplet("FtsDocument", "FtsDocument",
                                           "FtsReferences"));
  EXPECT_FALSE(fts_view.ContainsVertexLabel("DefaultPerson"));
  EXPECT_FALSE(fts_view.ContainsEdgeLabel("DefaultKnows"));
  EXPECT_FALSE(fts_view.ContainsEdgeTriplet("DefaultPerson", "DefaultPerson",
                                            "DefaultKnows"));
  EXPECT_FALSE(fts_view.GetVertexSchema(default_vertex_id));
  EXPECT_FALSE(fts_view.GetEdgeSchema(default_vertex_id, default_vertex_id,
                                      default_edge_id));

  ASSERT_EQ(default_view.GetVertexSchemas().size(), 1u);
  EXPECT_EQ(default_view.GetVertexSchemas()[0]->label_name, "DefaultPerson");
  ASSERT_EQ(default_view.GetEdgeSchemas().size(), 1u);
  EXPECT_EQ(default_view.GetEdgeSchemas()[0]->edge_label_name, "DefaultKnows");
  ASSERT_EQ(fts_view.GetVertexSchemas().size(), 1u);
  EXPECT_EQ(fts_view.GetVertexSchemas()[0]->label_name, "FtsDocument");
  ASSERT_EQ(fts_view.GetEdgeSchemas().size(), 1u);
  EXPECT_EQ(fts_view.GetEdgeSchemas()[0]->edge_label_name, "FtsReferences");
}

TEST_F(SchemaViewTest, GoptPlannerUsesDefaultNamespace) {
  CreateVertex("DefaultPerson", "default");
  CreateVertex("FtsDocument", "fts");

  GOptPlanner planner;
  GraphStats stats;
  auto default_result = planner.compilePlan("MATCH (n:DefaultPerson) RETURN n",
                                            &graph_->schema(), stats);
  EXPECT_TRUE(default_result.has_value());

  auto fts_result = planner.compilePlan("MATCH (n:FtsDocument) RETURN n",
                                        &graph_->schema(), stats);
  EXPECT_FALSE(fts_result.has_value());
}

TEST_F(SchemaViewTest, HidesEdgesWhoseEndpointWasDeleted) {
  CreateVertex("DefaultPerson", "default");
  CreateSelfEdge("DefaultPerson", "DefaultKnows", "default");

  const auto vertex_id = graph_->schema().get_vertex_label_id("DefaultPerson");
  const auto edge_id = graph_->schema().get_edge_label_id("DefaultKnows");
  ASSERT_TRUE(graph_interface_->DeleteVertexType("DefaultPerson").ok());

  SchemaView view(graph_->schema(), "default");
  EXPECT_TRUE(view.GetVertexSchemas().empty());
  EXPECT_TRUE(view.GetEdgeSchemas().empty());
  EXPECT_FALSE(view.ContainsEdgeLabel(edge_id));
  EXPECT_FALSE(view.ContainsEdgeTriplet(vertex_id, vertex_id, edge_id));

  auto edge_schema = view.GetEdgeSchema(vertex_id, vertex_id, edge_id);
  ASSERT_FALSE(edge_schema);
  EXPECT_EQ(edge_schema.error().error_code(), StatusCode::ERR_NOT_FOUND);
}

#ifdef BUILD_HTTP_SERVER
TEST(StorageTPSchemaViewTest,
     StorageTPUpdateInterfaceIsolatesTypesByNamespaceAfterCommit) {
  const auto work_dir =
      std::string("/tmp/test_schema_view_") +
      ::testing::UnitTest::GetInstance()->current_test_info()->name();
  std::filesystem::remove_all(work_dir);

  NeugDB db;
  NeugDBConfig config(work_dir);
  config.memory_level = MemoryLevel::kInMemory;
  ASSERT_TRUE(db.Open(config));
  auto service = std::make_shared<NeugDBService>(db);

  {
    auto session = service->AcquireSession();
    auto txn = session->GetUpdateTransaction();
    StorageTPUpdateInterface graph_interface(txn);

    auto create_vertex = [&](const std::string& label,
                             const std::string& namespace_name) {
      CreateVertexTypeParamBuilder builder;
      return graph_interface.CreateVertexType(
          builder.VertexLabel(label)
              .AddProperty("id", Value::INT64(0))
              .AddPrimaryKeyName("id")
              .Namespace(namespace_name)
              .Build());
    };
    auto create_self_edge = [&](const std::string& vertex_label,
                                const std::string& edge_label,
                                const std::string& namespace_name) {
      CreateEdgeTypeParamBuilder builder;
      return graph_interface.CreateEdgeType(builder.SrcLabel(vertex_label)
                                                .DstLabel(vertex_label)
                                                .EdgeLabel(edge_label)
                                                .Namespace(namespace_name)
                                                .Build());
    };

    ASSERT_TRUE(create_vertex("DefaultPerson", "default").ok());
    ASSERT_TRUE(
        create_self_edge("DefaultPerson", "DefaultKnows", "default").ok());
    ASSERT_TRUE(create_vertex("FtsDocument", "fts").ok());
    ASSERT_TRUE(create_self_edge("FtsDocument", "FtsReferences", "fts").ok());

    SchemaView default_view(txn.schema(), "default");
    SchemaView fts_view(txn.schema(), "fts");
    EXPECT_TRUE(default_view.ContainsVertexLabel("DefaultPerson"));
    EXPECT_TRUE(default_view.ContainsEdgeLabel("DefaultKnows"));
    EXPECT_FALSE(default_view.ContainsVertexLabel("FtsDocument"));
    EXPECT_FALSE(default_view.ContainsEdgeLabel("FtsReferences"));
    EXPECT_TRUE(fts_view.ContainsVertexLabel("FtsDocument"));
    EXPECT_TRUE(fts_view.ContainsEdgeLabel("FtsReferences"));
    EXPECT_FALSE(fts_view.ContainsVertexLabel("DefaultPerson"));
    EXPECT_FALSE(fts_view.ContainsEdgeLabel("DefaultKnows"));

    ASSERT_TRUE(txn.Commit());
  }

  {
    auto session = service->AcquireSession();
    auto txn = session->GetReadTransaction();
    const auto& schema = txn.schema();
    SchemaView default_view(schema, "default");
    SchemaView fts_view(schema, "fts");

    const auto default_vertex_id = schema.get_vertex_label_id("DefaultPerson");
    const auto fts_vertex_id = schema.get_vertex_label_id("FtsDocument");
    const auto default_edge_id = schema.get_edge_label_id("DefaultKnows");
    const auto fts_edge_id = schema.get_edge_label_id("FtsReferences");

    EXPECT_TRUE(default_view.ContainsEdgeTriplet(
        "DefaultPerson", "DefaultPerson", "DefaultKnows"));
    EXPECT_FALSE(default_view.ContainsEdgeTriplet("FtsDocument", "FtsDocument",
                                                  "FtsReferences"));
    auto hidden_fts_vertex = default_view.GetVertexSchema(fts_vertex_id);
    ASSERT_FALSE(hidden_fts_vertex);
    EXPECT_EQ(hidden_fts_vertex.error().error_code(),
              StatusCode::ERR_NOT_FOUND);
    auto hidden_fts_edge =
        default_view.GetEdgeSchema(fts_vertex_id, fts_vertex_id, fts_edge_id);
    ASSERT_FALSE(hidden_fts_edge);
    EXPECT_EQ(hidden_fts_edge.error().error_code(), StatusCode::ERR_NOT_FOUND);

    EXPECT_TRUE(fts_view.ContainsEdgeTriplet("FtsDocument", "FtsDocument",
                                             "FtsReferences"));
    EXPECT_FALSE(fts_view.ContainsEdgeTriplet("DefaultPerson", "DefaultPerson",
                                              "DefaultKnows"));
    auto hidden_default_vertex = fts_view.GetVertexSchema(default_vertex_id);
    ASSERT_FALSE(hidden_default_vertex);
    EXPECT_EQ(hidden_default_vertex.error().error_code(),
              StatusCode::ERR_NOT_FOUND);
    auto hidden_default_edge = fts_view.GetEdgeSchema(
        default_vertex_id, default_vertex_id, default_edge_id);
    ASSERT_FALSE(hidden_default_edge);
    EXPECT_EQ(hidden_default_edge.error().error_code(),
              StatusCode::ERR_NOT_FOUND);
  }

  db.Close();
  std::filesystem::remove_all(work_dir);
}
#endif

}  // namespace neug
