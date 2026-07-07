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
#include <utility>
#include <vector>

#include "neug/storages/allocators.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/graph/graph_view.h"
#include "neug/storages/graph/operation_params.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/utils/exception/exception.h"
#include "unittest/utils.h"

namespace neug {

class GraphViewTest : public ::testing::Test {
 protected:
  std::string work_dir_;
  std::unique_ptr<PropertyGraph> graph_;
  CheckpointManager checkpoint_mgr_;
  // Owns the buffers backing the SetUp edges' adjacency lists. Must outlive
  // graph_; tests that need to mutate the graph reuse this allocator so the
  // SetUp buffers stay live alongside any new ones they add.
  std::unique_ptr<Allocator> alloc_;

  void SetUp() override {
    work_dir_ = std::string("/tmp/test_graph_view") +
                ::testing::UnitTest::GetInstance()->current_test_info()->name();
    if (std::filesystem::exists(work_dir_)) {
      std::filesystem::remove_all(work_dir_);
    }
    std::filesystem::create_directories(work_dir_);
    graph_ = std::make_unique<PropertyGraph>();
    checkpoint_mgr_.Open(work_dir_);
    auto ckp = make_checkpoint(checkpoint_mgr_);
    graph_->Open(ckp, MemoryLevel::kInMemory);

    // Create vertex type: person with id as primary key and name as property
    CreateVertexTypeParamBuilder person_builder;
    ASSERT_TRUE(
        graph_
            ->CreateVertexType(person_builder.VertexLabel("person")
                                   .AddProperty("id", neug::Value::INT64(0))
                                   .AddProperty("name", neug::Value::STRING(""))
                                   .AddPrimaryKeyName("id")
                                   .Build())
            .ok());

    // Create edge type: knows
    CreateEdgeTypeParamBuilder knows_builder;
    ASSERT_TRUE(graph_
                    ->CreateEdgeType(
                        knows_builder.SrcLabel("person")
                            .DstLabel("person")
                            .EdgeLabel("knows")
                            .AddProperty("weight", neug::Value::DOUBLE(0.0))
                            .Build())
                    .ok());

    // Add vertices
    label_t person_label = graph_->schema().get_vertex_label_id("person");
    vid_t vid1, vid2, vid3;
    ASSERT_TRUE(graph_
                    ->AddVertex(person_label, neug::Value::INT64(1),
                                {neug::Value::STRING("Alice")}, vid1, 0, false)
                    .ok());
    ASSERT_TRUE(graph_
                    ->AddVertex(person_label, neug::Value::INT64(2),
                                {neug::Value::STRING("Bob")}, vid2, 0, false)
                    .ok());
    ASSERT_TRUE(graph_
                    ->AddVertex(person_label, neug::Value::INT64(3),
                                {neug::Value::STRING("Charlie")}, vid3, 0,
                                false)
                    .ok());

    label_t knows_label = graph_->schema().get_edge_label_id("knows");
    alloc_ = std::make_unique<Allocator>(MemoryLevel::kInMemory, work_dir_);
    int32_t oe_offset = 0;
    const void* edge_prop = nullptr;
    ASSERT_TRUE(graph_
                    ->AddEdge(person_label, vid1, person_label, vid2,
                              knows_label, {neug::Value::DOUBLE(0.5)}, 0,
                              *alloc_, oe_offset, edge_prop, false)
                    .ok());
    ASSERT_TRUE(graph_
                    ->AddEdge(person_label, vid2, person_label, vid3,
                              knows_label, {neug::Value::DOUBLE(0.7)}, 0,
                              *alloc_, oe_offset, edge_prop, false)
                    .ok());
  }

  void TearDown() override {
    graph_.reset();
    alloc_.reset();
    if (std::filesystem::exists(work_dir_)) {
      std::filesystem::remove_all(work_dir_);
    }
  }
};

TEST_F(GraphViewTest, Construction) {
  GraphView view(*graph_);

  EXPECT_EQ(view.schema().vertex_label_num(), 1u);
}

TEST_F(GraphViewTest, GetLid) {
  GraphView view(*graph_);
  label_t person_label = view.schema().get_vertex_label_id("person");

  vid_t lid;
  EXPECT_TRUE(view.get_lid(person_label, neug::Value::INT64(1), lid, 0));
  EXPECT_TRUE(view.get_lid(person_label, neug::Value::INT64(2), lid, 0));
  EXPECT_TRUE(view.get_lid(person_label, neug::Value::INT64(3), lid, 0));
  EXPECT_FALSE(view.get_lid(person_label, neug::Value::INT64(999), lid, 0));
}

TEST_F(GraphViewTest, GetOid) {
  GraphView view(*graph_);
  label_t person_label = view.schema().get_vertex_label_id("person");

  auto oid0 = view.GetOid(person_label, 0, MAX_TIMESTAMP);
  EXPECT_EQ(oid0.type(), DataTypeId::kInt64);
  EXPECT_EQ(oid0.GetValue<int64_t>(), 1);

  EXPECT_EQ(view.GetOid(person_label, 1, MAX_TIMESTAMP).GetValue<int64_t>(), 2);
  EXPECT_EQ(view.GetOid(person_label, 2, MAX_TIMESTAMP).GetValue<int64_t>(), 3);
}

TEST_F(GraphViewTest, SchemaAccess) {
  GraphView view(*graph_);

  EXPECT_EQ(view.schema().vertex_label_num(), 1);
  EXPECT_EQ(view.schema().edge_label_num(), 1);
}

TEST_F(GraphViewTest, IsValidLid) {
  GraphView view(*graph_);
  label_t person_label = view.schema().get_vertex_label_id("person");

  EXPECT_TRUE(view.IsValidLid(person_label, 0, 0));
  EXPECT_TRUE(view.IsValidLid(person_label, 1, 0));
  EXPECT_TRUE(view.IsValidLid(person_label, 2, 0));
  // Out-of-range lid should not be valid.
  EXPECT_FALSE(view.IsValidLid(person_label, 3, 0));
  EXPECT_FALSE(view.IsValidLid(person_label, 1000, 0));
}

TEST_F(GraphViewTest, GetVertexPropertyColumnByName) {
  GraphView view(*graph_);
  label_t person_label = view.schema().get_vertex_label_id("person");

  // Primary-key fast path: "id" returns a non-null column wrapping the
  // indexer's keys.
  auto pk_col = view.GetVertexPropertyColumn(person_label, "id");
  ASSERT_NE(pk_col, nullptr);
  EXPECT_EQ(pk_col->get_any(0).GetValue<int64_t>(), 1);
  EXPECT_EQ(pk_col->get_any(1).GetValue<int64_t>(), 2);
  EXPECT_EQ(pk_col->get_any(2).GetValue<int64_t>(), 3);

  // Non-PK property goes through the underlying Table.
  auto name_col = view.GetVertexPropertyColumn(person_label, "name");
  ASSERT_NE(name_col, nullptr);

  // Unknown property returns nullptr.
  EXPECT_EQ(view.GetVertexPropertyColumn(person_label, "does_not_exist"),
            nullptr);
}

TEST_F(GraphViewTest, GetVertexPropertyColumnByIdSkipsPk) {
  GraphView view(*graph_);
  label_t person_label = view.schema().get_vertex_label_id("person");

  auto col0 = view.GetVertexPropertyColumn(person_label, 0);
  ASSERT_NE(col0, nullptr);

  // Negative / out-of-range ids return null rather than throw.
  EXPECT_EQ(view.GetVertexPropertyColumn(person_label, -1), nullptr);
  EXPECT_EQ(view.GetVertexPropertyColumn(person_label, 100), nullptr);
}

TEST_F(GraphViewTest, EdgeBasicTraversal) {
  GraphView view(*graph_);
  label_t person_label = view.schema().get_vertex_label_id("person");
  label_t knows_label = view.schema().get_edge_label_id("knows");

  CsrView out_csr =
      view.GetGenericOutgoingView(person_label, person_label, knows_label, 0);

  // Collect (src_oid, dst_oid) tuples by traversing through the view alone.
  std::vector<std::pair<int64_t, int64_t>> edges;
  for (vid_t v = 0; v < 3; ++v) {
    auto nbrs = out_csr.get_edges(v);
    for (auto it = nbrs.begin(); it != nbrs.end(); ++it) {
      edges.emplace_back(
          view.GetOid(person_label, v, MAX_TIMESTAMP).GetValue<int64_t>(),
          view.GetOid(person_label, it.get_vertex(), MAX_TIMESTAMP)
              .GetValue<int64_t>());
    }
  }

  EXPECT_EQ(edges.size(), 2u);
  // SetUp inserts 1->2 and 2->3.
  EXPECT_NE(std::find(edges.begin(), edges.end(),
                      std::make_pair<int64_t, int64_t>(1, 2)),
            edges.end());
  EXPECT_NE(std::find(edges.begin(), edges.end(),
                      std::make_pair<int64_t, int64_t>(2, 3)),
            edges.end());
}

TEST_F(GraphViewTest, EdgeIncomingMirrorsOutgoing) {
  GraphView view(*graph_);
  label_t person_label = view.schema().get_vertex_label_id("person");
  label_t knows_label = view.schema().get_edge_label_id("knows");

  CsrView out_csr =
      view.GetGenericOutgoingView(person_label, person_label, knows_label, 0);
  CsrView in_csr =
      view.GetGenericIncomingView(person_label, person_label, knows_label, 0);

  size_t out_count = 0;
  size_t in_count = 0;
  for (vid_t v = 0; v < 3; ++v) {
    for (auto it = out_csr.get_edges(v).begin();
         it != out_csr.get_edges(v).end(); ++it) {
      ++out_count;
    }
    for (auto it = in_csr.get_edges(v).begin(); it != in_csr.get_edges(v).end();
         ++it) {
      ++in_count;
    }
  }
  EXPECT_EQ(out_count, in_count);
  EXPECT_EQ(out_count, 2u);
}

TEST_F(GraphViewTest, EdgeDataAccessorByIdAndName) {
  GraphView view(*graph_);
  label_t person_label = view.schema().get_vertex_label_id("person");
  label_t knows_label = view.schema().get_edge_label_id("knows");

  auto by_id =
      view.GetEdgeDataAccessor(person_label, person_label, knows_label, 0);
  auto by_name = view.GetEdgeDataAccessor(person_label, person_label,
                                          knows_label, "weight");
  // Both overloads must agree on the underlying column / type.
  EXPECT_EQ(by_id.is_bundled(), by_name.is_bundled());

  // Iterate edges and read weights via the accessor.
  CsrView out_csr =
      view.GetGenericOutgoingView(person_label, person_label, knows_label, 0);
  std::vector<double> weights;
  for (vid_t v = 0; v < 3; ++v) {
    auto nbrs = out_csr.get_edges(v);
    for (auto it = nbrs.begin(); it != nbrs.end(); ++it) {
      weights.push_back(by_id.get_typed_data<double>(it));
    }
  }
  std::sort(weights.begin(), weights.end());
  ASSERT_EQ(weights.size(), 2u);
  EXPECT_DOUBLE_EQ(weights[0], 0.5);
  EXPECT_DOUBLE_EQ(weights[1], 0.7);
}

TEST_F(GraphViewTest, EdgeDataAccessorErrorPaths) {
  GraphView view(*graph_);
  label_t person_label = view.schema().get_vertex_label_id("person");
  label_t knows_label = view.schema().get_edge_label_id("knows");

  // Out-of-range column id throws.
  EXPECT_THROW(
      view.GetEdgeDataAccessor(person_label, person_label, knows_label, 99),
      exception::InvalidArgumentException);
  EXPECT_THROW(
      view.GetEdgeDataAccessor(person_label, person_label, knows_label, -1),
      exception::InvalidArgumentException);
  // Unknown property name throws.
  EXPECT_THROW(view.GetEdgeDataAccessor(person_label, person_label, knows_label,
                                        "missing"),
               exception::InvalidArgumentException);
}

TEST_F(GraphViewTest, InvalidEdgeTripletThrows) {
  GraphView view(*graph_);
  label_t person_label = view.schema().get_vertex_label_id("person");
  label_t knows_label = view.schema().get_edge_label_id("knows");

  // Triplet whose entries aren't a registered edge schema -> get_edge_table
  // throws.
  EXPECT_THROW(view.GetGenericOutgoingView(99, person_label, knows_label, 0),
               exception::InvalidArgumentException);
  EXPECT_THROW(view.GetGenericOutgoingView(person_label, 99, knows_label, 0),
               exception::InvalidArgumentException);
  EXPECT_THROW(view.GetGenericOutgoingView(person_label, person_label, 99, 0),
               exception::InvalidArgumentException);
}

}  // namespace neug
