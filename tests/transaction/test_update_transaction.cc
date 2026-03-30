
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

#include "neug/neug.h"
#include "neug/server/neug_db_service.h"
#include "neug/storages/csr/generic_view_utils.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/transaction/update_transaction.h"

#include <limits>

#include "column_assertions.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

class UpdateTransactionTest : public ::testing::Test {
 protected:
  std::string db_dir;
  neug::MemoryLevel memory_level;

  void SetUp() override {
    db_dir = "/tmp/test_update_transaction_db";
    if (std::filesystem::exists(db_dir)) {
      std::filesystem::remove_all(db_dir);
    }
    std::filesystem::create_directories(db_dir);

    neug::NeugDB db;
    neug::NeugDBConfig config(db_dir);
    config.memory_level = neug::MemoryLevel::kInMemory;
    config.checkpoint_on_close = true;
    db.Open(db_dir);
    auto conn = db.Connect();
    EXPECT_TRUE(
        conn->Query("CREATE NODE TABLE person(id INT64, name STRING, "
                    "age INT64, PRIMARY KEY(id));"));
    EXPECT_TRUE(
        conn->Query("CREATE NODE TABLE software(id INT64, name STRING, "
                    "lang STRING, PRIMARY KEY(id));"));
    EXPECT_TRUE(conn->Query(
        "CREATE REL TABLE created(FROM person TO software, weight DOUBLE, "
        "since INT64);"));
    EXPECT_TRUE(
        conn->Query("CREATE REL TABLE knows(FROM person TO person, "
                    "closeness DOUBLE);"));
    EXPECT_TRUE(
        conn->Query("Create ( n:person {id: 1, name: 'Alice', age: 30});"));
    EXPECT_TRUE(conn->Query(
        "Create ( n:software {id: 1, name: 'GraphDB', lang: 'C++'});"));
    EXPECT_TRUE(
        conn->Query("Create ( n:person {id: 2, name: 'Bob', age: 25});"));
    EXPECT_TRUE(conn->Query(
        "Create ( n:software {id: 2, name: 'FastGraph', lang: 'Rust'});"));
    EXPECT_TRUE(
        conn->Query("MATCH (a:person {id: 1}), (b:software {id: 1}) "
                    "CREATE (a)-[:created {weight: 0.8, since: 2021}]->(b);"));
    EXPECT_TRUE(
        conn->Query("MATCH (a:person {id: 2}), (b:software {id: 2}) "
                    "CREATE (a)-[:created {weight: 0.7, since: 2020}]->(b);"));
    EXPECT_TRUE(
        conn->Query("MATCH (a:person {id: 1}), (b:person {id: 2}) "
                    "CREATE (a)-[:knows {closeness: 0.9}]->(b);"));
    db.Close();
  }

  void TearDown() override {
    if (std::filesystem::exists(db_dir)) {
      std::filesystem::remove_all(db_dir);
    }
  }

  size_t count_edges_filter_src(const neug::StorageReadInterface& gi,
                                neug::label_t src_label,
                                neug::label_t neighbor_label,
                                neug::label_t edge_label, neug::vid_t src_vid,
                                bool oe) {
    size_t edge_count = 0;
    auto view = oe ? gi.GetGenericOutgoingGraphView(src_label, neighbor_label,
                                                    edge_label)
                   : gi.GetGenericIncomingGraphView(src_label, neighbor_label,
                                                    edge_label);
    auto edge_iter = view.get_edges(src_vid);
    for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
      edge_count++;
    }
    return edge_count;
  }

  size_t count_vertices(const neug::StorageReadInterface& gi,
                        neug::label_t label) {
    size_t vertex_count = 0;
    auto v_set = gi.GetVertexSet(label);
    v_set.foreach_vertex([&](neug::vid_t vid) { vertex_count++; });
    return vertex_count;
  }

  size_t count_edges(const neug::StorageReadInterface& gi,
                     neug::label_t src_label, neug::label_t neighbor_label,
                     neug::label_t edge_label, bool oe) {
    size_t edge_count = 0;
    auto view = oe ? gi.GetGenericOutgoingGraphView(src_label, neighbor_label,
                                                    edge_label)
                   : gi.GetGenericIncomingGraphView(src_label, neighbor_label,
                                                    edge_label);
    auto v_set = gi.GetVertexSet(src_label);
    v_set.foreach_vertex([&](neug::vid_t vid) {
      auto edge_iter = view.get_edges(vid);
      for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
        edge_count++;
      }
    });
    return edge_count;
  }

  void create_new_edge_type(neug::UpdateTransaction& txn,
                            neug::StorageTPUpdateInterface& interface,
                            neug::label_t& cmp_label, neug::label_t& dev_label,
                            neug::label_t& employ_label) {
    auto person_label = interface.schema().get_vertex_label_id("person");
    auto software_label = interface.schema().get_vertex_label_id("software");
    std::vector<std::tuple<neug::DataType, std::string, neug::Property>>
        edge_props = {std::make_tuple(neug::DataTypeId::kDouble, "rating",
                                      neug::Property::from_double(0.0)),
                      std::make_tuple(neug::DataTypeId::kInt64, "year",
                                      neug::Property::from_int64(2000))};
    EXPECT_TRUE(interface.CreateEdgeType(
        "person", "software", "developed", edge_props, true,
        neug::EdgeStrategy::kMultiple, neug::EdgeStrategy::kMultiple));
    std::vector<std::tuple<neug::DataType, std::string, neug::Property>>
        v_props = {std::make_tuple(neug::DataTypeId::kInt64, "id",
                                   neug::Property::from_int64(0)),
                   std::make_tuple(neug::DataTypeId::kVarchar, "name",
                                   neug::Property::from_string_view(""))};
    EXPECT_TRUE(interface.CreateVertexType("company", v_props, {"id"}, true));
    EXPECT_TRUE(interface.CreateEdgeType("person", "company", "employed_by", {},
                                         true, neug::EdgeStrategy::kMultiple,
                                         neug::EdgeStrategy::kMultiple));
    employ_label = interface.schema().get_edge_label_id("employed_by");
    cmp_label = interface.schema().get_vertex_label_id("company");
    dev_label = interface.schema().get_edge_label_id("developed");
    neug::vid_t vid;
    EXPECT_TRUE(interface.AddVertex(
        cmp_label, neug::Property::from_int64(1),
        {neug::Property::from_string_view("TechCorp")}, vid));
    neug::vid_t p1_vid;
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Property::from_int64(1),
                                   p1_vid));
    neug::vid_t software_vid;
    EXPECT_TRUE(txn.GetVertexIndex(
        software_label, neug::Property::from_int64(1), software_vid));
    neug::vid_t cmp_vid;
    EXPECT_TRUE(
        txn.GetVertexIndex(cmp_label, neug::Property::from_int64(1), cmp_vid));
    EXPECT_TRUE(interface.AddEdge(
        person_label, p1_vid, software_label, software_vid, dev_label,
        {neug::Property::from_double(4.5), neug::Property::from_int64(2023)}));
    EXPECT_TRUE(interface.AddEdge(person_label, p1_vid, cmp_label, cmp_vid,
                                  employ_label, {}));
  }

  template <typename FUNC_T>
  void update_edge_property(neug::UpdateTransaction& txn,
                            neug::label_t src_label, neug::label_t dst_label,
                            neug::label_t edge_label, neug::vid_t src_vid,
                            std::function<bool(neug::vid_t)> condition,
                            FUNC_T func) {
    auto oe_view =
        txn.GetGenericOutgoingGraphView(src_label, dst_label, edge_label);
    auto ie_view =
        txn.GetGenericIncomingGraphView(dst_label, src_label, edge_label);
    auto e_prop_types = txn.schema()
                            .get_edge_schema(src_label, dst_label, edge_label)
                            ->properties;
    auto edge_iter = oe_view.get_edges(src_vid);
    int32_t oe_offset = 0;
    for (auto it = edge_iter.begin(); it != edge_iter.end();
         ++it, ++oe_offset) {
      if (condition(it.get_vertex())) {
        auto ie_offset = neug::search_other_offset_with_cur_offset(
            oe_view, ie_view, src_vid, it.get_vertex(), oe_offset,
            e_prop_types);
        func(it.get_vertex(), oe_offset, ie_offset);
      }
    }
  }

  std::vector<std::string> create_string_prop_relation(
      neug::StorageTPUpdateInterface& graph, int num_edges) {
    auto person_label = graph.schema().get_vertex_label_id("person");
    auto software_label = graph.schema().get_vertex_label_id("software");
    std::vector<std::tuple<neug::DataType, std::string, neug::Property>>
        edge_props = {
            std::make_tuple(neug::DataTypeId::kVarchar, "review",
                            neug::Property::from_string_view("no review"))};
    EXPECT_TRUE(graph.CreateEdgeType(
        "person", "software", "reviewed", edge_props, true,
        neug::EdgeStrategy::kMultiple, neug::EdgeStrategy::kMultiple));
    neug::label_t review_label = graph.schema().get_edge_label_id("reviewed");
    neug::vid_t p1_vid;
    EXPECT_TRUE(graph.GetVertexIndex(person_label,
                                     neug::Property::from_int64(1), p1_vid));
    neug::vid_t s1_vid;
    EXPECT_TRUE(graph.GetVertexIndex(software_label,
                                     neug::Property::from_int64(1), s1_vid));
    std::string review_text("Review number: ");
    std::vector<std::string> reviews;
    for (int i = 0; i < num_edges; i++) {
      std::string full_review = review_text + std::to_string(i);
      reviews.push_back(full_review);
      EXPECT_TRUE(graph.AddEdge(
          person_label, p1_vid, software_label, s1_vid, review_label,
          {neug::Property::from_string_view(full_review)}));
    }
    return reviews;
  }

  // Helper function to fetch all edge string properties
  std::vector<std::string_view> fetch_edge_string_properties(
      neug::StorageReadInterface& gi, neug::label_t person_label,
      neug::label_t software_label, neug::label_t review_label) {
    auto ed_accessor =
        gi.GetEdgeDataAccessor(person_label, software_label, review_label, 0);
    auto view = gi.GetGenericOutgoingGraphView(person_label, software_label,
                                               review_label);
    auto vertex_set = gi.GetVertexSet(person_label);
    std::vector<std::string_view> fetched_views;
    for (neug::vid_t vid : vertex_set) {
      auto edges = view.get_edges(vid);
      for (auto it = edges.begin(); it != edges.end(); ++it) {
        auto prop = ed_accessor.get_data(it);
        fetched_views.push_back(prop.as_string_view());
      }
    }
    return fetched_views;
  }

  // Helper function to verify expected and fetched string views match
  void verify_string_views(const std::vector<std::string>& expected,
                           const std::vector<std::string_view>& fetched) {
    auto sorted_expected = expected;
    auto sorted_fetched = fetched;
    std::sort(sorted_expected.begin(), sorted_expected.end());
    std::sort(sorted_fetched.begin(), sorted_fetched.end());
    CHECK_EQ(sorted_expected.size(), sorted_fetched.size());
    for (size_t i = 0; i < sorted_expected.size(); ++i) {
      EXPECT_EQ(sorted_expected[i], sorted_fetched[i]);
    }
  }
};

TEST_F(UpdateTransactionTest, AddVertex) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    neug::vid_t vid;
    EXPECT_TRUE(txn.AddVertex(person_label, neug::Property::from_int64(3),
                              {neug::Property::from_string_view("Eve"),
                               neug::Property::from_int64(28)},
                              vid));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    EXPECT_EQ(count_vertices(gi, person_label), 3);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, AddVertexBatch) {
  // To trigger the internal resize of vertex property columns,
  // we add a batch of vertices.
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    for (int i = 4; i <= 10000; i++) {
      neug::vid_t vid;
      EXPECT_TRUE(txn.AddVertex(person_label, neug::Property::from_int64(i),
                                {neug::Property::from_string_view("User"),
                                 neug::Property::from_int64(20 + i % 10)},
                                vid));
    }
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    EXPECT_EQ(count_vertices(gi, person_label), 9999);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, AddEdge) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    neug::vid_t vid;
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, neug::Property::from_int64(1), vid));
    neug::vid_t vid2;
    EXPECT_TRUE(txn.GetVertexIndex(software_label,
                                   neug::Property::from_int64(2), vid2));
    EXPECT_TRUE(txn.AddEdge(
        person_label, vid, software_label, vid2, created_label,
        {neug::Property::from_double(0.9), neug::Property::from_int64(2022)}));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto created_label = gi.schema().get_edge_label_id("created");
    auto view = gi.GetGenericOutgoingGraphView(person_label, software_label,
                                               created_label);

    size_t edge_count = 0;
    auto vertex_set = gi.GetVertexSet(person_label);
    for (neug::vid_t vid : vertex_set) {
      auto oid = gi.GetVertexId(person_label, vid);
      if (oid.as_int64() == 1) {
        auto edge_iter = view.get_edges(vid);
        for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
          edge_count++;
        }
      }
    }
    EXPECT_EQ(edge_count, 2);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, AddVertexEdge) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    neug::vid_t vid2, vid4, vid3;
    EXPECT_TRUE(txn.AddVertex(person_label, neug::Property::from_int64(4),
                              {neug::Property::from_string_view("David"),
                               neug::Property::from_int64(32)},
                              vid4));
    EXPECT_TRUE(txn.AddVertex(software_label, neug::Property::from_int64(3),
                              {neug::Property::from_string_view("NeugDB"),
                               neug::Property::from_string_view("C++")},
                              vid3));
    EXPECT_TRUE(txn.AddEdge(
        person_label, vid4, software_label, vid3, created_label,
        {neug::Property::from_double(0.85), neug::Property::from_int64(2023)}));
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, neug::Property::from_int64(2), vid2));
    EXPECT_TRUE(txn.AddEdge(
        person_label, vid2, software_label, vid3, created_label,
        {neug::Property::from_double(0.75), neug::Property::from_int64(2021)}));
    neug::vid_t vid1;
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, neug::Property::from_int64(1), vid1));
    EXPECT_TRUE(txn.AddEdge(person_label, vid4, person_label, vid1,
                            txn.schema().get_edge_label_id("knows"),
                            {neug::Property::from_double(0.95)}));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    EXPECT_EQ(count_vertices(gi, person_label), 3);
    auto software_label = gi.schema().get_vertex_label_id("software");
    EXPECT_EQ(count_vertices(gi, software_label), 3);
    auto created_label = gi.schema().get_edge_label_id("created");
    auto knows_label = gi.schema().get_edge_label_id("knows");
    neug::vid_t david_vid;
    EXPECT_TRUE(gi.GetVertexIndex(person_label, neug::Property::from_int64(4),
                                  david_vid));
    EXPECT_EQ(count_edges_filter_src(gi, person_label, software_label,
                                     created_label, david_vid, true),
              1);
    neug::vid_t neugdb_vid;
    EXPECT_TRUE(gi.GetVertexIndex(software_label, neug::Property::from_int64(3),
                                  neugdb_vid));
    EXPECT_EQ(count_edges_filter_src(gi, software_label, person_label,
                                     created_label, neugdb_vid, false),
              2);
    EXPECT_EQ(count_edges_filter_src(gi, person_label, person_label,
                                     knows_label, david_vid, true),
              1);
  }

  db.Close();
}

TEST_F(UpdateTransactionTest, AddVertexEdgeAbort) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    neug::vid_t vid5, vid4;
    EXPECT_TRUE(txn.AddVertex(person_label, neug::Property::from_int64(5),
                              {neug::Property::from_string_view("Frank"),
                               neug::Property::from_int64(27)},
                              vid5));
    EXPECT_TRUE(txn.AddVertex(software_label, neug::Property::from_int64(4),
                              {neug::Property::from_string_view("UltraGraph"),
                               neug::Property::from_string_view("Go")},
                              vid4));
    EXPECT_TRUE(txn.AddEdge(
        person_label, vid5, software_label, vid4, created_label,
        {neug::Property::from_double(0.65), neug::Property::from_int64(2022)}));
    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    EXPECT_EQ(count_vertices(gi, person_label), 2);
    auto software_label = gi.schema().get_vertex_label_id("software");
    EXPECT_EQ(count_vertices(gi, software_label), 2);
    auto created_label = gi.schema().get_edge_label_id("created");
    auto oe_view = gi.GetGenericOutgoingGraphView(person_label, software_label,
                                                  created_label);
    size_t edge_count = 0;
    auto vertex_set = gi.GetVertexSet(person_label);
    for (neug::vid_t vid : vertex_set) {
      auto edges = oe_view.get_edges(vid);
      for (auto it = edges.begin(); it != edges.end(); ++it) {
        edge_count++;
      }
    }
    EXPECT_EQ(edge_count, 2);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, UpdateVertexProperty) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    neug::vid_t vertex_id;
    CHECK(txn.GetVertexIndex(person_label, neug::Property::from_int64(2),
                             vertex_id));
    EXPECT_TRUE(txn.UpdateVertexProperty(person_label, vertex_id, 1,
                                         neug::Property::from_int64(26)));

    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto vprop_accessor = std::dynamic_pointer_cast<
        neug::StorageReadInterface::vertex_column_t<int64_t>>(
        gi.GetVertexPropColumn(person_label, "age"));
    auto vertex_set = gi.GetVertexSet(person_label);
    for (neug::vid_t vid : vertex_set) {
      auto oid = gi.GetVertexId(person_label, vid);
      if (oid.as_int64() == 2) {
        EXPECT_EQ(vprop_accessor->get_view(vid), 26);
      }
    }
  }
  db.Close();
}
TEST_F(UpdateTransactionTest, UpdateEdgeProperty) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    neug::vid_t vertex_id;
    CHECK(txn.GetVertexIndex(person_label, neug::Property::from_int64(1),
                             vertex_id));
    update_edge_property(
        txn, person_label, software_label, created_label, vertex_id,
        [](neug::vid_t dst_vid) { return true; },
        [&](neug::vid_t dst_vid, int32_t oe_offset, int32_t ie_offset) {
          txn.UpdateEdgeProperty(person_label, vertex_id, software_label,
                                 dst_vid, created_label, oe_offset, ie_offset,
                                 0, neug::Property::from_double(0.99));
        });

    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto created_label = gi.schema().get_edge_label_id("created");
    auto view = gi.GetGenericOutgoingGraphView(person_label, software_label,
                                               created_label);
    auto ed_accessor =
        gi.GetEdgeDataAccessor(person_label, software_label, created_label, 0);
    auto vertex_set = gi.GetVertexSet(person_label);
    for (neug::vid_t vid : vertex_set) {
      auto oid = gi.GetVertexId(person_label, vid);
      if (oid.as_int64() == 1) {
        auto edge_iter = view.get_edges(vid);
        for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
          EXPECT_EQ(ed_accessor.get_data(it).as_double(), 0.99);
        }
      }
    }
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, AddVertexAbort) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    neug::vid_t vid;
    EXPECT_TRUE(txn.AddVertex(person_label, neug::Property::from_int64(4),
                              {neug::Property::from_string_view("Charlie"),
                               neug::Property::from_int64(29)},
                              vid));
    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    EXPECT_EQ(count_vertices(gi, person_label), 2);
  }
  {
    auto conn = db.Connect();
    auto result = conn->Query(
        "MATCH (n:person {id: 4}) RETURN n.name AS name, n.age AS age;");
    EXPECT_EQ(result.value().length(), 0);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, AddEdgeAbort) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    neug::vid_t vid2, vid1;
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, neug::Property::from_int64(2), vid2));
    EXPECT_TRUE(txn.GetVertexIndex(software_label,
                                   neug::Property::from_int64(1), vid1));
    EXPECT_TRUE(txn.AddEdge(
        person_label, vid2, software_label, vid1, created_label,
        {neug::Property::from_double(0.8), neug::Property::from_int64(2021)}));
    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto created_label = gi.schema().get_edge_label_id("created");
    auto view = gi.GetGenericOutgoingGraphView(person_label, software_label,
                                               created_label);
    size_t edge_count = 0;
    auto vertex_set = gi.GetVertexSet(person_label);
    for (neug::vid_t vid : vertex_set) {
      auto oid = gi.GetVertexId(person_label, vid);
      if (oid.as_int64() == 2) {
        auto edge_iter = view.get_edges(vid);
        for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
          edge_count++;
        }
      }
    }
    EXPECT_EQ(edge_count, 1);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, UpdateVertexAbort) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    neug::vid_t vertex_id;
    CHECK(txn.GetVertexIndex(person_label, neug::Property::from_int64(2),
                             vertex_id));
    EXPECT_TRUE(txn.UpdateVertexProperty(person_label, vertex_id, 1,
                                         neug::Property::from_int64(27)));
    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto vprop_accessor = std::dynamic_pointer_cast<
        neug::StorageReadInterface::vertex_column_t<int64_t>>(
        gi.GetVertexPropColumn(person_label, "age"));
    auto vertex_set = gi.GetVertexSet(person_label);
    for (neug::vid_t vid : vertex_set) {
      auto oid = gi.GetVertexId(person_label, vid);
      if (oid.as_int64() == 2) {
        EXPECT_EQ(vprop_accessor->get_view(vid), 25);
      }
    }
  }
  {
    auto conn = db.Connect();
    auto result = conn->Query(
        "MATCH (n:person {id: 2}) RETURN n.name AS name, n.age AS age;");
    EXPECT_TRUE(result);
    auto& value = result.value();
    neug::test::AssertStringColumn(value.response(), 0, {"Bob"});
    neug::test::AssertInt64Column(value.response(), 1, {25});
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, UpdateEdgeAbort) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    neug::vid_t vertex_id;
    CHECK(txn.GetVertexIndex(person_label, neug::Property::from_int64(1),
                             vertex_id));

    update_edge_property(
        txn, person_label, software_label, created_label, vertex_id,
        [](neug::vid_t dst_vid) { return true; },
        [&](neug::vid_t dst_vid, int32_t oe_offset, int32_t ie_offset) {
          txn.UpdateEdgeProperty(person_label, vertex_id, software_label,
                                 dst_vid, created_label, oe_offset, ie_offset,
                                 0, neug::Property::from_double(0.9));
          txn.UpdateEdgeProperty(person_label, vertex_id, software_label,
                                 dst_vid, created_label, oe_offset, ie_offset,
                                 1, neug::Property::from_int64(2023));
        });

    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto created_label = gi.schema().get_edge_label_id("created");
    auto view = gi.GetGenericOutgoingGraphView(person_label, software_label,
                                               created_label);
    auto ed_accessor =
        gi.GetEdgeDataAccessor(person_label, software_label, created_label, 0);
    auto since_accessor =
        gi.GetEdgeDataAccessor(person_label, software_label, created_label, 1);
    auto vertex_set = gi.GetVertexSet(person_label);
    for (neug::vid_t vid : vertex_set) {
      auto oid = gi.GetVertexId(person_label, vid);
      if (oid.as_int64() == 1) {
        auto edge_iter = view.get_edges(vid);
        for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
          EXPECT_EQ(ed_accessor.get_data(it).as_double(), 0.8);
          EXPECT_EQ(since_accessor.get_data(it).as_int64(), 2021);
        }
      }
    }
  }
  db.Close();
  {
    neug::NeugDB db2;
    neug::NeugDBConfig config2(db_dir);
    config2.memory_level = neug::MemoryLevel::kInMemory;
    db2.Open(config2);
    auto conn = db2.Connect();
    auto result = conn->Query(
        "MATCH (a:person {id: 1})-[r:created]->(b:software) "
        "RETURN r.weight AS weight, r.since AS since;");
    EXPECT_TRUE(result);
    auto& value = result.value();
    neug::test::AssertDoubleColumn(value.response(), 0, {0.8});
    neug::test::AssertInt64Column(value.response(), 1, {2021});
  }
}

TEST_F(UpdateTransactionTest, UpdateEdgeAbort2) {
  // Update a bundled edge property and abort the transaction
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto knows_label = txn.schema().get_edge_label_id("knows");
    neug::vid_t vertex_id;
    CHECK(txn.GetVertexIndex(person_label, neug::Property::from_int64(1),
                             vertex_id));

    update_edge_property(
        txn, person_label, person_label, knows_label, vertex_id,
        [](neug::vid_t dst_vid) { return true; },
        [&](neug::vid_t dst_vid, int32_t oe_offset, int32_t ie_offset) {
          txn.UpdateEdgeProperty(person_label, vertex_id, person_label, dst_vid,
                                 knows_label, oe_offset, ie_offset, 0,
                                 neug::Property::from_double(0.95));
        });

    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto knows_label = gi.schema().get_edge_label_id("knows");
    auto view =
        gi.GetGenericOutgoingGraphView(person_label, person_label, knows_label);
    auto ed_accessor =
        gi.GetEdgeDataAccessor(person_label, person_label, knows_label, 0);
    auto vertex_set = gi.GetVertexSet(person_label);
    for (neug::vid_t vid : vertex_set) {
      auto oid = gi.GetVertexId(person_label, vid);
      if (oid.as_int64() == 1) {
        auto edge_iter = view.get_edges(vid);
        for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
          EXPECT_EQ(ed_accessor.get_data(it).as_double(), 0.9);
        }
      }
    }
  }
  db.Close();
  {
    neug::NeugDB db2;
    neug::NeugDBConfig config2(db_dir);
    config2.memory_level = neug::MemoryLevel::kInMemory;
    db2.Open(config2);
    auto conn = db2.Connect();
    auto result = conn->Query(
        "MATCH (a:person {id: 1})-[r:knows]->(b:person {id: 2}) "
        "RETURN r.closeness AS closeness;");
    EXPECT_TRUE(result);
    const auto& value = result.value();
    neug::test::AssertDoubleColumn(value.response(), 0, {0.9});
  }
}

TEST_F(UpdateTransactionTest, AddEdgeAndUpdateAndAbort) {
  GTEST_SKIP()
      << "Currently not support update bundled edge property and abort";
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    neug::vid_t vid1, vid2;
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, neug::Property::from_int64(1), vid1));
    EXPECT_TRUE(txn.GetVertexIndex(software_label,
                                   neug::Property::from_int64(2), vid2));
    EXPECT_TRUE(txn.AddEdge(
        person_label, vid1, software_label, vid2, created_label,
        {neug::Property::from_double(0.85), neug::Property::from_int64(2023)}));
    neug::vid_t vertex_id;
    CHECK(txn.GetVertexIndex(person_label, neug::Property::from_int64(1),
                             vertex_id));

    update_edge_property(
        txn, person_label, software_label, created_label, vertex_id,
        [](neug::vid_t dst_vid) { return true; },
        [&](neug::vid_t dst_vid, int32_t oe_offset, int32_t ie_offset) {
          txn.UpdateEdgeProperty(person_label, vertex_id, software_label,
                                 dst_vid, created_label, oe_offset, ie_offset,
                                 0, neug::Property::from_double(0.9));
        });

    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto created_label = gi.schema().get_edge_label_id("created");
    auto view = gi.GetGenericOutgoingGraphView(person_label, software_label,
                                               created_label);
    auto ed_accessor =
        gi.GetEdgeDataAccessor(person_label, software_label, created_label, 0);
    size_t edge_count = 0;
    auto vertex_set = gi.GetVertexSet(person_label);
    for (neug::vid_t vid : vertex_set) {
      auto oid = gi.GetVertexId(person_label, vid);
      if (oid.as_int64() == 1) {
        auto edge_iter = view.get_edges(vid);
        for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
          if (ed_accessor.get_data(it).as_double() == 0.9 ||
              ed_accessor.get_data(it).as_double() == 0.85) {
            ADD_FAILURE() << "Found aborted edge update or addition.";
          } else {
            edge_count++;
          }
        }
      }
    }

    EXPECT_EQ(edge_count, 1);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, DeleteVertex) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    neug::vid_t vertex_id;
    CHECK(txn.GetVertexIndex(person_label, neug::Property::from_int64(2),
                             vertex_id));
    EXPECT_TRUE(txn.DeleteVertex(person_label, vertex_id));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto created_label = gi.schema().get_edge_label_id("created");
    auto software_label = gi.schema().get_vertex_label_id("software");
    EXPECT_EQ(count_vertices(gi, person_label), 1);
    neug::vid_t vertex_id;
    EXPECT_FALSE(gi.GetVertexIndex(person_label, neug::Property::from_int64(2),
                                   vertex_id));
    EXPECT_EQ(
        count_edges(gi, person_label, software_label, created_label, true), 1);
    EXPECT_EQ(
        count_edges(gi, software_label, person_label, created_label, false), 1);
  }
  {
    // Delete person label and then delete vertex should throw
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    EXPECT_TRUE(txn.DeleteVertexType("person"));
    neug::vid_t vertex_id;
    EXPECT_THROW(txn.GetVertexIndex(person_label, neug::Property::from_int64(1),
                                    vertex_id),
                 neug::exception::Exception);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, DeleteEdgeAbort) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    neug::vid_t vid2, vid1;
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, neug::Property::from_int64(1), vid2));
    EXPECT_TRUE(txn.GetVertexIndex(software_label,
                                   neug::Property::from_int64(1), vid1));
    EXPECT_TRUE(txn.DeleteEdges(person_label, vid2, software_label, vid1,
                                created_label));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto knows_label = txn.schema().get_edge_label_id("knows");
    neug::vid_t vid1, vid2;
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, neug::Property::from_int64(1), vid1));
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, neug::Property::from_int64(2), vid2));
    EXPECT_TRUE(
        txn.DeleteEdges(person_label, vid1, person_label, vid2, knows_label));
    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto knows_label = txn.schema().get_edge_label_id("knows");
    neug::vid_t vid1, vid2;
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, neug::Property::from_int64(1), vid1));
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, neug::Property::from_int64(2), vid2));
    auto oe_edges =
        txn.GetGenericOutgoingGraphView(person_label, person_label, knows_label)
            .get_edges(vid1);
    auto ie_edges =
        txn.GetGenericIncomingGraphView(person_label, person_label, knows_label)
            .get_edges(vid2);
    auto edge_prop_types = txn.schema().get_edge_properties(
        person_label, person_label, knows_label);
    auto search_edge_prop_type = edge_prop_types.size() == 1
                                     ? edge_prop_types[0].id()
                                     : neug::DataTypeId::kUInt64;
    int32_t oe_offset = 0, ie_offset = 0;
    for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
      if (it.get_vertex() == vid2) {
        ie_offset = fuzzy_search_offset_from_nbr_list(
            ie_edges, vid1, it.get_data_ptr(), search_edge_prop_type);
        break;
      }
      oe_offset++;
    }
    EXPECT_TRUE(txn.DeleteEdge(person_label, vid1, person_label, vid2,
                               knows_label, oe_offset, ie_offset));
    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    // Check edge count
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto p_label_id = gi.schema().get_vertex_label_id("person");
    auto s_label_id = gi.schema().get_vertex_label_id("software");
    auto e_label_id = gi.schema().get_edge_label_id("created");
    auto knows_label_id = gi.schema().get_edge_label_id("knows");
    auto oe_created_count =
        count_edges(gi, p_label_id, s_label_id, e_label_id, true);
    auto ie_created_count =
        count_edges(gi, s_label_id, p_label_id, e_label_id, false);
    EXPECT_EQ(oe_created_count, 1);
    EXPECT_EQ(ie_created_count, 1);
    auto oe_knows_count =
        count_edges(gi, p_label_id, p_label_id, knows_label_id, true);
    auto ie_knows_count =
        count_edges(gi, p_label_id, p_label_id, knows_label_id, false);
    EXPECT_EQ(oe_knows_count, 1);
    EXPECT_EQ(ie_knows_count, 1);
  }
}

TEST_F(UpdateTransactionTest, AddDeleteVertexAbort) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    auto person_label = interface.schema().get_vertex_label_id("person");
    neug::vid_t vertex_id;
    CHECK(txn.GetVertexIndex(person_label, neug::Property::from_int64(2),
                             vertex_id));
    EXPECT_TRUE(txn.DeleteVertex(person_label, vertex_id));
    neug::vid_t vid;
    EXPECT_TRUE(interface.AddVertex(person_label, neug::Property::from_int64(3),
                                    {neug::Property::from_string_view("Eve"),
                                     neug::Property::from_int64(28)},
                                    vid));
    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    EXPECT_EQ(count_vertices(gi, person_label), 2);
    neug::vid_t vertex_id;
    EXPECT_TRUE(gi.GetVertexIndex(person_label, neug::Property::from_int64(2),
                                  vertex_id));
    EXPECT_FALSE(gi.GetVertexIndex(person_label, neug::Property::from_int64(3),
                                   vertex_id));
  }
  {
    // Add again
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    neug::vid_t vid;
    EXPECT_TRUE(txn.AddVertex(person_label, neug::Property::from_int64(3),
                              {neug::Property::from_string_view("Eve"),
                               neug::Property::from_int64(28)},
                              vid));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    EXPECT_EQ(count_vertices(gi, person_label), 3);
    neug::vid_t vertex_id;
    EXPECT_TRUE(gi.GetVertexIndex(person_label, neug::Property::from_int64(3),
                                  vertex_id));
    EXPECT_EQ(count_vertices(gi, person_label), 3);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, CreteEdgeTypeAndAbort) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  neug::label_t dev_label, employ_label, cmp_label;
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    create_new_edge_type(txn, interface, cmp_label, dev_label, employ_label);
    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    EXPECT_THROW(gi.schema().get_edge_label_id("developed"),
                 neug::exception::Exception);
    EXPECT_THROW(gi.schema().get_edge_label_id("employed_by"),
                 neug::exception::Exception);
    EXPECT_THROW(gi.schema().get_vertex_label_id("company"),
                 neug::exception::Exception);
    EXPECT_FALSE(
        gi.schema().has_edge_label("person", "company", "employed_by"));
    EXPECT_THROW(
        gi.GetGenericOutgoingGraphView(person_label, cmp_label, employ_label),
        neug::exception::Exception);
    EXPECT_THROW(
        gi.GetGenericOutgoingGraphView(person_label, software_label, dev_label),
        neug::exception::Exception);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, CreteEdgeTypeAndCommit) {
  GTEST_SKIP() << "Enable this test after in-place AddEdge is supported";
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  neug::label_t dev_label, employ_label, cmp_label;
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    create_new_edge_type(txn, interface, cmp_label, dev_label, employ_label);
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    EXPECT_EQ(gi.schema().get_edge_label_id("developed"), dev_label);
    EXPECT_EQ(gi.schema().get_edge_label_id("employed_by"), employ_label);
    EXPECT_EQ(gi.schema().get_vertex_label_id("company"), cmp_label);
    EXPECT_EQ(count_edges(gi, person_label, software_label, dev_label, true),
              1);
    EXPECT_EQ(count_edges(gi, person_label, cmp_label, employ_label, true), 1);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, DeleteEdgeTypeAbort) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto created_label = txn.schema().get_edge_label_id("created");
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    EXPECT_TRUE(txn.DeleteEdgeType("person", "software", "created", true));
    EXPECT_FALSE(txn.schema().edge_triplet_valid(person_label, software_label,
                                                 created_label));
    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto created_label = gi.schema().get_edge_label_id("created");
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    EXPECT_TRUE(gi.schema().exist("person", "software", "created"));
    EXPECT_TRUE(gi.schema().edge_triplet_valid(person_label, software_label,
                                               created_label));
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, AddVertexProperties) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    std::vector<std::tuple<neug::DataType, std::string, neug::Property>>
        new_props = {std::make_tuple(neug::DataTypeId::kVarchar, "email",
                                     neug::Property::from_string_view("")),
                     std::make_tuple(neug::DataTypeId::kDouble, "height",
                                     neug::Property::from_double(0.0))};
    EXPECT_TRUE(txn.AddVertexProperties("person", new_props, true));
    auto email_accessor = txn.get_vertex_property_column(person_label, "email");
    neug::vid_t vid;
    CHECK(txn.GetVertexIndex(person_label, neug::Property::from_int64(1), vid));
    EXPECT_TRUE(txn.UpdateVertexProperty(
        person_label, vid, 2,
        neug::Property::from_string_view("eve@example.com")));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto height_accessor =
        txn.get_vertex_property_column(person_label, "height");
    std::vector<std::tuple<neug::DataType, std::string, neug::Property>>
        new_props = {std::make_tuple(neug::DataTypeId::kVarchar, "address",
                                     neug::Property::from_string_view(""))};
    EXPECT_TRUE(txn.AddVertexProperties("person", new_props, true));
    neug::vid_t vid;
    CHECK(txn.GetVertexIndex(person_label, neug::Property::from_int64(2), vid));
    EXPECT_TRUE(txn.UpdateVertexProperty(person_label, vid, 3,
                                         neug::Property::from_double(175.5)));
    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    EXPECT_EQ(gi.GetVertexPropColumn(person_label, "address"), nullptr);

    auto email_accessor = std::dynamic_pointer_cast<
        neug::StorageReadInterface::vertex_column_t<std::string_view>>(
        gi.GetVertexPropColumn(person_label, "email"));
    auto height_accessor = std::dynamic_pointer_cast<
        neug::StorageReadInterface::vertex_column_t<double>>(
        gi.GetVertexPropColumn(person_label, "height"));
    neug::vid_t vid;
    CHECK(gi.GetVertexIndex(person_label, neug::Property::from_int64(1), vid));
    EXPECT_EQ(email_accessor->get(vid).as_string_view(), "eve@example.com");
    CHECK(gi.GetVertexIndex(person_label, neug::Property::from_int64(2), vid));
    EXPECT_EQ(height_accessor->get(vid).as_double(), 0.0);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, AddEdgeProperties) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    std::vector<std::tuple<neug::DataType, std::string, neug::Property>>
        new_props = {std::make_tuple(neug::DataTypeId::kInt64, "version",
                                     neug::Property::from_int64(0)),
                     std::make_tuple(neug::DataTypeId::kVarchar, "license",
                                     neug::Property::from_string_view(""))};
    EXPECT_TRUE(interface.AddEdgeProperties("person", "software", "created",
                                            new_props, true));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    std::vector<std::tuple<neug::DataType, std::string, neug::Property>>
        new_props = {std::make_tuple(neug::DataTypeId::kDouble, "contributions",
                                     neug::Property::from_double(0.0))};
    EXPECT_TRUE(interface.AddEdgeProperties("person", "software", "created",
                                            new_props, true));
    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    auto gi = neug::StorageReadInterface(txn.graph(), txn.timestamp());
    auto created_label = gi.schema().get_edge_label_id("created");
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    EXPECT_EQ(gi.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("weight"),
              0);
    EXPECT_EQ(gi.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("since"),
              1);
    EXPECT_EQ(gi.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("version"),
              2);
    EXPECT_EQ(gi.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("license"),
              3);
    EXPECT_EQ(gi.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("contributions"),
              -1);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, RenameVertexProperty) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    EXPECT_TRUE(interface.RenameVertexProperties(
        "person", {std::make_pair("age", "years")}, true));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    std::vector<std::pair<std::string, std::string>> rename_props = {
        std::make_pair("lang", "language")};
    EXPECT_TRUE(
        interface.RenameVertexProperties("software", rename_props, true));
    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    EXPECT_EQ(gi.GetVertexPropColumn(person_label, "age"), nullptr);
    EXPECT_NO_THROW(gi.GetVertexPropColumn(person_label, "years"));
    EXPECT_EQ(gi.GetVertexPropColumn(software_label, "language"), nullptr);
    EXPECT_NO_THROW(gi.GetVertexPropColumn(software_label, "lang"));
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, RenameEdgeProperty) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    EXPECT_TRUE(interface.RenameEdgeProperties(
        "person", "software", "created",
        {std::make_pair("since", "start_year")}, true));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    std::vector<std::pair<std::string, std::string>> rename_props = {
        std::make_pair("weight", "importance")};
    EXPECT_TRUE(interface.RenameEdgeProperties("person", "software", "created",
                                               rename_props, true));
    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto created_label = gi.schema().get_edge_label_id("created");
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    EXPECT_EQ(gi.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("since"),
              -1);
    EXPECT_EQ(gi.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("start_year"),
              1);
    EXPECT_EQ(gi.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("weight"),
              0);
    EXPECT_EQ(gi.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("importance"),
              -1);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, DeleteEdgeProperties) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    LOG(INFO) << "Starting delete edge properties transaction.";
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    EXPECT_TRUE(txn.DeleteEdgeProperties("person", "software", "created",
                                         {"since"}, true));
    std::vector<std::tuple<neug::DataType, std::string, neug::Property>>
        new_props = {std::make_tuple(neug::DataTypeId::kDouble, "contributions",
                                     neug::Property::from_double(0.0))};
    LOG(INFO) << "Adding new edge property 'contributions'.";
    EXPECT_TRUE(interface.AddEdgeProperties("person", "software", "created",
                                            new_props, true));
    LOG(INFO) << "Committing delete edge properties transaction.";
    EXPECT_TRUE(txn.Commit());
    LOG(INFO) << "Committed delete edge properties transaction.";
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    EXPECT_TRUE(interface.DeleteEdgeProperties("person", "software", "created",
                                               {"weight"}, true));
    txn.Abort();
    LOG(INFO) << "Aborted delete edge properties transaction.";
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto created_label = gi.schema().get_edge_label_id("created");
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    EXPECT_EQ(gi.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("since"),
              -1);
    EXPECT_EQ(gi.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("weight"),
              0);
    EXPECT_EQ(gi.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("contributions"),
              1);
    auto ed_accessor = gi.GetEdgeDataAccessor(person_label, software_label,
                                              created_label, "contributions");
    auto view = gi.GetGenericOutgoingGraphView(person_label, software_label,
                                               created_label);
    LOG(INFO) << "Checking edge properties after delete.";
    auto vertex_set = gi.GetVertexSet(person_label);
    for (neug::vid_t vid : vertex_set) {
      auto edges = view.get_edges(vid);
      for (auto it = edges.begin(); it != edges.end(); ++it) {
        EXPECT_EQ(ed_accessor.get_data(it).as_double(), 0.0);
      }
    }
    EXPECT_THROW(gi.GetEdgeDataAccessor(person_label, software_label,
                                        created_label, "since"),
                 neug::exception::Exception);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, DeleteVertexProperties) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    // auto person_label = txn.schema().get_vertex_label_id("person");
    // auto software_label = txn.schema().get_vertex_label_id("software");
    EXPECT_TRUE(interface.DeleteVertexProperties("person", {"age"}, true));
    EXPECT_TRUE(interface.DeleteVertexProperties("software", {"lang"}, true));
    std::vector<std::tuple<neug::DataType, std::string, neug::Property>>
        new_props = {std::make_tuple(neug::DataTypeId::kVarchar, "authors",
                                     neug::Property::from_string_view(""))};
    EXPECT_TRUE(interface.AddVertexProperties("software", new_props, true));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    // auto person_label = txn.schema().get_vertex_label_id("person");
    EXPECT_TRUE(interface.DeleteVertexProperties("person", {"name"}, true));
    EXPECT_TRUE(interface.DeleteVertexProperties("software",
                                                 {"name", "authors"}, true));
    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    EXPECT_EQ(gi.GetVertexPropColumn(person_label, "age"), nullptr);
    EXPECT_NO_THROW(gi.GetVertexPropColumn(person_label, "name"));
    EXPECT_EQ(gi.GetVertexPropColumn(software_label, "lang"), nullptr);
    EXPECT_NO_THROW(gi.GetVertexPropColumn(software_label, "name"));
    EXPECT_NO_THROW(gi.GetVertexPropColumn(software_label, "authors"));
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, TestReplayWal) {
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  config.checkpoint_on_close = false;
  config.compact_on_close = false;
  config.checkpoint_after_recovery = true;
  {
    neug::NeugDB db;
    db.Open(config);
    auto svc = std::make_shared<neug::NeugDBService>(db);
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    auto person_label = txn.schema().get_vertex_label_id("person");
    neug::vid_t vid;
    EXPECT_TRUE(interface.AddVertex(person_label, neug::Property::from_int64(3),
                                    {neug::Property::from_string_view("Eve"),
                                     neug::Property::from_int64(28)},
                                    vid));
    EXPECT_TRUE(interface.CreateVertexType(
        "company",
        {std::make_tuple(neug::DataTypeId::kInt64, "id",
                         neug::Property::from_int64(0)),
         std::make_tuple(neug::DataTypeId::kVarchar, "name",
                         neug::Property::from_string_view(""))},
        {"id"}, true));
    EXPECT_TRUE(interface.CreateEdgeType("person", "company", "employed_by", {},
                                         true, neug::EdgeStrategy::kMultiple,
                                         neug::EdgeStrategy::kMultiple));
    EXPECT_TRUE(
        interface.DeleteEdgeType("person", "software", "created", true));
    EXPECT_TRUE(interface.DeleteVertexType("software"));
    neug::vid_t src_p, dst_p;
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, neug::Property::from_int64(1), src_p));
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, neug::Property::from_int64(2), dst_p));

    EXPECT_TRUE(txn.UpdateVertexProperty(person_label, src_p, 1,
                                         neug::Property::from_int64(29)));
    txn.UpdateEdgeProperty(person_label, src_p, person_label, dst_p,
                           txn.schema().get_edge_label_id("knows"), 0,
                           neug::Property::from_double(0.5));
    EXPECT_TRUE(txn.Commit());
    db.Close();
  }
  {
    neug::NeugDB db;
    db.Open(config);
    auto svc = std::make_shared<neug::NeugDBService>(db);
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    EXPECT_EQ(count_vertices(gi, person_label), 3);
    neug::vid_t src_p, dst_p;
    EXPECT_TRUE(
        gi.GetVertexIndex(person_label, neug::Property::from_int64(1), src_p));
    EXPECT_TRUE(
        gi.GetVertexIndex(person_label, neug::Property::from_int64(2), dst_p));
    auto vprop_accessor = std::dynamic_pointer_cast<
        neug::StorageReadInterface::vertex_column_t<int64_t>>(
        gi.GetVertexPropColumn(person_label, "age"));
    EXPECT_EQ(vprop_accessor->get(src_p).as_int64(), 29);
    auto knows_label = gi.schema().get_edge_label_id("knows");
    auto ed_accessor =
        gi.GetEdgeDataAccessor(person_label, person_label, knows_label, 0);
    auto view =
        gi.GetGenericOutgoingGraphView(person_label, person_label, knows_label);
    auto edge_iter = view.get_edges(src_p);
    bool found = false;
    for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
      if (it.get_vertex() == dst_p) {
        EXPECT_EQ(ed_accessor.get_data(it).as_double(), 0.5);
        found = true;
      }
    }
    EXPECT_TRUE(found);
    EXPECT_FALSE(gi.schema().contains_vertex_label("software"));
    EXPECT_TRUE(gi.schema().contains_vertex_label("company"));
    EXPECT_FALSE(gi.schema().contains_edge_label("created"));
    EXPECT_TRUE(gi.schema().contains_edge_label("employed_by"));
    txn.Commit();
    db.Close();
  }
  {
    // Open again to check checkpoint after recovery
    neug::NeugDB db;
    db.Open(config);
    auto svc = std::make_shared<neug::NeugDBService>(db);
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());

    auto person_label = gi.schema().get_vertex_label_id("person");
    auto knows_label = gi.schema().get_edge_label_id("knows");
    neug::vid_t src_p, dst_p;
    EXPECT_TRUE(
        gi.GetVertexIndex(person_label, neug::Property::from_int64(1), src_p));
    EXPECT_TRUE(
        gi.GetVertexIndex(person_label, neug::Property::from_int64(2), dst_p));
    auto ed_accessor =
        gi.GetEdgeDataAccessor(person_label, person_label, knows_label, 0);
    auto view =
        gi.GetGenericOutgoingGraphView(person_label, person_label, knows_label);
    auto edge_iter = view.get_edges(src_p);
    for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
      if (it.get_vertex() == dst_p) {
        EXPECT_EQ(ed_accessor.get_data(it).as_double(), 0.5);
      }
    }
  }
}

TEST_F(UpdateTransactionTest, TestAPIAfterDeleteVertexLabel) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    auto person_label = txn.schema().get_vertex_label_id("person");
    EXPECT_TRUE(interface.DeleteVertexType("person"));
    neug::vid_t vid;
    EXPECT_THROW(
        txn.GetVertexIndex(person_label, neug::Property::from_int64(1), vid),
        neug::exception::Exception);
    EXPECT_THROW(
        interface.AddVertex(person_label, neug::Property::from_int64(3),
                            {neug::Property::from_string_view("Eve"),
                             neug::Property::from_int64(28)},
                            vid),
        neug::exception::Exception);
    EXPECT_THROW(interface.UpdateVertexProperty(person_label, 0, 1,
                                                neug::Property::from_int64(30)),
                 neug::exception::Exception);
    EXPECT_THROW(
        interface.AddVertex(person_label, neug::Property::from_int64(3),
                            {neug::Property::from_string_view("Eve"),
                             neug::Property::from_int64(28)},
                            vid),
        neug::exception::Exception);
    EXPECT_THROW(txn.DeleteVertex(person_label, 0), neug::exception::Exception);
    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    auto person_label = interface.schema().get_vertex_label_id("person");
    EXPECT_TRUE(interface.DeleteVertexProperties("person", {"age"}, true));
    EXPECT_THROW(interface.RenameVertexProperties(
                     "person", {std::make_pair("age", "full_name")}, true),
                 neug::exception::Exception);
    EXPECT_THROW(txn.GetVertexProperty(person_label, 0, 2),
                 neug::exception::Exception);
    EXPECT_NO_THROW(txn.GetVertexId(person_label, 0));
    EXPECT_THROW(txn.get_vertex_property_column(person_label, "age"),
                 neug::exception::Exception);

    // add back age property
    std::vector<std::tuple<neug::DataType, std::string, neug::Property>>
        new_props = {std::make_tuple(neug::DataTypeId::kInt32, "age",
                                     neug::Property::from_int32(0))};
    EXPECT_NO_THROW(interface.AddVertexProperties("person", new_props, true));
    EXPECT_NO_THROW(txn.get_vertex_property_column(person_label, "age"));

    txn.Abort();
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, TestAPIAfterDeleteEdgeLabel) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    auto person_label = interface.schema().get_vertex_label_id("person");
    auto knows_label = interface.schema().get_edge_label_id("knows");
    EXPECT_TRUE(interface.DeleteEdgeType("person", "person", "knows", true));
    neug::vid_t src_vid, dst_vid;
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Property::from_int64(1),
                                   src_vid));
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Property::from_int64(2),
                                   dst_vid));
    EXPECT_THROW(txn.UpdateEdgeProperty(person_label, src_vid, person_label,
                                        dst_vid, knows_label, 0,
                                        neug::Property::from_double(0.8)),
                 neug::exception::Exception);
    EXPECT_THROW(
        interface.AddEdge(person_label, src_vid, person_label, dst_vid,
                          knows_label, {neug::Property::from_double(0.7)}),
        neug::exception::Exception);
    EXPECT_THROW(txn.GetGenericOutgoingGraphView(person_label, person_label,
                                                 knows_label),
                 neug::exception::Exception);
    EXPECT_THROW(
        txn.GetEdgeDataAccessor(person_label, person_label, knows_label, 0),
        neug::exception::Exception);
    txn.Abort();
  }
  {
    // Delete Edge Properties
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto knows_label = txn.schema().get_edge_label_id("knows");
    EXPECT_TRUE(txn.DeleteEdgeProperties("person", "person", "knows",
                                         {"closeness"}, true));
    EXPECT_THROW(txn.RenameEdgeProperties(
                     "person", "person", "knows",
                     {std::make_pair("closeness", "importance")}, true),
                 neug::exception::Exception);
    neug::vid_t src_vid, dst_vid;
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Property::from_int64(1),
                                   src_vid));
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Property::from_int64(2),
                                   dst_vid));
    EXPECT_THROW(
        txn.GetEdgeDataAccessor(person_label, person_label, knows_label, 0),
        neug::exception::Exception);
    txn.Abort();
  }
  db.Close();
}

// Test DeleteVertex deletes all associated outgoing edges
TEST_F(UpdateTransactionTest, DeleteVertexWithOutgoingEdges) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    neug::vid_t p1_vid;
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Property::from_int64(1),
                                   p1_vid));
    EXPECT_TRUE(txn.DeleteVertex(person_label, p1_vid));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto created_label = gi.schema().get_edge_label_id("created");
    auto knows_label = gi.schema().get_edge_label_id("knows");

    neug::vid_t p1_vid;
    EXPECT_FALSE(
        gi.GetVertexIndex(person_label, neug::Property::from_int64(1), p1_vid));
    EXPECT_EQ(count_vertices(gi, person_label), 1);
    EXPECT_EQ(
        count_edges(gi, person_label, software_label, created_label, true), 1);
    EXPECT_EQ(count_edges(gi, person_label, person_label, knows_label, true),
              0);
  }
  db.Close();
}

// Test DeleteVertex with both incoming and outgoing edges
TEST_F(UpdateTransactionTest, DeleteVertexWithBidirectionalEdges) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto knows_label = txn.schema().get_edge_label_id("knows");
    neug::vid_t p1_vid, p2_vid;
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Property::from_int64(1),
                                   p1_vid));
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Property::from_int64(2),
                                   p2_vid));
    EXPECT_TRUE(txn.AddEdge(person_label, p2_vid, person_label, p1_vid,
                            knows_label, {neug::Property::from_double(0.85)}));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto knows_label = gi.schema().get_edge_label_id("knows");
    EXPECT_EQ(count_edges(gi, person_label, person_label, knows_label, true),
              2);
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    neug::vid_t p1_vid;
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Property::from_int64(1),
                                   p1_vid));
    EXPECT_TRUE(txn.DeleteVertex(person_label, p1_vid));
    EXPECT_TRUE(txn.Commit());
  }

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto knows_label = gi.schema().get_edge_label_id("knows");
    EXPECT_EQ(count_edges(gi, person_label, person_label, knows_label, true),
              0);
    EXPECT_EQ(count_edges(gi, person_label, person_label, knows_label, false),
              0);
  }
  db.Close();
}

// Test DeleteVertex rollback restores edges correctly
TEST_F(UpdateTransactionTest, DeleteVertexAbortRestoresEdges) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  size_t initial_created_count, initial_knows_count;
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    neug::vid_t p1_vid, p2_vid;
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Property::from_int64(1),
                                   p1_vid));
    EXPECT_TRUE(txn.GetVertexIndex(software_label,
                                   neug::Property::from_int64(1), p2_vid));
    auto oe_view = txn.GetGenericOutgoingGraphView(person_label, software_label,
                                                   created_label);
    auto ie_view = txn.GetGenericIncomingGraphView(software_label, person_label,
                                                   created_label);
    auto props = txn.schema().get_edge_properties(person_label, software_label,
                                                  created_label);
    auto oe_edges = oe_view.get_edges(p1_vid);
    auto stride = oe_edges.cfg.stride;
    auto begin_ptr = reinterpret_cast<const char*>(oe_edges.start_ptr);
    for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
      if (it.get_vertex() != p2_vid) {
        continue;
      }
      auto oe_offset =
          (reinterpret_cast<const char*>(it.get_nbr_ptr()) - begin_ptr) /
          stride;
      auto ie_offset = neug::search_other_offset_with_cur_offset(
          oe_view, ie_view, p1_vid, p2_vid, oe_offset, props);
      EXPECT_TRUE(txn.DeleteEdge(person_label, p1_vid, software_label, p2_vid,
                                 created_label, oe_offset, ie_offset));
    }
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto created_label = gi.schema().get_edge_label_id("created");
    auto knows_label = gi.schema().get_edge_label_id("knows");
    initial_created_count =
        count_edges(gi, person_label, software_label, created_label, true);
    initial_knows_count =
        count_edges(gi, person_label, person_label, knows_label, true);
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    neug::vid_t p1_vid;
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Property::from_int64(1),
                                   p1_vid));
    EXPECT_TRUE(txn.DeleteVertex(person_label, p1_vid));
    txn.Abort();
  }
  {
    // Verify person 1 and all edges are restored
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto created_label = gi.schema().get_edge_label_id("created");
    auto knows_label = gi.schema().get_edge_label_id("knows");
    neug::vid_t p1_vid;
    EXPECT_TRUE(
        gi.GetVertexIndex(person_label, neug::Property::from_int64(1), p1_vid));
    EXPECT_EQ(count_vertices(gi, person_label), 2);
    EXPECT_EQ(
        count_edges(gi, person_label, software_label, created_label, true),
        initial_created_count);
    EXPECT_EQ(count_edges(gi, person_label, person_label, knows_label, true),
              initial_knows_count);
  }
  db.Close();
}

// Test DeleteVertex with multiple edge types
TEST_F(UpdateTransactionTest, DeleteVertexWithMultipleEdgeTypes) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    std::vector<std::tuple<neug::DataType, std::string, neug::Property>>
        edge_props = {std::make_tuple(neug::DataTypeId::kInt64, "since",
                                      neug::Property::from_int64(2020))};
    EXPECT_TRUE(txn.CreateEdgeType("person", "person", "follows", edge_props,
                                   true, neug::EdgeStrategy::kMultiple,
                                   neug::EdgeStrategy::kMultiple));

    neug::vid_t p1_vid, p2_vid;
    auto follows_label = txn.schema().get_edge_label_id("follows");
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Property::from_int64(1),
                                   p1_vid));
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Property::from_int64(2),
                                   p2_vid));
    EXPECT_TRUE(txn.AddEdge(person_label, p1_vid, person_label, p2_vid,
                            follows_label, {neug::Property::from_int64(2022)}));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    neug::vid_t p1_vid;
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Property::from_int64(1),
                                   p1_vid));
    EXPECT_TRUE(txn.DeleteVertex(person_label, p1_vid));
    EXPECT_TRUE(txn.Commit());
  }
  {
    // Verify all edge types are deleted
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto created_label = gi.schema().get_edge_label_id("created");
    auto knows_label = gi.schema().get_edge_label_id("knows");
    auto follows_label = gi.schema().get_edge_label_id("follows");
    EXPECT_EQ(
        count_edges(gi, person_label, software_label, created_label, true), 1);
    EXPECT_EQ(count_edges(gi, person_label, person_label, knows_label, true),
              0);
    EXPECT_EQ(count_edges(gi, person_label, person_label, follows_label, true),
              0);
  }

  db.Close();
}

TEST_F(UpdateTransactionTest, TestCheckpoint) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    interface.CreateCheckpoint();
  }
}

TEST_F(UpdateTransactionTest, TestUnsupportedInterface) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    std::vector<neug::vid_t> vids;
    std::vector<std::tuple<neug::vid_t, neug::vid_t>> edges;
    std::vector<std::pair<neug::vid_t, int32_t>> oe_edges, ie_edges;
    EXPECT_EQ(interface.BatchAddVertices(0, nullptr).error_code(),
              neug::StatusCode::ERR_NOT_SUPPORTED);
    EXPECT_EQ(interface.BatchAddEdges(0, 0, 0, nullptr).error_code(),
              neug::StatusCode::ERR_NOT_SUPPORTED);
  }
}

TEST_F(UpdateTransactionTest, BatchDeleteVertices) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    auto person_label = txn.schema().get_vertex_label_id("person");
    neug::vid_t alice_vid, bob_vid;
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Property::from_int64(1),
                                   alice_vid));
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Property::from_int64(2),
                                   bob_vid));
    std::vector<neug::vid_t> lids = {alice_vid, bob_vid};
    EXPECT_EQ(interface.BatchDeleteVertices(person_label, lids).error_code(),
              neug::StatusCode::OK);
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    EXPECT_EQ(count_vertices(gi, person_label), 0);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, BatchDeleteEdges) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    neug::vid_t p1_vid, p2_vid, s1_vid, s2_vid;
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Property::from_int64(1),
                                   p1_vid));
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Property::from_int64(2),
                                   p2_vid));
    EXPECT_TRUE(txn.GetVertexIndex(software_label,
                                   neug::Property::from_int64(1), s1_vid));
    EXPECT_TRUE(txn.GetVertexIndex(software_label,
                                   neug::Property::from_int64(2), s2_vid));
    std::vector<std::tuple<neug::vid_t, neug::vid_t>> edges = {
        std::make_tuple(p1_vid, s1_vid), std::make_tuple(p2_vid, s2_vid)};
    EXPECT_EQ(interface.BatchDeleteEdges(person_label, software_label,
                                         created_label, edges)
                  .error_code(),
              neug::StatusCode::OK);
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto created_label = gi.schema().get_edge_label_id("created");
    EXPECT_EQ(
        count_edges(gi, person_label, software_label, created_label, true), 0);
    EXPECT_EQ(
        count_edges(gi, software_label, person_label, created_label, false), 0);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, BatchDeleteVerticesFailure) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    auto person_label = txn.schema().get_vertex_label_id("person");
    neug::vid_t alice_vid;
    EXPECT_TRUE(txn.GetVertexIndex(person_label, neug::Property::from_int64(1),
                                   alice_vid));
    auto invalid_vid = std::numeric_limits<neug::vid_t>::max();
    EXPECT_THROW(
        interface.BatchDeleteVertices(person_label, {alice_vid, invalid_vid}),
        neug::exception::Exception);
    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    EXPECT_EQ(count_vertices(gi, person_label), 2);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, BatchDeleteEdgesFailure) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    auto invalid_vid = std::numeric_limits<neug::vid_t>::max();
    std::vector<std::tuple<neug::vid_t, neug::vid_t>> edges = {
        std::make_tuple(invalid_vid, invalid_vid)};
    EXPECT_THROW(interface.BatchDeleteEdges(person_label, software_label,
                                            created_label, edges),
                 neug::exception::Exception);
    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto created_label = gi.schema().get_edge_label_id("created");
    EXPECT_EQ(
        count_edges(gi, person_label, software_label, created_label, true), 2);
    EXPECT_EQ(
        count_edges(gi, software_label, person_label, created_label, false), 2);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, TestUpdateStringProperty) {
  // By default, the string property has max length: STRING_DEFAULT_MAX_LENGTH.
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    auto person_label = txn.schema().get_vertex_label_id("person");
    neug::vid_t p1_vid, p2_vid;
    EXPECT_TRUE(interface.GetVertexIndex(
        person_label, neug::Property::from_int64(1), p1_vid));
    EXPECT_TRUE(interface.GetVertexIndex(
        person_label, neug::Property::from_int64(2), p2_vid));
    std::string long_name(neug::STRING_DEFAULT_MAX_LENGTH + 10, 'a');
    interface.UpdateVertexProperty(person_label, p1_vid, 0,
                                   neug::Property::from_string_view(long_name));
    auto prop = interface.GetVertexProperty(person_label, p1_vid, 0);
    EXPECT_EQ(prop.as_string_view(),
              std::string(neug::STRING_DEFAULT_MAX_LENGTH, 'a'));  // truncated
    std::string valid_name(neug::STRING_DEFAULT_MAX_LENGTH - 10, 'b');
    EXPECT_NO_THROW(interface.UpdateVertexProperty(
        person_label, p1_vid, 0, neug::Property::from_string_view(valid_name)));
    prop = interface.GetVertexProperty(person_label, p1_vid, 0);
    EXPECT_EQ(prop.as_string_view(), valid_name);
    auto p2_prop = interface.GetVertexProperty(person_label, p2_vid, 0);
    EXPECT_EQ(p2_prop.as_string_view(), "Bob");  // unchanged
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    neug::vid_t p1_vid;
    EXPECT_TRUE(
        gi.GetVertexIndex(person_label, neug::Property::from_int64(1), p1_vid));
    auto vprop_accessor = std::dynamic_pointer_cast<
        neug::StorageReadInterface::vertex_column_t<std::string_view>>(
        gi.GetVertexPropColumn(person_label, "name"));
    EXPECT_EQ(vprop_accessor->get(p1_vid).as_string_view(),
              std::string(neug::STRING_DEFAULT_MAX_LENGTH - 10, 'b'));
    EXPECT_TRUE(txn.Commit());
  }
}

TEST_F(UpdateTransactionTest, TestUpdateEdgeStringPropertyCompact) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  config.checkpoint_on_close = true;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  std::vector<std::string> reviews;
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    reviews = create_string_prop_relation(interface, 300);
    EXPECT_TRUE(txn.Commit());
  }
  CHECK_EQ(reviews.size(), 300);

  // Verify initial reviews
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto review_label = gi.schema().get_edge_label_id("reviewed");

    auto fetched_views = fetch_edge_string_properties(
        gi, person_label, software_label, review_label);
    verify_string_views(reviews, fetched_views);
    EXPECT_TRUE(txn.Commit());
  }

  std::vector<std::string> updated_views;
  {
    // Update edge string property with suffix: "_updated"
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    neug::StorageTPUpdateInterface interface(txn);
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto review_label = txn.schema().get_edge_label_id("reviewed");
    auto oe_view = txn.GetGenericOutgoingGraphView(person_label, software_label,
                                                   review_label);
    auto ie_view = txn.GetGenericIncomingGraphView(software_label, person_label,
                                                   review_label);
    auto e_prop_types = txn.schema().get_edge_properties(
        person_label, software_label, review_label);
    auto ed_accessor =
        txn.GetEdgeDataAccessor(person_label, software_label, review_label, 0);
    auto vertex_set = interface.GetVertexSet(person_label);
    for (neug::vid_t vid : vertex_set) {
      auto edges = oe_view.get_edges(vid);
      auto begin = edges.start_ptr;
      for (auto it = edges.begin(); it != edges.end(); ++it) {
        uint32_t oe_offset = (reinterpret_cast<const char*>(it.get_nbr_ptr()) -
                              reinterpret_cast<const char*>(begin)) /
                             edges.cfg.stride;
        auto ie_offset = neug::search_other_offset_with_cur_offset(
            oe_view, ie_view, vid, it.get_vertex(), oe_offset, e_prop_types);
        auto prop = ed_accessor.get_data(it).as_string_view();
        std::string updated_review;
        if (prop.size() % 2 == 0) {
          updated_review = std::string(prop) + "_updated";
        } else {
          updated_review = "";
        }
        txn.UpdateEdgeProperty(
            person_label, vid, software_label, it.get_vertex(), review_label,
            oe_offset, ie_offset, 0,
            neug::Property::from_string_view(updated_review));
        updated_views.push_back(updated_review);
      }
    }
    EXPECT_TRUE(txn.Commit());
  }

  // Verify updated reviews
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto review_label = gi.schema().get_edge_label_id("reviewed");

    auto fetched_views = fetch_edge_string_properties(
        gi, person_label, software_label, review_label);
    verify_string_views(updated_views, fetched_views);
    EXPECT_TRUE(txn.Commit());
  }

  // When closing, the string column should be compacted when creating
  // checkpoint.
  db.Close();
  neug::NeugDB db2;
  db2.Open(config);

  // Verify reviews persist after compaction
  {
    auto svc2 = std::make_shared<neug::NeugDBService>(db2);
    auto sess = svc2->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto review_label = gi.schema().get_edge_label_id("reviewed");

    auto fetched_views = fetch_edge_string_properties(
        gi, person_label, software_label, review_label);
    verify_string_views(updated_views, fetched_views);
    EXPECT_TRUE(txn.Commit());
  }
}

TEST_F(UpdateTransactionTest, TestTPServiceStart) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto conn = db.Connect();
  auto svc = std::make_shared<neug::NeugDBService>(db);
  // conn should be closed when tp service start
  auto result = conn->Query(
      "MATCH (a:person {id: 1})-[r:created]->(b:software) "
      "RETURN r.weight AS weight, r.since AS since;");
  EXPECT_FALSE(result);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    neug::vid_t vertex_id;
    CHECK(txn.GetVertexIndex(person_label, neug::Property::from_int64(1),
                             vertex_id));

    update_edge_property(
        txn, person_label, software_label, created_label, vertex_id,
        [](neug::vid_t dst_vid) { return true; },
        [&](neug::vid_t dst_vid, int32_t oe_offset, int32_t ie_offset) {
          txn.UpdateEdgeProperty(person_label, vertex_id, software_label,
                                 dst_vid, created_label, oe_offset, ie_offset,
                                 0, neug::Property::from_double(0.9));
          txn.UpdateEdgeProperty(person_label, vertex_id, software_label,
                                 dst_vid, created_label, oe_offset, ie_offset,
                                 1, neug::Property::from_int64(2023));
        });

    txn.Abort();
  }
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto created_label = gi.schema().get_edge_label_id("created");
    auto view = gi.GetGenericOutgoingGraphView(person_label, software_label,
                                               created_label);
    auto ed_accessor =
        gi.GetEdgeDataAccessor(person_label, software_label, created_label, 0);
    auto since_accessor =
        gi.GetEdgeDataAccessor(person_label, software_label, created_label, 1);
    auto vertex_set = gi.GetVertexSet(person_label);
    for (neug::vid_t vid : vertex_set) {
      auto oid = gi.GetVertexId(person_label, vid);
      if (oid.as_int64() == 1) {
        auto edge_iter = view.get_edges(vid);
        for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
          EXPECT_EQ(ed_accessor.get_data(it).as_double(), 0.8);
          EXPECT_EQ(since_accessor.get_data(it).as_int64(), 2021);
        }
      }
    }
  }
  db.Close();
  {
    neug::NeugDB db2;
    neug::NeugDBConfig config2(db_dir);
    config2.memory_level = neug::MemoryLevel::kInMemory;
    db2.Open(config2);
    auto conn2 = db2.Connect();
    auto result = conn2->Query(
        "MATCH (a:person {id: 1})-[r:created]->(b:software) "
        "RETURN r.weight AS weight, r.since AS since;");
    EXPECT_TRUE(result);
    const auto& value = result.value();
    neug::test::AssertDoubleColumn(value.response(), 0, {0.8});
    neug::test::AssertInt64Column(value.response(), 1, {2021});
  }
}