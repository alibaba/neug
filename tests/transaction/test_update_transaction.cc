
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

#include "neug/execution/common/graph_interface.h"
#include "neug/neug.h"
#include "neug/transaction/update_transaction.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

class UpdateTransactionTest : public ::testing::Test {
 protected:
  std::string db_dir;
  int memory_level;

  void SetUp() override {
    db_dir = "/tmp/test_update_transaction_db";
    if (std::filesystem::exists(db_dir)) {
      std::filesystem::remove_all(db_dir);
    }
    std::filesystem::create_directories(db_dir);

    gs::NeugDB db;
    gs::NeugDBConfig config(db_dir);
    config.memory_level = 1;
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

  size_t count_edges_filter_src(const gs::ReadTransaction& txn,
                                gs::label_t src_label,
                                gs::label_t neighbor_label,
                                gs::label_t edge_label, gs::vid_t src_vid,
                                bool oe) {
    size_t edge_count = 0;
    auto view = oe ? txn.GetGenericOutgoingGraphView(src_label, neighbor_label,
                                                     edge_label)
                   : txn.GetGenericIncomingGraphView(src_label, neighbor_label,
                                                     edge_label);
    auto edge_iter = view.get_edges(src_vid);
    for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
      edge_count++;
    }
    return edge_count;
  }

  size_t count_edges(const gs::ReadTransaction& txn, gs::label_t src_label,
                     gs::label_t neighbor_label, gs::label_t edge_label,
                     bool oe) {
    size_t edge_count = 0;
    auto view = oe ? txn.GetGenericOutgoingGraphView(src_label, neighbor_label,
                                                     edge_label)
                   : txn.GetGenericIncomingGraphView(src_label, neighbor_label,
                                                     edge_label);
    auto v_set = txn.GetVertexSet(src_label);
    v_set.foreach_vertex([&](gs::vid_t vid) {
      auto edge_iter = view.get_edges(vid);
      for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
        edge_count++;
      }
    });
    return edge_count;
  }

  void create_new_edge_type(gs::UpdateTransaction& txn, gs::label_t& cmp_label,
                            gs::label_t& dev_label, gs::label_t& employ_label) {
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    std::vector<std::tuple<gs::PropertyType, std::string, gs::Property>>
        edge_props = {std::make_tuple(gs::PropertyType::Double(), "rating",
                                      gs::Property::from_double(0.0)),
                      std::make_tuple(gs::PropertyType::Int64(), "year",
                                      gs::Property::from_int64(2000))};
    EXPECT_TRUE(txn.CreateEdgeType(
        "person", "software", "developed", edge_props, true,
        gs::EdgeStrategy::kMultiple, gs::EdgeStrategy::kMultiple));
    std::vector<std::tuple<gs::PropertyType, std::string, gs::Property>>
        v_props = {std::make_tuple(gs::PropertyType::Int64(), "id",
                                   gs::Property::from_int64(0)),
                   std::make_tuple(gs::PropertyType::StringView(), "name",
                                   gs::Property::from_string(""))};
    EXPECT_TRUE(txn.CreateVertexType("company", v_props, {"id"}, true));
    EXPECT_TRUE(txn.CreateEdgeType("person", "company", "employed_by", {}, true,
                                   gs::EdgeStrategy::kMultiple,
                                   gs::EdgeStrategy::kMultiple));
    employ_label = txn.schema().get_edge_label_id("employed_by");
    cmp_label = txn.schema().get_vertex_label_id("company");
    dev_label = txn.schema().get_edge_label_id("developed");
    EXPECT_TRUE(txn.AddVertex(cmp_label, gs::Property::from_int64(1),
                              {gs::Property::from_string("TechCorp")}));
    gs::vid_t p1_vid;
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, gs::Property::from_int64(1), p1_vid));
    gs::vid_t software_vid;
    EXPECT_TRUE(txn.GetVertexIndex(software_label, gs::Property::from_int64(1),
                                   software_vid));
    gs::vid_t cmp_vid;
    EXPECT_TRUE(
        txn.GetVertexIndex(cmp_label, gs::Property::from_int64(1), cmp_vid));
    EXPECT_TRUE(txn.AddEdge(
        person_label, p1_vid, software_label, software_vid, dev_label,
        {gs::Property::from_double(4.5), gs::Property::from_int64(2023)}));
    EXPECT_TRUE(txn.AddEdge(person_label, p1_vid, cmp_label, cmp_vid,
                            employ_label, {}));
  }
};

TEST_F(UpdateTransactionTest, AddVertex) {
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    EXPECT_TRUE(txn.AddVertex(
        person_label, gs::Property::from_int64(3),
        {gs::Property::from_string("Eve"), gs::Property::from_int64(28)}));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    EXPECT_EQ(txn.GetVertexNum(person_label), 3);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, AddVertexBatch) {
  // To trigger the internal resize of vertex property columns,
  // we add a batch of vertices.
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    for (int i = 4; i <= 10000; i++) {
      EXPECT_TRUE(txn.AddVertex(person_label, gs::Property::from_int64(i),
                                {gs::Property::from_string("User"),
                                 gs::Property::from_int64(20 + i % 10)}));
    }
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    EXPECT_EQ(txn.GetVertexNum(person_label), 9999);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, AddEdge) {
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    EXPECT_TRUE(txn.AddEdge(
        person_label, gs::Property::from_int64(1), software_label,
        gs::Property::from_int64(2), created_label,
        {gs::Property::from_double(0.9), gs::Property::from_int64(2022)}));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    auto view = txn.GetGenericOutgoingGraphView(person_label, software_label,
                                                created_label);

    size_t edge_count = 0;
    for (gs::vid_t vid = 0; vid < txn.GetVertexNum(person_label); vid++) {
      auto oid = txn.GetVertexId(person_label, vid);
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
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    EXPECT_TRUE(txn.AddVertex(
        person_label, gs::Property::from_int64(4),
        {gs::Property::from_string("David"), gs::Property::from_int64(32)}));
    EXPECT_TRUE(txn.AddVertex(software_label, gs::Property::from_int64(3),
                              {gs::Property::from_string("NeugDB"),
                               gs::Property::from_string("C++")}));
    EXPECT_TRUE(txn.AddEdge(
        person_label, gs::Property::from_int64(4), software_label,
        gs::Property::from_int64(3), created_label,
        {gs::Property::from_double(0.85), gs::Property::from_int64(2023)}));
    EXPECT_TRUE(txn.AddEdge(
        person_label, gs::Property::from_int64(2), software_label,
        gs::Property::from_int64(3), created_label,
        {gs::Property::from_double(0.75), gs::Property::from_int64(2021)}));
    EXPECT_TRUE(txn.AddEdge(person_label, gs::Property::from_int64(4),
                            person_label, gs::Property::from_int64(1),
                            txn.schema().get_edge_label_id("knows"),
                            {gs::Property::from_double(0.95)}));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    EXPECT_EQ(txn.GetVertexNum(person_label), 3);
    auto software_label = txn.schema().get_vertex_label_id("software");
    EXPECT_EQ(txn.GetVertexNum(software_label), 3);
    auto created_label = txn.schema().get_edge_label_id("created");
    auto knows_label = txn.schema().get_edge_label_id("knows");
    gs::vid_t david_vid;
    EXPECT_TRUE(txn.GetVertexIndex(person_label, gs::Property::from_int64(4),
                                   david_vid));
    EXPECT_EQ(count_edges_filter_src(txn, person_label, software_label,
                                     created_label, david_vid, true),
              1);
    gs::vid_t neugdb_vid;
    EXPECT_TRUE(txn.GetVertexIndex(software_label, gs::Property::from_int64(3),
                                   neugdb_vid));
    EXPECT_EQ(count_edges_filter_src(txn, software_label, person_label,
                                     created_label, neugdb_vid, false),
              2);
    EXPECT_EQ(count_edges_filter_src(txn, person_label, person_label,
                                     knows_label, david_vid, true),
              1);
  }

  db.Close();
}

TEST_F(UpdateTransactionTest, AddVertexEdgeAbort) {
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    EXPECT_TRUE(txn.AddVertex(
        person_label, gs::Property::from_int64(5),
        {gs::Property::from_string("Frank"), gs::Property::from_int64(27)}));
    EXPECT_TRUE(txn.AddVertex(software_label, gs::Property::from_int64(4),
                              {gs::Property::from_string("UltraGraph"),
                               gs::Property::from_string("Go")}));
    EXPECT_TRUE(txn.AddEdge(
        person_label, gs::Property::from_int64(5), software_label,
        gs::Property::from_int64(4), created_label,
        {gs::Property::from_double(0.65), gs::Property::from_int64(2022)}));
    txn.Abort();
  }
  {
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    EXPECT_EQ(txn.GetVertexNum(person_label), 2);
    auto software_label = txn.schema().get_vertex_label_id("software");
    EXPECT_EQ(txn.GetVertexNum(software_label), 2);
    auto created_label = txn.schema().get_edge_label_id("created");
    auto oe_view = txn.GetGenericOutgoingGraphView(person_label, software_label,
                                                   created_label);
    size_t edge_count = 0;
    for (gs::vid_t vid = 0; vid < txn.GetVertexNum(person_label); vid++) {
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
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    gs::vid_t vertex_id;
    CHECK(txn.GetVertexIndex(person_label, gs::Property::from_int64(2),
                             vertex_id));
    EXPECT_TRUE(txn.UpdateVertexProperty(person_label, vertex_id, 1,
                                         gs::Property::from_int64(26)));

    EXPECT_TRUE(txn.Commit());
  }
  {
    auto txn = db.GetReadTransaction();
    gs::runtime::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto vprop_accessor = gi.GetVertexPropColumn<int64_t>(person_label, "age");
    for (gs::vid_t vid = 0; vid < txn.GetVertexNum(person_label); vid++) {
      auto oid = txn.GetVertexId(person_label, vid);
      if (oid.as_int64() == 2) {
        EXPECT_EQ(vprop_accessor->get_view(vid), 26);
      }
    }
  }
  db.Close();
}
TEST_F(UpdateTransactionTest, UpdateEdgeProperty) {
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    gs::vid_t vertex_id;
    CHECK(txn.GetVertexIndex(person_label, gs::Property::from_int64(1),
                             vertex_id));
    auto edge_iter = txn.GetOutEdgeIterator(person_label, vertex_id,
                                            software_label, created_label, 0);
    while (edge_iter.IsValid()) {
      edge_iter.SetData(gs::Property::from_double(0.99));
      edge_iter.Next();
    }

    EXPECT_TRUE(txn.Commit());
  }
  {
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    auto view = txn.GetGenericOutgoingGraphView(person_label, software_label,
                                                created_label);
    auto ed_accessor =
        txn.GetEdgeDataAccessor(person_label, software_label, created_label, 0);
    for (gs::vid_t vid = 0; vid < txn.GetVertexNum(person_label); vid++) {
      auto oid = txn.GetVertexId(person_label, vid);
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
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    EXPECT_TRUE(txn.AddVertex(
        person_label, gs::Property::from_int64(4),
        {gs::Property::from_string("Charlie"), gs::Property::from_int64(29)}));
    txn.Abort();
  }
  {
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    EXPECT_EQ(txn.GetVertexNum(person_label), 2);
  }
  {
    auto conn = db.Connect();
    auto result = conn->Query(
        "MATCH (n:person {id: 4}) RETURN n.name AS name, n.age AS age;");
    EXPECT_TRUE(result.value().length() == 0) << result.value().ToString();
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, AddEdgeAbort) {
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    EXPECT_TRUE(txn.AddEdge(
        person_label, gs::Property::from_int64(2), software_label,
        gs::Property::from_int64(1), created_label,
        {gs::Property::from_double(0.8), gs::Property::from_int64(2021)}));
    txn.Abort();
  }
  {
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    auto view = txn.GetGenericOutgoingGraphView(person_label, software_label,
                                                created_label);
    size_t edge_count = 0;
    for (gs::vid_t vid = 0; vid < txn.GetVertexNum(person_label); vid++) {
      auto oid = txn.GetVertexId(person_label, vid);
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
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    gs::vid_t vertex_id;
    CHECK(txn.GetVertexIndex(person_label, gs::Property::from_int64(2),
                             vertex_id));
    EXPECT_TRUE(txn.UpdateVertexProperty(person_label, vertex_id, 1,
                                         gs::Property::from_int64(27)));
    txn.Abort();
  }
  {
    auto txn = db.GetReadTransaction();
    gs::runtime::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto vprop_accessor = gi.GetVertexPropColumn<int64_t>(person_label, "age");
    for (gs::vid_t vid = 0; vid < txn.GetVertexNum(person_label); vid++) {
      auto oid = txn.GetVertexId(person_label, vid);
      if (oid.as_int64() == 2) {
        EXPECT_EQ(vprop_accessor->get_view(vid), 25);
      }
    }
  }
  {
    auto conn = db.Connect();
    auto result = conn->Query(
        "MATCH (n:person {id: 2}) RETURN n.name AS name, n.age AS age;");
    EXPECT_TRUE(result && result.value().hasNext());
    auto record = result.value().next();
    EXPECT_EQ(record.ToString(),
              "<element { object { str: \"Bob\" } }, element { object { i64: "
              "25 } }>");
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, UpdateEdgeAbort) {
  GTEST_SKIP()
      << "Currently not support update bundled edge property and abort";
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    gs::vid_t vertex_id;
    CHECK(txn.GetVertexIndex(person_label, gs::Property::from_int64(1),
                             vertex_id));

    auto edge_iter = txn.GetOutEdgeIterator(person_label, vertex_id,
                                            software_label, created_label, 0);
    while (edge_iter.IsValid()) {
      edge_iter.SetData(gs::Property::from_double(0.88));
      txn.SetEdgeData(true, person_label, vertex_id, software_label,
                      edge_iter.GetNeighbor(), created_label,
                      gs::Property::from_int64(2023), 1);
      txn.SetEdgeData(false, software_label, edge_iter.GetNeighbor(),
                      person_label, vertex_id, created_label,
                      gs::Property::from_int64(2023), 1);
      edge_iter.Next();
    }

    txn.Abort();
  }
  {
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    auto view = txn.GetGenericOutgoingGraphView(person_label, software_label,
                                                created_label);
    auto ed_accessor =
        txn.GetEdgeDataAccessor(person_label, software_label, created_label, 0);
    auto since_accessor =
        txn.GetEdgeDataAccessor(person_label, software_label, created_label, 1);
    for (gs::vid_t vid = 0; vid < txn.GetVertexNum(person_label); vid++) {
      auto oid = txn.GetVertexId(person_label, vid);
      if (oid.as_int64() == 1) {
        auto edge_iter = view.get_edges(vid);
        for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
          EXPECT_EQ(ed_accessor.get_data(it).as_double(), 0.8);
          EXPECT_EQ(since_accessor.get_data(it).as_int64(), 2021);
        }
      }
    }
  }
  {
    auto conn = db.Connect();
    auto result = conn->Query(
        "MATCH (a:person {id: 1})-[r:created]->(b:software {id: 1}) "
        "RETURN r.weight AS weight, r.since AS since;");
    auto& value = result.value();
    while (value.hasNext()) {
      auto record = value.next();
      EXPECT_EQ(record.ToString(),
                "<element { object { f64: 0.8 } }, element { object { i64: "
                "2021 } }>");
    }
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, UpdateEdgeAbort2) {
  GTEST_SKIP()
      << "Currently not support update bundled edge property and abort";
  // Update a bundled edge property and abort the transaction
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto knows_label = txn.schema().get_edge_label_id("knows");
    gs::vid_t vertex_id;
    CHECK(txn.GetVertexIndex(person_label, gs::Property::from_int64(1),
                             vertex_id));

    auto edge_iter = txn.GetOutEdgeIterator(person_label, vertex_id,
                                            person_label, knows_label, 0);
    while (edge_iter.IsValid()) {
      edge_iter.SetData(gs::Property::from_double(0.95));
      edge_iter.Next();
    }

    txn.Abort();
  }
  {
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto knows_label = txn.schema().get_edge_label_id("knows");
    auto view = txn.GetGenericOutgoingGraphView(person_label, person_label,
                                                knows_label);
    auto ed_accessor =
        txn.GetEdgeDataAccessor(person_label, person_label, knows_label, 0);
    for (gs::vid_t vid = 0; vid < txn.GetVertexNum(person_label); vid++) {
      auto oid = txn.GetVertexId(person_label, vid);
      if (oid.as_int64() == 1) {
        auto edge_iter = view.get_edges(vid);
        for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
          EXPECT_EQ(ed_accessor.get_data(it).as_double(), 0.9);
        }
      }
    }
  }
  {
    auto conn = db.Connect();
    auto result = conn->Query(
        "MATCH (a:person {id: 1})-[r:knows]->(b:person {id: 2}) "
        "RETURN r.closeness AS closeness;");
    auto& value = result.value();
    while (value.hasNext()) {
      auto record = value.next();
      EXPECT_EQ(record.ToString(), "<element { object { f64: 0.9 } }>");
    }
  }
}

TEST_F(UpdateTransactionTest, AddEdgeAndUpdateAndAbort) {
  GTEST_SKIP()
      << "Currently not support update bundled edge property and abort";
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    EXPECT_TRUE(txn.AddEdge(
        person_label, gs::Property::from_int64(1), software_label,
        gs::Property::from_int64(2), created_label,
        {gs::Property::from_double(0.85), gs::Property::from_int64(2023)}));
    gs::vid_t vertex_id;
    CHECK(txn.GetVertexIndex(person_label, gs::Property::from_int64(1),
                             vertex_id));
    auto edge_iter = txn.GetOutEdgeIterator(person_label, vertex_id,
                                            software_label, created_label, 0);
    while (edge_iter.IsValid()) {
      edge_iter.SetData(gs::Property::from_double(0.9));
      edge_iter.Next();
    }

    txn.Abort();
  }
  {
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    auto view = txn.GetGenericOutgoingGraphView(person_label, software_label,
                                                created_label);
    auto ed_accessor =
        txn.GetEdgeDataAccessor(person_label, software_label, created_label, 0);
    size_t edge_count = 0;
    for (gs::vid_t vid = 0; vid < txn.GetVertexNum(person_label); vid++) {
      auto oid = txn.GetVertexId(person_label, vid);
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
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    gs::vid_t vertex_id;
    CHECK(txn.GetVertexIndex(person_label, gs::Property::from_int64(2),
                             vertex_id));
    EXPECT_TRUE(txn.DeleteVertex(person_label, vertex_id));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto created_label = txn.schema().get_edge_label_id("created");
    auto software_label = txn.schema().get_vertex_label_id("software");
    EXPECT_EQ(txn.GetVertexNum(person_label), 1);
    gs::vid_t vertex_id;
    EXPECT_FALSE(txn.GetVertexIndex(person_label, gs::Property::from_int64(2),
                                    vertex_id));
    EXPECT_EQ(
        count_edges(txn, person_label, software_label, created_label, true), 1);
    // TODO(zhanglei): comment out this line when issue #1132 is solved.
    // EXPECT_EQ(
    //     count_edges(txn, software_label, person_label, created_label, false),
    //     1);
  }
  {
    // Delete person label and then delete vertex should throw
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    EXPECT_TRUE(txn.DeleteVertexType("person"));
    gs::vid_t vertex_id;
    EXPECT_THROW(txn.GetVertexIndex(person_label, gs::Property::from_int64(1),
                                    vertex_id),
                 gs::exception::Exception);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, AddDeleteVertexAbort) {
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    gs::vid_t vertex_id;
    CHECK(txn.GetVertexIndex(person_label, gs::Property::from_int64(2),
                             vertex_id));
    EXPECT_TRUE(txn.DeleteVertex(person_label, vertex_id));
    EXPECT_TRUE(txn.AddVertex(
        person_label, gs::Property::from_int64(3),
        {gs::Property::from_string("Eve"), gs::Property::from_int64(28)}));
    txn.Abort();
  }
  {
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    EXPECT_EQ(txn.GetVertexNum(person_label), 2);
    gs::vid_t vertex_id;
    EXPECT_TRUE(txn.GetVertexIndex(person_label, gs::Property::from_int64(2),
                                   vertex_id));
    EXPECT_FALSE(txn.GetVertexIndex(person_label, gs::Property::from_int64(3),
                                    vertex_id));
  }
  {
    // Add again
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    EXPECT_TRUE(txn.AddVertex(
        person_label, gs::Property::from_int64(3),
        {gs::Property::from_string("Eve"), gs::Property::from_int64(28)}));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    EXPECT_EQ(txn.GetVertexNum(person_label), 3);
    gs::vid_t vertex_id;
    EXPECT_TRUE(txn.GetVertexIndex(person_label, gs::Property::from_int64(3),
                                   vertex_id));
    EXPECT_EQ(txn.GetVertexNum(person_label), 3);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, CreteEdgeTypeAndAbort) {
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  gs::label_t dev_label, employ_label, cmp_label;
  {
    auto txn = db.GetUpdateTransaction();
    create_new_edge_type(txn, cmp_label, dev_label, employ_label);
    txn.Abort();
  }
  {
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    EXPECT_THROW(txn.schema().get_edge_label_id("developed"),
                 gs::exception::Exception);
    EXPECT_THROW(txn.schema().get_edge_label_id("employed_by"),
                 gs::exception::Exception);
    EXPECT_THROW(txn.schema().get_vertex_label_id("company"),
                 gs::exception::Exception);
    EXPECT_FALSE(
        txn.schema().has_edge_label("person", "company", "employed_by"));
    EXPECT_THROW(
        txn.GetGenericOutgoingGraphView(person_label, cmp_label, employ_label),
        gs::exception::Exception);
    EXPECT_THROW(txn.GetGenericOutgoingGraphView(person_label, software_label,
                                                 dev_label),
                 gs::exception::Exception);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, CreteEdgeTypeAndCommit) {
  GTEST_SKIP() << "Enable this test after in-place AddEdge is supported";
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  gs::label_t dev_label, employ_label, cmp_label;
  {
    auto txn = db.GetUpdateTransaction();
    create_new_edge_type(txn, cmp_label, dev_label, employ_label);
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    EXPECT_EQ(txn.schema().get_edge_label_id("developed"), dev_label);
    EXPECT_EQ(txn.schema().get_edge_label_id("employed_by"), employ_label);
    EXPECT_EQ(txn.schema().get_vertex_label_id("company"), cmp_label);
    EXPECT_EQ(count_edges(txn, person_label, software_label, dev_label, true),
              1);
    EXPECT_EQ(count_edges(txn, person_label, cmp_label, employ_label, true), 1);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, DeleteEdgeTypeAbort) {
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    auto created_label = txn.schema().get_edge_label_id("created");
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    EXPECT_TRUE(txn.DeleteEdgeType("person", "software", "created", true));
    EXPECT_FALSE(txn.schema().edge_triplet_valid(person_label, software_label,
                                                 created_label));
    txn.Abort();
  }
  {
    auto txn = db.GetReadTransaction();
    auto created_label = txn.schema().get_edge_label_id("created");
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    EXPECT_TRUE(txn.schema().exist("person", "software", "created"));
    EXPECT_TRUE(txn.schema().edge_triplet_valid(person_label, software_label,
                                                created_label));
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, AddVertexProperties) {
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    std::vector<std::tuple<gs::PropertyType, std::string, gs::Property>>
        new_props = {std::make_tuple(gs::PropertyType::StringView(), "email",
                                     gs::Property::from_string("")),
                     std::make_tuple(gs::PropertyType::Double(), "height",
                                     gs::Property::from_double(0.0))};
    EXPECT_TRUE(txn.AddVertexProperties("person", new_props, true));
    auto email_accessor = txn.get_vertex_property_column(person_label, "email");
    gs::vid_t vid;
    CHECK(txn.GetVertexIndex(person_label, gs::Property::from_int64(1), vid));
    EXPECT_TRUE(txn.UpdateVertexProperty(
        person_label, vid, 2, gs::Property::from_string("eve@example.com")));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto height_accessor =
        txn.get_vertex_property_column(person_label, "height");
    std::vector<std::tuple<gs::PropertyType, std::string, gs::Property>>
        new_props = {std::make_tuple(gs::PropertyType::StringView(), "address",
                                     gs::Property::from_string(""))};
    EXPECT_TRUE(txn.AddVertexProperties("person", new_props, true));
    gs::vid_t vid;
    CHECK(txn.GetVertexIndex(person_label, gs::Property::from_int64(2), vid));
    EXPECT_TRUE(txn.UpdateVertexProperty(person_label, vid, 3,
                                         gs::Property::from_double(175.5)));
    txn.Abort();
  }
  {
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    EXPECT_EQ(txn.get_vertex_property_column(person_label, "address"), nullptr);

    auto email_accessor = txn.get_vertex_property_column(person_label, "email");
    auto height_accessor =
        txn.get_vertex_property_column(person_label, "height");
    gs::vid_t vid;
    CHECK(txn.GetVertexIndex(person_label, gs::Property::from_int64(1), vid));
    EXPECT_EQ(email_accessor->get(vid).as_string_view(), "eve@example.com");
    CHECK(txn.GetVertexIndex(person_label, gs::Property::from_int64(2), vid));
    EXPECT_EQ(height_accessor->get(vid).as_double(), 0.0);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, AddEdgeProperties) {
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    std::vector<std::tuple<gs::PropertyType, std::string, gs::Property>>
        new_props = {std::make_tuple(gs::PropertyType::Int64(), "version",
                                     gs::Property::from_int64(0)),
                     std::make_tuple(gs::PropertyType::StringView(), "license",
                                     gs::Property::from_string(""))};
    EXPECT_TRUE(txn.AddEdgeProperties("person", "software", "created",
                                      new_props, true));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto txn = db.GetUpdateTransaction();
    std::vector<std::tuple<gs::PropertyType, std::string, gs::Property>>
        new_props = {std::make_tuple(gs::PropertyType::Double(),
                                     "contributions",
                                     gs::Property::from_double(0.0))};
    EXPECT_TRUE(txn.AddEdgeProperties("person", "software", "created",
                                      new_props, true));
    txn.Abort();
  }
  {
    auto txn = db.GetReadTransaction();
    auto created_label = txn.schema().get_edge_label_id("created");
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    EXPECT_EQ(txn.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("weight"),
              0);
    EXPECT_EQ(txn.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("since"),
              1);
    EXPECT_EQ(txn.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("version"),
              2);
    EXPECT_EQ(txn.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("license"),
              3);
    EXPECT_EQ(txn.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("contributions"),
              -1);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, RenameVertexProperty) {
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    EXPECT_TRUE(txn.RenameVertexProperties(
        "person", {std::make_pair("age", "years")}, true));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto txn = db.GetUpdateTransaction();
    std::vector<std::pair<std::string, std::string>> rename_props = {
        std::make_pair("lang", "language")};
    EXPECT_TRUE(txn.RenameVertexProperties("software", rename_props, true));
    txn.Abort();
  }
  {
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    EXPECT_EQ(txn.get_vertex_property_column(person_label, "age"), nullptr);
    EXPECT_NO_THROW(txn.get_vertex_property_column(person_label, "years"));
    EXPECT_EQ(txn.get_vertex_property_column(software_label, "language"),
              nullptr);
    EXPECT_NO_THROW(txn.get_vertex_property_column(software_label, "lang"));
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, RenameEdgeProperty) {
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    EXPECT_TRUE(txn.RenameEdgeProperties(
        "person", "software", "created",
        {std::make_pair("since", "start_year")}, true));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto txn = db.GetUpdateTransaction();
    std::vector<std::pair<std::string, std::string>> rename_props = {
        std::make_pair("weight", "importance")};
    EXPECT_TRUE(txn.RenameEdgeProperties("person", "software", "created",
                                         rename_props, true));
    txn.Abort();
  }
  {
    auto txn = db.GetReadTransaction();
    auto created_label = txn.schema().get_edge_label_id("created");
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    EXPECT_EQ(txn.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("since"),
              -1);
    EXPECT_EQ(txn.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("start_year"),
              1);
    EXPECT_EQ(txn.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("weight"),
              0);
    EXPECT_EQ(txn.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("importance"),
              -1);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, DeleteEdgeProperties) {
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    LOG(INFO) << "Starting delete edge properties transaction.";
    auto txn = db.GetUpdateTransaction();
    EXPECT_TRUE(txn.DeleteEdgeProperties("person", "software", "created",
                                         {"since"}, true));
    std::vector<std::tuple<gs::PropertyType, std::string, gs::Property>>
        new_props = {std::make_tuple(gs::PropertyType::Double(),
                                     "contributions",
                                     gs::Property::from_double(0.0))};
    LOG(INFO) << "Adding new edge property 'contributions'.";
    EXPECT_TRUE(txn.AddEdgeProperties("person", "software", "created",
                                      new_props, true));
    LOG(INFO) << "Committing delete edge properties transaction.";
    EXPECT_TRUE(txn.Commit());
    LOG(INFO) << "Committed delete edge properties transaction.";
  }
  {
    auto txn = db.GetUpdateTransaction();
    EXPECT_TRUE(txn.DeleteEdgeProperties("person", "software", "created",
                                         {"weight"}, true));
    txn.Abort();
    LOG(INFO) << "Aborted delete edge properties transaction.";
  }
  {
    auto txn = db.GetReadTransaction();
    auto created_label = txn.schema().get_edge_label_id("created");
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    EXPECT_EQ(txn.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("since"),
              -1);
    EXPECT_EQ(txn.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("weight"),
              0);
    EXPECT_EQ(txn.schema()
                  .get_edge_schema(person_label, software_label, created_label)
                  ->get_property_index("contributions"),
              1);
    auto ed_accessor = txn.GetEdgeDataAccessor(person_label, software_label,
                                               created_label, "contributions");
    auto view = txn.GetGenericOutgoingGraphView(person_label, software_label,
                                                created_label);
    LOG(INFO) << "Checking edge properties after delete.";
    for (gs::vid_t vid = 0; vid < txn.GetVertexNum(person_label); vid++) {
      auto edges = view.get_edges(vid);
      for (auto it = edges.begin(); it != edges.end(); ++it) {
        EXPECT_EQ(ed_accessor.get_data(it).as_double(), 0.0);
      }
    }
    EXPECT_THROW(txn.GetEdgeDataAccessor(person_label, software_label,
                                         created_label, "since"),
                 gs::exception::Exception);
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, DeleteVertexProperties) {
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  db.SwitchToTPMode();
  {
    auto txn = db.GetUpdateTransaction();
    // auto person_label = txn.schema().get_vertex_label_id("person");
    // auto software_label = txn.schema().get_vertex_label_id("software");
    EXPECT_TRUE(txn.DeleteVertexProperties("person", {"age"}, true));
    EXPECT_TRUE(txn.DeleteVertexProperties("software", {"lang"}, true));
    std::vector<std::tuple<gs::PropertyType, std::string, gs::Property>>
        new_props = {std::make_tuple(gs::PropertyType::StringView(), "authors",
                                     gs::Property::from_string(""))};
    EXPECT_TRUE(txn.AddVertexProperties("software", new_props, true));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto txn = db.GetUpdateTransaction();
    // auto person_label = txn.schema().get_vertex_label_id("person");
    EXPECT_TRUE(txn.DeleteVertexProperties("person", {"name"}, true));
    EXPECT_TRUE(
        txn.DeleteVertexProperties("software", {"name", "authors"}, true));
    txn.Abort();
  }
  {
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    EXPECT_EQ(txn.get_vertex_property_column(person_label, "age"), nullptr);
    EXPECT_NO_THROW(txn.get_vertex_property_column(person_label, "name"));
    EXPECT_EQ(txn.get_vertex_property_column(software_label, "lang"), nullptr);
    EXPECT_NO_THROW(txn.get_vertex_property_column(software_label, "name"));
    EXPECT_NO_THROW(txn.get_vertex_property_column(software_label, "authors"));
  }
  db.Close();
}

TEST_F(UpdateTransactionTest, TestReplayWal) {
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  config.checkpoint_on_close = false;
  config.compact_on_close = false;
  {
    gs::NeugDB db;
    db.Open(config);
    db.SwitchToTPMode();
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    EXPECT_TRUE(txn.AddVertex(
        person_label, gs::Property::from_int64(3),
        {gs::Property::from_string("Eve"), gs::Property::from_int64(28)}));
    EXPECT_TRUE(txn.CreateVertexType(
        "company",
        {std::make_tuple(gs::PropertyType::Int64(), "id",
                         gs::Property::from_int64(0)),
         std::make_tuple(gs::PropertyType::StringView(), "name",
                         gs::Property::from_string(""))},
        {"id"}, true));
    EXPECT_TRUE(txn.CreateEdgeType("person", "company", "employed_by", {}, true,
                                   gs::EdgeStrategy::kMultiple,
                                   gs::EdgeStrategy::kMultiple));
    EXPECT_TRUE(txn.DeleteEdgeType("person", "software", "created", true));
    EXPECT_TRUE(txn.DeleteVertexType("software"));
    gs::vid_t src_p, dst_p;
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, gs::Property::from_int64(1), src_p));
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, gs::Property::from_int64(2), dst_p));

    EXPECT_TRUE(txn.UpdateVertexProperty(person_label, src_p, 1,
                                         gs::Property::from_int64(29)));
    txn.SetEdgeData(true, person_label, src_p, person_label, dst_p,
                    txn.schema().get_edge_label_id("knows"),
                    gs::Property::from_double(0.5), 0);
    EXPECT_TRUE(txn.Commit());
    db.Close();
  }
  {
    gs::NeugDB db;
    db.Open(config);
    auto txn = db.GetReadTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    EXPECT_EQ(txn.GetVertexNum(person_label), 3);
    gs::vid_t src_p, dst_p;
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, gs::Property::from_int64(1), src_p));
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, gs::Property::from_int64(2), dst_p));
    auto vprop_accessor = txn.get_vertex_property_column(person_label, "age");
    EXPECT_EQ(vprop_accessor->get(src_p).as_int64(), 29);
    auto knows_label = txn.schema().get_edge_label_id("knows");
    auto ed_accessor =
        txn.GetEdgeDataAccessor(person_label, person_label, knows_label, 0);
    auto view = txn.GetGenericOutgoingGraphView(person_label, person_label,
                                                knows_label);
    auto edge_iter = view.get_edges(src_p);
    bool found = false;
    for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
      if (it.get_vertex() == dst_p) {
        EXPECT_EQ(ed_accessor.get_data(it).as_double(), 0.5);
        found = true;
      }
    }
    EXPECT_TRUE(found);
    EXPECT_FALSE(txn.schema().contains_vertex_label("software"));
    EXPECT_TRUE(txn.schema().contains_vertex_label("company"));
    EXPECT_FALSE(txn.schema().contains_edge_label("created"));
    EXPECT_TRUE(txn.schema().contains_edge_label("employed_by"));
    txn.Commit();
    db.Close();
  }
}