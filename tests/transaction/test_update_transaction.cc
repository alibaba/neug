
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
    gs::runtime::GraphReadInterface gi(txn);
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
  LOG(INFO) << "Open DB done";
  db.SwitchToTPMode();
  LOG(INFO) << "Switch to TP mode done";
  {
    LOG(INFO) << "Test AddEdgeAbort";
    auto txn = db.GetUpdateTransaction();
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    LOG(INFO) << "Get labels done";
    EXPECT_TRUE(txn.AddEdge(
        person_label, gs::Property::from_int64(2), software_label,
        gs::Property::from_int64(1), created_label,
        {gs::Property::from_double(0.8), gs::Property::from_int64(2021)}));
    LOG(INFO) << "AddEdge done";
    txn.Abort();
    LOG(INFO) << "Abort done";
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
    gs::runtime::GraphReadInterface gi(txn);
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