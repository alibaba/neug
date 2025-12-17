
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
#include "neug/transaction/insert_transaction.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

class InsertTransactionTest : public ::testing::Test {
 protected:
  std::string db_dir;
  int memory_level;

  void SetUp() override {
    db_dir = "/tmp/test_insert_transaction_db";
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

  size_t count_vertices(const gs::StorageReadInterface& gi, gs::label_t label) {
    size_t vertex_count = 0;
    auto v_set = gi.GetVertexSet(label);
    v_set.foreach_vertex([&](gs::vid_t vid) { vertex_count++; });
    return vertex_count;
  }
};

TEST_F(InsertTransactionTest, InsertTransactionBasic) {
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  auto svc = std::make_shared<server::NeugDBService>(db);
  {
    auto txn = svc->GetInsertTransaction();
    EXPECT_EQ(txn.timestamp(), 1);
    EXPECT_TRUE(txn.schema().contains_vertex_label("person"));
  }
}

TEST_F(InsertTransactionTest, AddVertex) {
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  auto svc = std::make_shared<server::NeugDBService>(db);
  {
    auto txn = svc->GetInsertTransaction();
    gs::StorageTPInsertInterface interface(txn);
    auto person_label = interface.schema().get_vertex_label_id("person");
    gs::vid_t vid;
    EXPECT_TRUE(interface.AddVertex(
        person_label, gs::Property::from_int64(3),
        {gs::Property::from_string("Eve"), gs::Property::from_int64(28)}, vid));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto txn = svc->GetReadTransaction();
    gs::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    EXPECT_EQ(count_vertices(gi, person_label), 3);
  }
  db.Close();
}

TEST_F(InsertTransactionTest, AddEdge) {
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  auto svc = std::make_shared<server::NeugDBService>(db);
  {
    auto txn = svc->GetInsertTransaction();
    gs::StorageTPInsertInterface interface(txn);
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    gs::vid_t vid;
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, gs::Property::from_int64(1), vid));
    gs::vid_t vid2;
    EXPECT_TRUE(
        txn.GetVertexIndex(software_label, gs::Property::from_int64(2), vid2));
    EXPECT_TRUE(interface.AddEdge(
        person_label, vid, software_label, vid2, created_label,
        {gs::Property::from_double(0.9), gs::Property::from_int64(2022)}));
    EXPECT_TRUE(txn.Commit());
  }
  {
    auto txn = svc->GetReadTransaction();
    gs::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto software_label = gi.schema().get_vertex_label_id("software");
    auto created_label = gi.schema().get_edge_label_id("created");
    auto view = gi.GetGenericOutgoingGraphView(person_label, software_label,
                                               created_label);

    size_t edge_count = 0;
    auto vertex_set = gi.GetVertexSet(person_label);
    for (gs::vid_t vid : vertex_set) {
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

TEST_F(InsertTransactionTest, TestUnsupportedInterface) {
  gs::NeugDB db;
  gs::NeugDBConfig config(db_dir);
  config.memory_level = 1;
  db.Open(config);
  auto svc = std::make_shared<server::NeugDBService>(db);

  {
    auto txn = svc->GetInsertTransaction();
    gs::StorageTPInsertInterface interface(txn);
    std::vector<gs::vid_t> vids;
    std::vector<std::tuple<gs::vid_t, gs::vid_t>> edges;
    std::vector<std::pair<gs::vid_t, int32_t>> oe_edges, ie_edges;
    EXPECT_EQ(interface.BatchAddVertices(0, nullptr).error_code(),
              gs::StatusCode::ERR_NOT_SUPPORTED);
    EXPECT_EQ(interface.BatchAddEdges(0, 0, 0, nullptr).error_code(),
              gs::StatusCode::ERR_NOT_SUPPORTED);
  }
}