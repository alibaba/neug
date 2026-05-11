
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
#include "neug/transaction/wal/wal.h"
#include "neug/utils/serialization/in_archive.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

class InsertTransactionTest : public ::testing::Test {
 protected:
  std::string db_dir;
  neug::MemoryLevel memory_level;

  void SetUp() override {
    db_dir = "/tmp/test_insert_transaction_db";
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

  size_t count_vertices(const neug::StorageReadInterface& gi,
                        neug::label_t label) {
    size_t vertex_count = 0;
    auto v_set = gi.GetVertexSet(label);
    v_set.foreach_vertex([&](neug::vid_t vid) { vertex_count++; });
    return vertex_count;
  }

  size_t count_edges_from_vertex(const neug::StorageReadInterface& gi,
                                 neug::label_t src_label,
                                 neug::label_t neighbor_label,
                                 neug::label_t edge_label,
                                 neug::vid_t src_vid) {
    size_t edge_count = 0;
    auto view =
        gi.GetGenericOutgoingGraphView(src_label, neighbor_label, edge_label);
    auto edge_iter = view.get_edges(src_vid);
    for (auto it = edge_iter.begin(); it != edge_iter.end(); ++it) {
      edge_count++;
    }
    return edge_count;
  }
};

TEST_F(InsertTransactionTest, InsertTransactionBasic) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetInsertTransaction();
    EXPECT_EQ(txn.timestamp(), 1);
    EXPECT_TRUE(txn.schema().contains_vertex_label("person"));
  }
}

TEST_F(InsertTransactionTest, AddVertex) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetInsertTransaction();
    neug::StorageTPInsertInterface interface(txn);
    auto person_label = interface.schema().get_vertex_label_id("person");
    neug::vid_t vid;
    EXPECT_TRUE(interface.AddVertex(person_label, neug::Property::from_int64(3),
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

TEST_F(InsertTransactionTest, AddEdge) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetInsertTransaction();
    neug::StorageTPInsertInterface interface(txn);
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto software_label = txn.schema().get_vertex_label_id("software");
    auto created_label = txn.schema().get_edge_label_id("created");
    neug::vid_t vid;
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, neug::Property::from_int64(1), vid));
    neug::vid_t vid2;
    EXPECT_TRUE(txn.GetVertexIndex(software_label,
                                   neug::Property::from_int64(2), vid2));
    EXPECT_TRUE(interface.AddEdge(
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

TEST_F(InsertTransactionTest, TestUnsupportedInterface) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetInsertTransaction();
    neug::StorageTPInsertInterface interface(txn);
    std::vector<neug::vid_t> vids;
    std::vector<std::tuple<neug::vid_t, neug::vid_t>> edges;
    std::vector<std::pair<neug::vid_t, int32_t>> oe_edges, ie_edges;
    EXPECT_EQ(interface.BatchAddVertices(0, nullptr).error_code(),
              neug::StatusCode::ERR_NOT_SUPPORTED);
    EXPECT_EQ(interface.BatchAddEdges(0, 0, 0, nullptr).error_code(),
              neug::StatusCode::ERR_NOT_SUPPORTED);
  }
}

// Test: Insert vertex and edge in the same transaction, verify both are
// committed.
TEST_F(InsertTransactionTest, InsertVertexAndEdge) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetInsertTransaction();
    neug::StorageTPInsertInterface interface(txn);
    auto person_label = txn.schema().get_vertex_label_id("person");
    auto knows_label = txn.schema().get_edge_label_id("knows");

    // Insert a new person vertex (id=3)
    neug::vid_t new_vid;
    EXPECT_TRUE(
        interface.AddVertex(person_label, neug::Property::from_int64(3),
                            {neug::Property::from_string_view("Charlie"),
                             neug::Property::from_int64(35)},
                            new_vid));

    // Insert an edge person(1) -[knows]-> person(3) in the same transaction
    neug::vid_t vid1;
    EXPECT_TRUE(
        txn.GetVertexIndex(person_label, neug::Property::from_int64(1), vid1));
    EXPECT_TRUE(interface.AddEdge(person_label, vid1, person_label, new_vid,
                                  knows_label,
                                  {neug::Property::from_double(0.5)}));

    EXPECT_TRUE(txn.Commit());
  }
  {
    // Verify: person count should be 3 (was 2)
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto knows_label = gi.schema().get_edge_label_id("knows");

    EXPECT_EQ(count_vertices(gi, person_label), 3);

    // Verify: person(1) should have 2 knows edges (was 1)
    neug::vid_t vid1;
    EXPECT_TRUE(
        gi.GetVertexIndex(person_label, neug::Property::from_int64(1), vid1));
    EXPECT_EQ(count_edges_from_vertex(gi, person_label, person_label,
                                      knows_label, vid1),
              2);
  }
  db.Close();
}

// Test: Partial vertex insertion failure triggers rollback of previously
// inserted vertices.
TEST_F(InsertTransactionTest, RollbackVertexOnPartialFailure) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  // Record original vertex count
  size_t original_person_count;
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    original_person_count = count_vertices(gi, person_label);
    EXPECT_EQ(original_person_count, 2);
  }

  // Construct WAL data with two InsertVertex operations.
  // The first one is valid (id=10), the second one duplicates an existing
  // id (id=1) which should trigger a failure during IngestWal.
  {
    auto sess = svc->AcquireSession();
    auto person_label = db.graph().schema().get_vertex_label_id("person");

    neug::InArchive arc;
    // First vertex: valid new vertex
    neug::InsertVertexRedo::Serialize(
        arc, person_label, neug::Property::from_int64(10),
        {neug::Property::from_string_view("NewPerson"),
         neug::Property::from_int64(40)});
    // Second vertex: duplicate PK (id=1 already exists)
    neug::InsertVertexRedo::Serialize(
        arc, person_label, neug::Property::from_int64(1),
        {neug::Property::from_string_view("Duplicate"),
         neug::Property::from_int64(99)});

    neug::Allocator alloc(neug::MemoryLevel::kInMemory, "/tmp/test_alloc_");
    // IngestWal should throw due to duplicate PK, and roll back the first
    // vertex
    EXPECT_THROW(neug::InsertTransaction::IngestWal(
                     db.graph(), 100, arc.GetBuffer(), arc.GetSize(), alloc),
                 std::exception);
  }

  // Verify: vertex count should remain unchanged after rollback
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    EXPECT_EQ(count_vertices(gi, person_label), original_person_count);
  }
  db.Close();
}

// Test: Insert vertex + edge, then a failing vertex insertion rolls back
// everything (vertex and edge).
TEST_F(InsertTransactionTest, RollbackVertexAndEdgeOnFailure) {
  neug::NeugDB db;
  neug::NeugDBConfig config(db_dir);
  config.memory_level = neug::MemoryLevel::kInMemory;
  db.Open(config);
  auto svc = std::make_shared<neug::NeugDBService>(db);

  // Record original counts
  size_t original_person_count;
  size_t original_knows_edge_count;
  neug::vid_t vid1;
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto knows_label = gi.schema().get_edge_label_id("knows");
    original_person_count = count_vertices(gi, person_label);
    EXPECT_EQ(original_person_count, 2);

    EXPECT_TRUE(
        gi.GetVertexIndex(person_label, neug::Property::from_int64(1), vid1));
    original_knows_edge_count = count_edges_from_vertex(
        gi, person_label, person_label, knows_label, vid1);
    EXPECT_EQ(original_knows_edge_count, 1);
  }

  // Construct WAL data with:
  //   1. InsertVertex person(id=20) - should succeed
  //   2. InsertEdge person(1)-[knows]->person(20) - should succeed
  //   3. InsertVertex person(id=1) - duplicate PK, should fail
  {
    auto sess = svc->AcquireSession();
    auto person_label = db.graph().schema().get_vertex_label_id("person");
    auto knows_label = db.graph().schema().get_edge_label_id("knows");

    neug::InArchive arc;
    // 1. Valid new vertex
    neug::InsertVertexRedo::Serialize(
        arc, person_label, neug::Property::from_int64(20),
        {neug::Property::from_string_view("TempPerson"),
         neug::Property::from_int64(22)});
    // 2. Valid new edge: person(1) -[knows]-> person(20)
    neug::InsertEdgeRedo::Serialize(arc, person_label,
                                    neug::Property::from_int64(1), person_label,
                                    neug::Property::from_int64(20), knows_label,
                                    {neug::Property::from_double(0.3)});
    // 3. Duplicate vertex (id=1 already exists) - triggers failure
    neug::InsertVertexRedo::Serialize(
        arc, person_label, neug::Property::from_int64(1),
        {neug::Property::from_string_view("Duplicate"),
         neug::Property::from_int64(99)});

    neug::Allocator alloc(neug::MemoryLevel::kInMemory, "/tmp/test_alloc_");
    // IngestWal should throw and roll back all operations
    EXPECT_THROW(neug::InsertTransaction::IngestWal(
                     db.graph(), 100, arc.GetBuffer(), arc.GetSize(), alloc),
                 std::exception);
  }

  // Verify: all operations should be rolled back
  {
    auto sess = svc->AcquireSession();
    auto txn = sess->GetReadTransaction();
    neug::StorageReadInterface gi(txn.graph(), txn.timestamp());
    auto person_label = gi.schema().get_vertex_label_id("person");
    auto knows_label = gi.schema().get_edge_label_id("knows");

    // Vertex count should remain unchanged
    EXPECT_EQ(count_vertices(gi, person_label), original_person_count);

    // Edge count from person(1) should remain unchanged
    EXPECT_EQ(count_edges_from_vertex(gi, person_label, person_label,
                                      knows_label, vid1),
              original_knows_edge_count);
  }
  db.Close();
}