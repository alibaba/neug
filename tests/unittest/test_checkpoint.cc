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
#include <iostream>
#include <string>
#include "neug/main/neug_db.h"
#include "neug/storages/file_names.h"
#include "neug/storages/graph/schema.h"

static const std::vector<std::string> basic_test_result_v = {
    "<element { object { i64: 1 } }, element { object { str: \"marko\" } }, "
    "element { object { i64: 29 } }>",
    "<element { object { i64: 2 } }, element { object { str: \"vadas\" } }, "
    "element { object { i64: 27 } }>",
    "<element { object { i64: 4 } }, element { object { str: \"josh\" } }, "
    "element { object { i64: 32 } }>",
    "<element { object { i64: 6 } }, element { object { str: \"peter\" } }, "
    "element { object { i64: 35 } }>"};
static const std::vector<std::string> basic_test_result_e = {
    "<element { object { f64: 0.5 } }>", "<element { object { f64: 1 } }>"};
static const std::vector<std::string> add_vertex_property_result_v = {
    "<element { object { i64: 1 } }, element { object { str: \"marko\" } }, "
    "element { object { i64: 29 } }, element { object { str: \"\" } }>",
    "<element { object { i64: 2 } }, element { object { str: \"vadas\" } }, "
    "element { object { i64: 27 } }, element { object { str: \"\" } }>",
    "<element { object { i64: 4 } }, element { object { str: \"josh\" } }, "
    "element { object { i64: 32 } }, element { object { str: \"\" } }>",
    "<element { object { i64: 6 } }, element { object { str: \"peter\" } }, "
    "element { object { i64: 35 } }, element { object { str: \"\" } }>"};
static const std::vector<std::string> add_vertex_property_result_e = {
    "<element { object { f64: 0.5 } }>", "<element { object { f64: 1 } }>"};
static const std::vector<std::string> delete_vertex_property_result_v = {
    "<element { object { i64: 1 } }, element { object { str: \"marko\" } }>",
    "<element { object { i64: 2 } }, element { object { str: \"vadas\" } }>",
    "<element { object { i64: 4 } }, element { object { str: \"josh\" } }>",
    "<element { object { i64: 6 } }, element { object { str: \"peter\" } }>"};
static const std::vector<std::string> delete_vertex_property_result_e = {
    "<element { object { f64: 0.5 } }>", "<element { object { f64: 1 } }>"};
static const std::vector<std::string> delete_vertex_result_v = {
    "<element { object { i64: 2 } }, element { object { str: \"vadas\" } }, "
    "element { object { i64: 27 } }>",
    "<element { object { i64: 4 } }, element { object { str: \"josh\" } }, "
    "element { object { i64: 32 } }>",
    "<element { object { i64: 6 } }, element { object { str: \"peter\" } }, "
    "element { object { i64: 35 } }>"};
static const std::vector<std::string> delete_vertex_result_e = {};
static const std::vector<std::string> add_edge_property_result_v = {
    "<element { object { i64: 1 } }, element { object { str: \"marko\" } }, "
    "element { object { i64: 29 } }>",
    "<element { object { i64: 2 } }, element { object { str: \"vadas\" } }, "
    "element { object { i64: 27 } }>",
    "<element { object { i64: 4 } }, element { object { str: \"josh\" } }, "
    "element { object { i64: 32 } }>",
    "<element { object { i64: 6 } }, element { object { str: \"peter\" } }, "
    "element { object { i64: 35 } }>"};
static const std::vector<std::string> add_edge_property_result_e = {
    "<element { object { f64: 0.5 } }, element { object { str: \"0-00-00\" } "
    "}>",
    "<element { object { f64: 1 } }, element { object { str: \"0-00-00\" } }>"};
static const std::vector<std::string> delete_edge_property_result_v = {
    "<element { object { i64: 1 } }, element { object { str: \"marko\" } }, "
    "element { object { i64: 29 } }>",
    "<element { object { i64: 2 } }, element { object { str: \"vadas\" } }, "
    "element { object { i64: 27 } }>",
    "<element { object { i64: 4 } }, element { object { str: \"josh\" } }, "
    "element { object { i64: 32 } }>",
    "<element { object { i64: 6 } }, element { object { str: \"peter\" } }, "
    "element { object { i64: 35 } }>"};
static const std::vector<std::string> delete_edge_property_result_e = {
    "<element { edge { id: 1 label { name: \"knows\" } src_label { name: "
    "\"person\" } dst_id: 1 dst_label { name: \"person\" } } }>",
    "<element { edge { id: 2 label { name: \"knows\" } src_label { name: "
    "\"person\" } dst_id: 2 dst_label { name: \"person\" } } }>"};
static const std::vector<std::string> delete_edge_result_v = {
    "<element { object { i64: 1 } }, element { object { str: \"marko\" } }, "
    "element { object { i64: 29 } }>",
    "<element { object { i64: 2 } }, element { object { str: \"vadas\" } }, "
    "element { object { i64: 27 } }>",
    "<element { object { i64: 4 } }, element { object { str: \"josh\" } }, "
    "element { object { i64: 32 } }>",
    "<element { object { i64: 6 } }, element { object { str: \"peter\" } }, "
    "element { object { i64: 35 } }>"};
static const std::vector<std::string> delete_edge_result_e = {};

namespace gs {

namespace test {

class CheckpointTest : public ::testing::Test {
 protected:
  static constexpr const char* DB_DIR = "/tmp/checkpoint_test";

  void SetUp() override {
    if (std::filesystem::exists(DB_DIR)) {
      std::filesystem::remove_all(DB_DIR);
    }
    std::filesystem::create_directories(DB_DIR);

    std::unique_ptr<gs::NeugDB> db_ = std::make_unique<gs::NeugDB>();
    gs::NeugDBConfig config;
    config.data_dir = DB_DIR;
    config.checkpoint_on_close = true;
    config.compact_on_close = true;
    config.compact_csr = true;
    config.enable_auto_compaction = false;  // TODO(zhanglei): very slow
    db_->Open(config);
    auto conn = db_->Connect();

    load_modern_graph(conn);
    LOG(INFO) << "[CheckPointTest]: Finished loading modern graph";
    conn->Close();
    db_->Close();
    db_.reset();
  }

  void TearDown() override {}

 protected:
  void load_modern_graph(std::shared_ptr<gs::Connection> conn) {
    const char* csv_dir_ptr = std::getenv("MODERN_GRAPH_DATA_DIR");
    if (csv_dir_ptr == nullptr) {
      throw std::runtime_error(
          "MODERN_GRAPH_DATA_DIR environment variable is not set");
    }
    LOG(INFO) << "CSV data dir: " << csv_dir_ptr;
    std::string csv_dir = csv_dir_ptr;
    auto res = conn->Query(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY "
        "KEY(id));");
    EXPECT_TRUE(res.ok()) << res.status().ToString();

    {
      auto res = conn->Query(
          "CREATE NODE TABLE software(id INT64, name STRING, lang STRING, "
          "PRIMARY "
          "KEY(id));");
      EXPECT_TRUE(res.ok()) << res.status().ToString();
    }
    {
      auto res = conn->Query(
          "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);");
      EXPECT_TRUE(res.ok()) << res.status().ToString();
    }

    {
      auto res = conn->Query(
          "CREATE REL TABLE created(FROM person TO software, weight DOUBLE, "
          "since INT64);");
      EXPECT_TRUE(res.ok()) << res.status().ToString();
    }

    {
      auto res =
          conn->Query("COPY person from \"" + csv_dir + "/person.csv\";");
      EXPECT_TRUE(res.ok()) << res.status().ToString();
    }

    {
      auto res =
          conn->Query("COPY software from \"" + csv_dir + "/software.csv\";");
      EXPECT_TRUE(res.ok()) << res.status().ToString();
    }

    {
      auto res = conn->Query(
          "COPY knows from \"" + csv_dir +
          "/person_knows_person.csv\" (from=\"person\", to=\"person\");");
      EXPECT_TRUE(res.ok()) << res.status().ToString();
    }

    {
      auto res = conn->Query("COPY created from \"" + csv_dir +
                             "/person_created_software.csv\" (from=\"person\", "
                             "to =\"software\");");
      EXPECT_TRUE(res.ok()) << res.status().ToString();
    }
  }
};

TEST_F(CheckpointTest, test_basic) {
  std::unique_ptr<gs::NeugDB> db_ = std::make_unique<gs::NeugDB>();
  db_->Open(DB_DIR);
  auto conn = db_->Connect();

  gs::PropertyGraph& frag = db_->graph();
  frag.Dump();

  std::vector<std::string> result_v, result_e;
  {
    auto res = conn->Query("MATCH (v:person) RETURN v.*;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    while (res_val.hasNext()) {
      auto row = res_val.next();
      result_v.emplace_back(row.ToString());
    }
  }
  assert(result_v == basic_test_result_v);
  {
    auto res = conn->Query("MATCH (v:person)-[e:knows]->(:person) RETURN e.*;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    while (res_val.hasNext()) {
      auto row = res_val.next();
      result_e.emplace_back(row.ToString());
    }
  }
  assert(result_e == basic_test_result_e);
}

TEST_F(CheckpointTest, test_after_add_vertex_property) {
  gs::NeugDB db;
  db.Open(DB_DIR);
  auto conn = db.Connect();

  {
    auto res = conn->Query("ALTER TABLE person ADD created STRING;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
  }

  gs::PropertyGraph& frag = db.graph();
  frag.Dump();

  std::vector<std::string> result_v, result_e;
  {
    auto res = conn->Query("MATCH (v:person) RETURN v.*;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    while (res_val.hasNext()) {
      auto row = res_val.next();
      result_v.emplace_back(row.ToString());
    }
  }
  assert(result_v == add_vertex_property_result_v);
  {
    auto res = conn->Query("MATCH (v:person)-[e:knows]->(:person) RETURN e.*;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    while (res_val.hasNext()) {
      auto row = res_val.next();
      result_e.emplace_back(row.ToString());
    }
  }
  assert(result_e == add_vertex_property_result_e);
}

TEST_F(CheckpointTest, test_after_delete_vertex_property) {
  gs::NeugDB db;
  db.Open(DB_DIR);
  auto conn = db.Connect();

  {
    auto res = conn->Query("ALTER TABLE person DROP age;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
  }

  gs::PropertyGraph& frag = db.graph();
  frag.Dump();

  std::vector<std::string> result_v, result_e;
  {
    auto res = conn->Query("MATCH (v:person) RETURN v.*;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    while (res_val.hasNext()) {
      auto row = res_val.next();
      result_v.emplace_back(row.ToString());
    }
  }
  assert(result_v == delete_vertex_property_result_v);
  {
    auto res = conn->Query("MATCH (v:person)-[e:knows]->(:person) RETURN e.*;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    while (res_val.hasNext()) {
      auto row = res_val.next();
      result_e.emplace_back(row.ToString());
    }
  }
  assert(result_e == delete_vertex_property_result_e);
}

TEST_F(CheckpointTest, test_after_delete_vertex) {
  gs::NeugDB db;
  db.Open(DB_DIR);
  auto conn = db.Connect();

  {
    auto res = conn->Query("MATCH (v:person) WHERE v.id = 1 DELETE v;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
  }

  gs::PropertyGraph& frag = db.graph();
  frag.Dump();

  std::vector<std::string> result_v, result_e;
  {
    auto res = conn->Query("MATCH (v:person) RETURN v.*;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    while (res_val.hasNext()) {
      auto row = res_val.next();
      result_v.emplace_back(row.ToString());
    }
    assert(result_v == delete_vertex_result_v);
  }

  {
    auto res = conn->Query("MATCH (v:person)-[e:knows]->(:person) RETURN e.*;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    while (res_val.hasNext()) {
      auto row = res_val.next();
      result_e.emplace_back(row.ToString());
    }
    assert(result_e == delete_vertex_result_e);
  }
}

TEST_F(CheckpointTest, test_after_add_edge_property) {
  gs::NeugDB db;
  db.Open(DB_DIR);
  auto conn = db.Connect();

  {
    auto res = conn->Query("ALTER TABLE knows ADD registration DATE;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
  }

  gs::PropertyGraph& frag = db.graph();
  frag.Dump();

  std::vector<std::string> result_v, result_e;
  {
    auto res = conn->Query("MATCH (v:person) RETURN v.*;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    while (res_val.hasNext()) {
      auto row = res_val.next();
      result_v.emplace_back(row.ToString());
    }
    assert(result_v == add_edge_property_result_v);
  }

  {
    auto res = conn->Query("MATCH (v:person)-[e:knows]->(:person) RETURN e.*;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    while (res_val.hasNext()) {
      auto row = res_val.next();
      result_e.emplace_back(row.ToString());
    }
    assert(result_e == add_edge_property_result_e);
  }
}

TEST_F(CheckpointTest, test_after_delete_edge_property) {
  gs::NeugDB db;
  db.Open(DB_DIR);
  auto conn = db.Connect();

  {
    auto res = conn->Query("ALTER TABLE knows DROP weight");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
  }

  gs::PropertyGraph& frag = db.graph();
  frag.Dump();

  std::vector<std::string> result_v, result_e;
  {
    auto res = conn->Query("MATCH (v:person) RETURN v.*;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    while (res_val.hasNext()) {
      auto row = res_val.next();
      result_v.emplace_back(row.ToString());
    }
    assert(result_v == delete_edge_property_result_v);
  }

  {
    auto res = conn->Query("MATCH (v:person)-[e:knows]->(:person) RETURN e;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    while (res_val.hasNext()) {
      auto row = res_val.next();
      result_e.emplace_back(row.ToString());
    }
    assert(result_e == delete_edge_property_result_e);
  }
}

TEST_F(CheckpointTest, test_after_delete_edge) {
  gs::NeugDB db;
  db.Open(DB_DIR);
  auto conn = db.Connect();

  {
    auto res = conn->Query(
        "MATCH (v:person)-[e:knows]->(:person) WHERE v.id = 1 DELETE e;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
  }

  gs::PropertyGraph& frag = db.graph();
  frag.Dump();

  std::vector<std::string> result_v, result_e;
  {
    auto res = conn->Query("MATCH (v:person) RETURN v.*;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    while (res_val.hasNext()) {
      auto row = res_val.next();
      result_v.emplace_back(row.ToString());
    }
    assert(result_v == delete_edge_result_v);
  }
  {
    auto res = conn->Query("MATCH (:person)-[e:knows]->(v:person) RETURN e.*;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    while (res_val.hasNext()) {
      auto row = res_val.next();
      result_e.emplace_back(row.ToString());
    }
    assert(result_e == delete_edge_result_e);
  }
}

TEST_F(CheckpointTest, test_compact) {
  std::string db_path = "/tmp/test_compact_db";
  if (std::filesystem::exists(db_path)) {
    std::filesystem::remove_all(db_path);
  }
  std::filesystem::create_directories(db_path);
  {
    gs::NeugDB db;
    db.Open(db_path);
    auto conn = db.Connect();
    load_modern_graph(conn);
    conn->Close();

    auto compact_txn = db.GetCompactTransaction(0);
    compact_txn.Commit();

    db.Close();
  }

  gs::NeugDB db2;
  db2.Open(db_path);
  auto conn2 = db2.Connect();
  std::vector<std::string> result_v, result_e;
  {
    auto res = conn2->Query("MATCH (v:person) RETURN COUNT(v);");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    EXPECT_TRUE(res_val.hasNext());
    auto row = res_val.next();
    EXPECT_EQ(row.ToString(), "<element { object { i64: 4 } }>");
    EXPECT_FALSE(res_val.hasNext());
  }

  {
    // Delete half vertices
    auto res = conn2->Query("MATCH (v:person) WHERE v.id <= 2 DELETE v;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
  }

  auto compact_txn2 = db2.GetCompactTransaction(0);
  compact_txn2.Commit();

  {
    auto res = conn2->Query("MATCH (v:person) RETURN COUNT(v);");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    EXPECT_TRUE(res_val.hasNext());
    auto row = res_val.next();
    EXPECT_EQ(row.ToString(), "<element { object { i64: 2 } }>");
    EXPECT_FALSE(res_val.hasNext());
  }

  {
    auto res =
        conn2->Query("MATCH (v:person)-[e:knows]->(:person) RETURN count(e);");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    while (res_val.hasNext()) {
      auto row = res_val.next();
      result_e.emplace_back(row.ToString());
    }
    assert(result_e ==
           std::vector<std::string>{"<element { object { i64: 0 } }>"});
  }
  conn2->Close();
  db2.Close();
}

TEST_F(CheckpointTest, test_recover_from_checkpoint) {
  gs::NeugDB db;
  db.Open(DB_DIR);
  auto conn = db.Connect();

  {
    auto res = conn->Query("MATCH (v:person) RETURN COUNT(v);");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    EXPECT_TRUE(res_val.hasNext());
    auto row = res_val.next();
    EXPECT_EQ(row.ToString(), "<element { object { i64: 4 } }>");
    EXPECT_FALSE(res_val.hasNext());
  }
  {
    auto res = conn->Query("MATCH (v)-[e]->(a) RETURN COUNT(e);");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    EXPECT_TRUE(res_val.hasNext());
    auto row = res_val.next();
    EXPECT_EQ(row.ToString(), "<element { object { i64: 6 } }>");
    EXPECT_FALSE(res_val.hasNext());
  }

  {
    auto res = conn->Query(
        "MATCH (v:person)-[e:created]->(f:software) return v.id, e.since, "
        "f.id;");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    {
      auto res_val = res.value();
      while (res_val.hasNext()) {
        auto row = res_val.next();
        LOG(INFO) << row.ToString();
      }
    }
  }

  EXPECT_TRUE(conn->Query("MATCH (v:person) WHERE v.id = 1 DELETE v;").ok());
  EXPECT_TRUE(conn->Query("MATCH (v:person)-[e:created]->(f:software) WHERE "
                          "v.id > 4 DELETE e;")
                  .ok());
  auto res = conn->Query(
      "MATCH (v:person)-[e:created]->(f:software) return v.id, e.since, f.id;");
  EXPECT_TRUE(res.ok()) << res.status().ToString();
  {
    auto res_val = res.value();
    std::vector<std::string> result;
    while (res_val.hasNext()) {
      auto row = res_val.next();
      LOG(INFO) << row.ToString();
    }
  }
  conn->Close();
  db.Close();

  // Reopen the db, should recover from checkpoint
  gs::NeugDB db2;
  db2.Open(DB_DIR);
  auto conn2 = db2.Connect();
  {
    auto res = conn2->Query("MATCH (v:person) RETURN COUNT(v);");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    EXPECT_TRUE(res_val.hasNext());
    auto row = res_val.next();
    EXPECT_EQ(row.ToString(), "<element { object { i64: 3 } }>");
    EXPECT_FALSE(res_val.hasNext());
  }
  {
    auto res = conn2->Query("MATCH (v)-[e]->(a) RETURN COUNT(e);");
    EXPECT_TRUE(res.ok()) << res.status().ToString();
    auto res_val = res.value();
    EXPECT_TRUE(res_val.hasNext());
    auto row = res_val.next();
    EXPECT_EQ(row.ToString(), "<element { object { i64: 2 } }>");
    EXPECT_FALSE(res_val.hasNext());
  }
}

}  // namespace test

}  // namespace gs
