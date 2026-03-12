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
#include "column_assertions.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/main/neug_db.h"
#include "neug/storages/file_names.h"
#include "neug/storages/graph/schema.h"

TEST(StorageDMLTest, SetVertexAndEdgeProperty) {
  std::string data_path = "/tmp/test_set_property";
  // remove the directory if it exists
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }
  // create the directory
  std::filesystem::create_directories(data_path);
  neug::NeugDB db;
  db.Open(data_path);
  // Get current directory where the .cc exists
  const char* flex_data_dir_ptr = std::getenv("MODERN_GRAPH_DATA_DIR");
  if (flex_data_dir_ptr == nullptr) {
    throw std::runtime_error(
        "MODERN_GRAPH_DATA_DIR environment variable is not set");
  }
  std::string flex_data_dir = flex_data_dir_ptr;
  auto conn = db.Connect();
  EXPECT_TRUE(
      conn->Query("CREATE NODE TABLE person(id INT64, name STRING, age "
                  "INT64, PRIMARY "
                  "KEY(id));"));
  EXPECT_TRUE(conn->Query(
      "CREATE NODE TABLE software(id INT64, name STRING, lang STRING, "
      "PRIMARY "
      "KEY(id));"));
  EXPECT_TRUE(conn->Query(
      "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);"));
  EXPECT_TRUE(
      conn->Query("CREATE REL TABLE created(FROM person TO software, "
                  "weight DOUBLE, "
                  "since INT64);"));
  EXPECT_TRUE(
      conn->Query("COPY person from \"" + flex_data_dir + "/person.csv\";"));
  EXPECT_TRUE(conn->Query("COPY software from \"" + flex_data_dir +
                          "/software.csv\";"));
  EXPECT_TRUE(conn->Query("COPY knows from \"" + flex_data_dir +
                          "/person_knows_person.csv\" (from=\"person\", "
                          "to=\"person\");"));
  EXPECT_TRUE(conn->Query("COPY created from \"" + flex_data_dir +
                          "/person_created_software.csv\" (from=\"person\", "
                          "to=\"software\");"));

  // Set the vertex property to a constant value.
  {
    EXPECT_TRUE(conn->Query("ALTER TABLE person ADD score INT64;"));
    EXPECT_TRUE(conn->Query("MATCH (v:person) SET v.score=3;"));
    auto res = conn->Query("MATCH (v:person) WHERE v.id=1 RETURN v.score;");
    EXPECT_TRUE(res);
    const auto& res_val = res.value();
    neug::test::AssertInt64Column(res_val.response(), 0, {3});
  }

  // Set the vertex property by expression
  {
    EXPECT_TRUE(conn->Query("MATCH (v:person) SET v.score=v.id+4;"));
    {
      auto res = conn->Query("MATCH (v:person) WHERE v.id=1 RETURN v.score;");
      EXPECT_TRUE(res);
      const auto& res_val = res.value();
      neug::test::AssertInt64Column(res_val.response(), 0, {5});
    }
    {
      auto res = conn->Query("MATCH (v:person) WHERE v.id=2 RETURN v.score;");
      EXPECT_TRUE(res);
      const auto& res_val = res.value();
      neug::test::AssertInt64Column(res_val.response(), 0, {6});
    }
  }
  // Set the edge property to a constant value.
  {
    EXPECT_TRUE(
        conn->Query("MATCH (:person)-[e:knows]->(:person) SET e.weight=3.0;"));
    auto res = conn->Query(
        "MATCH (:person)-[e:knows]->(v:person) WHERE v.id=2 RETURN e.weight;");
    EXPECT_TRUE(res);
    const auto& res_val = res.value();
    neug::test::AssertDoubleColumn(res_val.response(), 0, {3.0});
  }

  // Set the edge property by expression
  {
    EXPECT_TRUE(
        conn->Query("MATCH (:person)-[e:knows]->(v:person) SET "
                    "e.weight=e.weight * 2.0;"));
    {
      auto res = conn->Query(
          "MATCH (:person)-[e:knows]->(v:person) WHERE v.id=2 RETURN "
          "e.weight;");
      EXPECT_TRUE(res);
      const auto& res_val = res.value();
      neug::test::AssertDoubleColumn(res_val.response(), 0, {6.0});
    }
  }
}
