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
#include "neug/execution/execute/plan_parser.h"
#include "neug/main/neug_db.h"
#include "neug/storages/file_names.h"
#include "neug/storages/graph/schema.h"

TEST(StorageDDLTest, CreateAndAlterTables) {
  std::string data_path = "/tmp/test_batch_loading";
  // remove the directory if it exists
  if (std::filesystem::exists(data_path)) {
    std::filesystem::remove_all(data_path);
  }
  // create the directory
  std::filesystem::create_directories(data_path);
  gs::NeugDB db;
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
  EXPECT_TRUE(conn->Query("ALTER TABLE person ADD birthday DATE;"));
  EXPECT_TRUE(
      conn->Query("ALTER TABLE person ADD IF NOT EXISTS birthday DATE;"));
  EXPECT_FALSE(
      conn->Query("ALTER TABLE person ADD name STRING;"));  // should fail
  EXPECT_TRUE(conn->Query("ALTER TABLE knows ADD registion DATE;"));
  EXPECT_FALSE(conn->Query(
      "ALTER TABLE person DROP non_existing_column;"));  // should fail
  EXPECT_TRUE(conn->Query("ALTER TABLE person DROP birthday;"));
  EXPECT_TRUE(conn->Query("ALTER TABLE person DROP IF EXISTS birthday;"));
  EXPECT_TRUE(conn->Query("ALTER TABLE person DROP age;"));
  EXPECT_TRUE(conn->Query("ALTER TABLE person RENAME name TO username;"));
  EXPECT_TRUE(
      conn->Query("MATCH (v:person)-[e:created]->(:software) "
                  "DELETE e;"));
  EXPECT_TRUE(conn->Query("MATCH (v:person) DELETE v;"));
  EXPECT_TRUE(conn->Query("DROP TABLE knows;"));
  {
    auto res = conn->Query("MATCH (v:person) RETURN v;");
    EXPECT_TRUE(res);
    auto res_val = res.value();
    EXPECT_FALSE(res_val.hasNext());
  }
  {
    auto res = conn->Query(
        "MATCH (v:software)<-[e:created]-(:person) "
        "RETURN e;");
    EXPECT_TRUE(res);
    auto res_val = res.value();
    EXPECT_FALSE(res_val.hasNext());
  }
  {
    auto res = conn->Query("COPY person from \"" + flex_data_dir +
                           "/person_after_alter.csv\";");
    EXPECT_TRUE(res);
  }
  {
    auto res = conn->Query("MATCH (v:person) RETURN count(v);");
    EXPECT_TRUE(res);
    auto res_val = res.value();
    EXPECT_TRUE(res_val.hasNext());
    auto row = res_val.next();
    EXPECT_EQ(row.ToString(), "<element { object { i64: 4 } }>");
  }
}