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
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include "column_assertions.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/main/neug_db.h"
#include "neug/storages/file_names.h"
#include "neug/storages/graph/schema.h"

class DDLTestDBFixture : public ::testing::Test {
 protected:
  std::string data_path;
  std::unique_ptr<neug::NeugDB> db;
  std::shared_ptr<neug::Connection> conn;

  void SetUp() override {
    data_path = std::string("/tmp/test_ddl_") +
                ::testing::UnitTest::GetInstance()->current_test_info()->name();
    if (std::filesystem::exists(data_path)) {
      std::filesystem::remove_all(data_path);
    }
    std::filesystem::create_directories(data_path);
    db = std::make_unique<neug::NeugDB>();
    db->Open(data_path);
    conn = db->Connect();
  }

  void TearDown() override {
    conn.reset();
    db.reset();
    if (std::filesystem::exists(data_path)) {
      std::filesystem::remove_all(data_path);
    }
  }
};

class StorageDDLConflictActionTest : public DDLTestDBFixture {};

TEST_F(StorageDDLConflictActionTest, CreateNodeTable) {
  // Without IF NOT EXISTS
  EXPECT_TRUE(conn->Query(
      "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));"));
  // Should fail: table exists
  EXPECT_FALSE(conn->Query(
      "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));"));
  // With IF NOT EXISTS
  EXPECT_TRUE(
      conn->Query("CREATE NODE TABLE IF NOT EXISTS person(id INT64, name "
                  "STRING, PRIMARY KEY(id));"));
}

TEST_F(StorageDDLConflictActionTest, CreateRelTable) {
  EXPECT_TRUE(
      conn->Query("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));"));
  // Without IF NOT EXISTS
  EXPECT_TRUE(conn->Query(
      "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);"));
  EXPECT_FALSE(conn->Query(
      "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);"));
  // With IF NOT EXISTS
  EXPECT_TRUE(
      conn->Query("CREATE REL TABLE IF NOT EXISTS knows(FROM person TO person, "
                  "weight DOUBLE);"));
}

TEST_F(StorageDDLConflictActionTest, DropNodeTable) {
  GTEST_SKIP() << "Currently not support drop table if exists";
  EXPECT_TRUE(
      conn->Query("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));"));
  // Without IF EXISTS
  EXPECT_TRUE(conn->Query("DROP TABLE person;"));
  EXPECT_FALSE(conn->Query("DROP TABLE person;"));
  // With IF EXISTS
  EXPECT_TRUE(conn->Query("DROP TABLE IF EXISTS person;"));
}

TEST_F(StorageDDLConflictActionTest, DropRelTable) {
  GTEST_SKIP() << "Currently not support drop table if exists";
  EXPECT_TRUE(
      conn->Query("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));"));
  EXPECT_TRUE(conn->Query(
      "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);"));
  // Without IF EXISTS
  EXPECT_TRUE(conn->Query("DROP TABLE knows;"));
  EXPECT_FALSE(conn->Query("DROP TABLE knows;"));
  // With IF EXISTS
  EXPECT_TRUE(conn->Query("DROP TABLE IF EXISTS knows;"));
}

TEST_F(StorageDDLConflictActionTest, AddNodeProperty) {
  EXPECT_TRUE(
      conn->Query("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));"));
  // Without IF NOT EXISTS
  EXPECT_TRUE(conn->Query("ALTER TABLE person ADD birthday DATE;"));
  EXPECT_FALSE(conn->Query("ALTER TABLE person ADD birthday DATE;"));
  // With IF NOT EXISTS
  EXPECT_TRUE(
      conn->Query("ALTER TABLE person ADD IF NOT EXISTS birthday DATE;"));
}

TEST_F(StorageDDLConflictActionTest, AddRelProperty) {
  EXPECT_TRUE(
      conn->Query("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));"));
  EXPECT_TRUE(conn->Query(
      "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);"));
  // Without IF NOT EXISTS
  EXPECT_TRUE(conn->Query("ALTER TABLE knows ADD since DATE;"));
  EXPECT_FALSE(conn->Query("ALTER TABLE knows ADD since DATE;"));
  // With IF NOT EXISTS
  EXPECT_TRUE(conn->Query("ALTER TABLE knows ADD IF NOT EXISTS since DATE;"));
}

TEST_F(StorageDDLConflictActionTest, DropNodeProperty) {
  EXPECT_TRUE(conn->Query(
      "CREATE NODE TABLE person(id INT64, birthday DATE, PRIMARY KEY(id));"));
  // Without IF EXISTS
  EXPECT_TRUE(conn->Query("ALTER TABLE person DROP birthday;"));
  EXPECT_FALSE(conn->Query("ALTER TABLE person DROP birthday;"));
  // With IF EXISTS
  EXPECT_TRUE(conn->Query("ALTER TABLE person DROP IF EXISTS birthday;"));
}

TEST_F(StorageDDLConflictActionTest, DropRelProperty) {
  EXPECT_TRUE(
      conn->Query("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));"));
  EXPECT_TRUE(
      conn->Query("CREATE REL TABLE knows(FROM person TO person, weight "
                  "DOUBLE, since DATE);"));
  // Without IF EXISTS
  EXPECT_TRUE(conn->Query("ALTER TABLE knows DROP since;"));
  EXPECT_FALSE(conn->Query("ALTER TABLE knows DROP since;"));
  // With IF EXISTS
  EXPECT_TRUE(conn->Query("ALTER TABLE knows DROP IF EXISTS since;"));
}

TEST_F(StorageDDLConflictActionTest, RenameNodeProperty) {
  EXPECT_TRUE(conn->Query(
      "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));"));
  EXPECT_TRUE(conn->Query("ALTER TABLE person RENAME name TO username;"));
  // Should fail if property does not exist
  EXPECT_FALSE(conn->Query("ALTER TABLE person RENAME nonexist TO foo;"));
}

TEST_F(StorageDDLConflictActionTest, RenameRelProperty) {
  EXPECT_TRUE(
      conn->Query("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));"));
  EXPECT_TRUE(conn->Query(
      "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);"));
  EXPECT_TRUE(conn->Query("ALTER TABLE knows RENAME weight TO w;"));
  EXPECT_FALSE(conn->Query("ALTER TABLE knows RENAME nonexist TO foo;"));
}

TEST_F(StorageDDLConflictActionTest, RenameNodeType) {
  GTEST_SKIP() << "Currently not support rename table";
  EXPECT_TRUE(
      conn->Query("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));"));
  EXPECT_TRUE(conn->Query("ALTER TABLE person RENAME TO people;"));
}

TEST_F(StorageDDLConflictActionTest, RenameRelType) {
  GTEST_SKIP() << "Currently not support rename table";
  EXPECT_TRUE(
      conn->Query("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));"));
  EXPECT_TRUE(conn->Query(
      "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);"));
  EXPECT_TRUE(conn->Query("ALTER TABLE knows RENAME TO acquaintances;"));
}

TEST(StorageDDLTest, CreateAndAlterTables) {
  std::string data_path = "/tmp/test_batch_loading";
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
    EXPECT_EQ(res.value().length(), 0);
  }
  {
    auto res = conn->Query(
        "MATCH (v:software)<-[e:created]-(:person) "
        "RETURN e;");
    EXPECT_TRUE(res);
    const auto& res_val = res.value();
    EXPECT_EQ(res_val.length(), 0);
  }
  {
    auto res = conn->Query("COPY person from \"" + flex_data_dir +
                           "/test_data/person_after_alter.csv\";");
    EXPECT_TRUE(res);
  }
  {
    auto res = conn->Query("MATCH (v:person) RETURN count(v);");
    EXPECT_TRUE(res);
    const auto& table = res.value();
    EXPECT_EQ(table.length(), 1);
    EXPECT_EQ(table.response().arrays_size(), 1);
    neug::test::AssertInt64Column(table.response(), 0, {4});
  }
}
