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
#include <memory>
#include <string>
#include <vector>
#include "arrow_column_assertions.h"
#include "neug/main/neug_db.h"

class MemoryLevelPersistenceTest : public ::testing::TestWithParam<int> {
 protected:
  std::string db_dir;
  int memory_level;

  void SetUp() override {
    memory_level = GetParam();
    db_dir = "/tmp/test_memory_level_db_" + std::to_string(memory_level);
    if (std::filesystem::exists(db_dir)) {
      std::filesystem::remove_all(db_dir);
    }
    std::filesystem::create_directories(db_dir);
  }

  void TearDown() override {
    if (std::filesystem::exists(db_dir)) {
      std::filesystem::remove_all(db_dir);
    }
  }
};

INSTANTIATE_TEST_SUITE_P(AllMemoryLevels, MemoryLevelPersistenceTest,
                         ::testing::Values(0, 1, 2, 3));

TEST_P(MemoryLevelPersistenceTest, DDLAndDMLPersistence) {
  // 1. Open DB, do DDL and DML
  neug::NeugDBConfig config(db_dir);
  config.memory_level = memory_level;
  config.checkpoint_on_close = true;  // ensure data is saved on close
  neug::NeugDB db;
  ASSERT_TRUE(db.Open(config));
  auto conn = db.Connect();
  // DDL: create table
  ASSERT_TRUE(conn->Query(
      "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));"));
  // DML: insert data
  ASSERT_TRUE(conn->Query("CREATE (n:person {id: 1, name: 'Alice'});"));
  ASSERT_TRUE(conn->Query("CREATE (n:person {id: 2, name: 'Bob'});"));
  ASSERT_TRUE(conn->Query(
      "CREATE REL TABLE knows(FROM person TO person, since INT64);"));
  ASSERT_TRUE(
      conn->Query("MATCH (a:person), (b:person) WHERE a.id=1 AND b.id=2 CREATE "
                  "(a)-[:knows {since: 2021}]->(b);"));
  db.Close();

  // 2. Reopen and check data
  neug::NeugDB db2;
  ASSERT_TRUE(db2.Open(config));
  auto conn2 = db2.Connect();
  auto res =
      conn2->Query("MATCH (v:person) RETURN v.id, v.name ORDER BY v.id;");
  ASSERT_TRUE(res);
  const auto& val = res.value();
  auto table = val.table();
  ASSERT_NE(table, nullptr);
  ASSERT_EQ(table->num_rows(), 2);
  ASSERT_EQ(table->num_columns(), 2);
  neug::test::AssertInt64Column(table, 0, {1, 2});
  neug::test::AssertStringColumn(table, 1, {"Alice", "Bob"});

  auto res1 = conn2->Query(
      "MATCH (a:person)-[r:knows]->(b:person) RETURN a.id, b.id, r.since;");
  ASSERT_TRUE(res1);
  const auto& val1 = res1.value();
  auto table1 = val1.table();
  ASSERT_NE(table1, nullptr);
  ASSERT_EQ(table1->num_rows(), 1);
  ASSERT_EQ(table1->num_columns(), 3);
  neug::test::AssertInt64Column(table1, 0, {1});
  neug::test::AssertInt64Column(table1, 1, {2});
  neug::test::AssertInt64Column(table1, 2, {2021});

  // 3. Do more DML/DDL
  ASSERT_TRUE(conn2->Query("CREATE (n:person {id: 3, name: 'Carol'});"));
  ASSERT_TRUE(conn2->Query(
      "CREATE NODE TABLE city(id INT64, name STRING, PRIMARY KEY(id));"));
  ASSERT_TRUE(conn2->Query("CREATE (c:city {id: 1, name: 'Beijing'});"));
  ASSERT_TRUE(conn2->Query(
      "CREATE REL TABLE lives_in(FROM person TO city, since INT64);"));
  ASSERT_TRUE(
      conn2->Query("MATCH (p:person), (c:city) WHERE p.id=1 AND c.id=1 CREATE "
                   "(p)-[:lives_in {since: 2020}]->(c);"));
  ASSERT_TRUE(
      conn2->Query("MATCH (a:person), (b:person) WHERE a.id=2 AND b.id=3 "
                   "CREATE (a)-[:knows {since: 2022}]->(b);"));
  db2.Close();

  // 4. Reopen and check again
  neug::NeugDB db3;
  ASSERT_TRUE(db3.Open(config));
  auto conn3 = db3.Connect();
  auto res2 =
      conn3->Query("MATCH (v:person) RETURN v.id, v.name ORDER BY v.id;");
  ASSERT_TRUE(res2);
  const auto& val2 = res2.value();
  auto table2 = val2.table();
  ASSERT_NE(table2, nullptr);
  ASSERT_EQ(table2->num_rows(), 3);
  ASSERT_EQ(table2->num_columns(), 2);
  neug::test::AssertInt64Column(table2, 0, {1, 2, 3});
  neug::test::AssertStringColumn(table2, 1, {"Alice", "Bob", "Carol"});
  auto res3 = conn3->Query(
      "MATCH (p:person)-[r:lives_in]->(c:city) RETURN p.id, c.id, r.since;");
  ASSERT_TRUE(res3);
  const auto& val3 = res3.value();
  auto table3 = val3.table();
  ASSERT_NE(table3, nullptr);
  ASSERT_EQ(table3->num_rows(), 1);
  ASSERT_EQ(table3->num_columns(), 3);
  neug::test::AssertInt64Column(table3, 0, {1});
  neug::test::AssertInt64Column(table3, 1, {1});
  neug::test::AssertInt64Column(table3, 2, {2020});
  auto res4 = conn3->Query(
      "MATCH (a:person)-[r:knows]->(b:person) RETURN a.id, b.id, r.since ORDER "
      "BY a.id, b.id;");
  ASSERT_TRUE(res4);
  const auto& val4 = res4.value();
  auto table4 = val4.table();
  ASSERT_NE(table4, nullptr);
  ASSERT_EQ(table4->num_rows(), 2);
  ASSERT_EQ(table4->num_columns(), 3);
  neug::test::AssertInt64Column(table4, 0, {1, 2});
  neug::test::AssertInt64Column(table4, 1, {2, 3});
  neug::test::AssertInt64Column(table4, 2, {2021, 2022});
  db3.Close();
}
