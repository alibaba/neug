/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>
#include "gopt_test.h"
#include "neug/compiler/gopt/g_physical_analyzer.h"

namespace gs {
namespace gopt {

class FlagTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/modern_schema_v2.yaml");
  std::string statsData = "";

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

// Test 1: MATCH with specific nodes CREATE relationship
TEST_F(FlagTest, MatchCreateRelationship1) {
  std::string query =
      "MATCH (p1:person {id:1}), (p2:person {id:2}) CREATE "
      "(p1)-[:knows]->(p2);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  GPhysicalAnalyzer analyzer;
  auto flag = analyzer.analyze(*logical);
  EXPECT_FALSE(flag.read);
  EXPECT_TRUE(flag.insert);
  EXPECT_FALSE(flag.update);
  EXPECT_FALSE(flag.schema);
  EXPECT_FALSE(flag.batch);
  EXPECT_FALSE(flag.create_temp_table);
  EXPECT_FALSE(flag.transaction);
  EXPECT_FALSE(flag.procedure_call);
}

// Test 2: MATCH all nodes CREATE relationship
TEST_F(FlagTest, MatchCreateRelationship2) {
  std::string query =
      "MATCH (p1:person), (p2:person) CREATE (p1)-[:knows]->(p2);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  GPhysicalAnalyzer analyzer;
  auto flag = analyzer.analyze(*logical);
  EXPECT_TRUE(flag.read);
  EXPECT_TRUE(flag.update);
  EXPECT_FALSE(flag.insert);
  EXPECT_FALSE(flag.schema);
  EXPECT_FALSE(flag.batch);
  EXPECT_FALSE(flag.create_temp_table);
  EXPECT_FALSE(flag.transaction);
  EXPECT_FALSE(flag.procedure_call);
}

TEST_F(FlagTest, MatchCreateRelationship5) {
  std::string query =
      "MATCH (p1:person {id:1}), (p2:person {id:2}), (p3:person {id:3}) CREATE "
      "(p1)-[:knows]->(p2);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  GPhysicalAnalyzer analyzer;
  auto flag = analyzer.analyze(*logical);
  EXPECT_TRUE(flag.read);
  EXPECT_TRUE(flag.insert);
  EXPECT_FALSE(flag.update);
  EXPECT_FALSE(flag.schema);
  EXPECT_FALSE(flag.batch);
  EXPECT_FALSE(flag.create_temp_table);
  EXPECT_FALSE(flag.transaction);
  EXPECT_FALSE(flag.procedure_call);
}

// Test 3: CREATE nodes and relationship directly
TEST_F(FlagTest, CreateNodesAndRelationship) {
  std::string query =
      "CREATE (p1:person {id:1})-[:knows]->(p2:person "
      "{id:2});";
  auto logical = planLogical(query, schemaData, statsData, rules);
  GPhysicalAnalyzer analyzer;
  auto flag = analyzer.analyze(*logical);
  EXPECT_TRUE(flag.insert);
  EXPECT_FALSE(flag.read);
  EXPECT_FALSE(flag.update);
  EXPECT_FALSE(flag.schema);
  EXPECT_FALSE(flag.batch);
  EXPECT_FALSE(flag.create_temp_table);
  EXPECT_FALSE(flag.transaction);
  EXPECT_FALSE(flag.procedure_call);
}

// Test 4: MATCH one node CREATE relationship to matched node
TEST_F(FlagTest, MatchCreateRelationship3) {
  std::string query =
      "Match (p2:person {id:2}) CREATE (p1:person {id:1})-[:knows]->(p2);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  GPhysicalAnalyzer analyzer;
  auto flag = analyzer.analyze(*logical);
  EXPECT_TRUE(flag.insert);
  EXPECT_FALSE(flag.read);
  EXPECT_FALSE(flag.update);
  EXPECT_FALSE(flag.schema);
  EXPECT_FALSE(flag.batch);
  EXPECT_FALSE(flag.create_temp_table);
  EXPECT_FALSE(flag.transaction);
  EXPECT_FALSE(flag.procedure_call);
}

// Test 5: MATCH with property filter CREATE relationship
TEST_F(FlagTest, MatchCreateRelationship4) {
  std::string query =
      "Match (p2:person {name:'abc'}) CREATE (p1:person "
      "{id:1})-[:knows]->(p2);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  GPhysicalAnalyzer analyzer;
  auto flag = analyzer.analyze(*logical);
  EXPECT_TRUE(flag.insert);
  EXPECT_TRUE(flag.read);
  EXPECT_TRUE(flag.update);
  EXPECT_FALSE(flag.schema);
  EXPECT_FALSE(flag.batch);
  EXPECT_FALSE(flag.create_temp_table);
  EXPECT_FALSE(flag.transaction);
  EXPECT_FALSE(flag.procedure_call);
}

// Test 6: CREATE single node
TEST_F(FlagTest, CreateSingleNode) {
  std::string query = "CREATE (p1:person {id:1});";
  auto logical = planLogical(query, schemaData, statsData, rules);
  GPhysicalAnalyzer analyzer;
  auto flag = analyzer.analyze(*logical);
  EXPECT_TRUE(flag.insert);
  EXPECT_FALSE(flag.read);
  EXPECT_FALSE(flag.update);
  EXPECT_FALSE(flag.schema);
  EXPECT_FALSE(flag.batch);
  EXPECT_FALSE(flag.create_temp_table);
  EXPECT_FALSE(flag.transaction);
  EXPECT_FALSE(flag.procedure_call);
}

// Test 7: COPY FROM (batch operation)
TEST_F(FlagTest, CopyFrom) {
  std::string query =
      replaceResource("COPY person FROM 'DML_RESOURCE/person.csv';");
  auto logical = planLogical(query, schemaData, statsData, rules);
  GPhysicalAnalyzer analyzer;
  auto flag = analyzer.analyze(*logical);
  EXPECT_TRUE(flag.batch);
  EXPECT_FALSE(flag.read);
  EXPECT_FALSE(flag.insert);
  EXPECT_FALSE(flag.update);
  EXPECT_FALSE(flag.schema);
  EXPECT_FALSE(flag.create_temp_table);
  EXPECT_FALSE(flag.transaction);
  EXPECT_FALSE(flag.procedure_call);
}

TEST_F(FlagTest, CopyTo) {
  std::string query =
      "COPY (MATCH (u:person) RETURN u.*) TO 'person.csv' (header=true);";
  auto logical = planLogical(query, schemaData, "", rules);
  GPhysicalAnalyzer analyzer;
  auto flag = analyzer.analyze(*logical);
  EXPECT_TRUE(flag.batch);
  EXPECT_TRUE(flag.read);
  EXPECT_FALSE(flag.insert);
  EXPECT_FALSE(flag.update);
  EXPECT_FALSE(flag.schema);
  EXPECT_FALSE(flag.create_temp_table);
  EXPECT_FALSE(flag.transaction);
  EXPECT_FALSE(flag.procedure_call);
}

// Test 8: LOAD FROM (batch operation)
TEST_F(FlagTest, LoadFrom) {
  std::string query =
      replaceResource("LOAD FROM 'DML_RESOURCE/person.csv' Return *;");
  auto logical = planLogical(query);
  GPhysicalAnalyzer analyzer;
  auto flag = analyzer.analyze(*logical);
  EXPECT_TRUE(flag.batch);
  EXPECT_TRUE(flag.read);
  EXPECT_FALSE(flag.insert);
  EXPECT_FALSE(flag.update);
  EXPECT_FALSE(flag.schema);
  EXPECT_FALSE(flag.create_temp_table);
  EXPECT_FALSE(flag.transaction);
  EXPECT_FALSE(flag.procedure_call);
}

// Test 10: CHECKPOINT (transaction operation)
TEST_F(FlagTest, Checkpoint) {
  std::string query = "CHECKPOINT;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  GPhysicalAnalyzer analyzer;
  auto flag = analyzer.analyze(*logical);
  EXPECT_TRUE(flag.transaction);
  EXPECT_FALSE(flag.read);
  EXPECT_FALSE(flag.insert);
  EXPECT_FALSE(flag.update);
  EXPECT_FALSE(flag.schema);
  EXPECT_FALSE(flag.batch);
  EXPECT_FALSE(flag.create_temp_table);
  EXPECT_FALSE(flag.procedure_call);
}

// Test 11: MATCH with RETURN (read operation)
TEST_F(FlagTest, MatchReturn) {
  std::string query = "Match (a:person {id:1})-[:knows]->(p1) Return count(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  GPhysicalAnalyzer analyzer;
  auto flag = analyzer.analyze(*logical);
  EXPECT_TRUE(flag.read);
  EXPECT_FALSE(flag.insert);
  EXPECT_FALSE(flag.update);
  EXPECT_FALSE(flag.schema);
  EXPECT_FALSE(flag.batch);
  EXPECT_FALSE(flag.create_temp_table);
  EXPECT_FALSE(flag.transaction);
  EXPECT_FALSE(flag.procedure_call);
}

// Test 12: LOAD JSON (procedure_call or batch)
TEST_F(FlagTest, LoadJson) {
  std::string query = "LOAD JSON;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  GPhysicalAnalyzer analyzer;
  auto flag = analyzer.analyze(*logical);
  EXPECT_TRUE(flag.procedure_call);
  EXPECT_FALSE(flag.batch);
  EXPECT_FALSE(flag.read);
  EXPECT_FALSE(flag.insert);
  EXPECT_FALSE(flag.update);
  EXPECT_FALSE(flag.schema);
  EXPECT_FALSE(flag.create_temp_table);
  EXPECT_FALSE(flag.transaction);
}

// Test 13: INSTALL JSON (procedure_call)
TEST_F(FlagTest, InstallJson) {
  std::string query = "INSTALL JSON;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  GPhysicalAnalyzer analyzer;
  auto flag = analyzer.analyze(*logical);
  EXPECT_TRUE(flag.procedure_call);
  EXPECT_FALSE(flag.read);
  EXPECT_FALSE(flag.insert);
  EXPECT_FALSE(flag.update);
  EXPECT_FALSE(flag.schema);
  EXPECT_FALSE(flag.batch);
  EXPECT_FALSE(flag.create_temp_table);
  EXPECT_FALSE(flag.transaction);
}

// Test 9: CALL procedure (procedure_call)
TEST_F(FlagTest, CallProcedure) {
  std::string query = "CALL SHOW_LOADED_EXTENSIONS();";
  auto logical = planLogical(query, schemaData, statsData, rules);
  GPhysicalAnalyzer analyzer;
  auto flag = analyzer.analyze(*logical);
  EXPECT_TRUE(flag.procedure_call);
  EXPECT_FALSE(flag.read);
  EXPECT_FALSE(flag.insert);
  EXPECT_FALSE(flag.update);
  EXPECT_FALSE(flag.schema);
  EXPECT_FALSE(flag.batch);
  EXPECT_FALSE(flag.create_temp_table);
  EXPECT_FALSE(flag.transaction);
}

TEST_F(FlagTest, CreateTable) {
  std::string query =
      "CREATE NODE TABLE User (name STRING, age INT64 DEFAULT 0, reg_date "
      "INT64, PRIMARY KEY (name));";
  auto logical = planLogical(query);
  GPhysicalAnalyzer analyzer;
  auto flag = analyzer.analyze(*logical);
  EXPECT_TRUE(flag.schema);
  EXPECT_FALSE(flag.procedure_call);
  EXPECT_FALSE(flag.read);
  EXPECT_FALSE(flag.insert);
  EXPECT_FALSE(flag.update);
  EXPECT_FALSE(flag.batch);
  EXPECT_FALSE(flag.create_temp_table);
  EXPECT_FALSE(flag.transaction);
}

TEST_F(FlagTest, SetProperty) {
  std::string query =
      "MATCH (u:person) WHERE u.name = 'Adam' SET u.age = 50, u.name = 'mark' "
      "RETURN u";
  auto logical = planLogical(query, schemaData, statsData, rules);
  GPhysicalAnalyzer analyzer;
  auto flag = analyzer.analyze(*logical);
  EXPECT_TRUE(flag.read);
  EXPECT_TRUE(flag.update);
  EXPECT_FALSE(flag.procedure_call);
  EXPECT_FALSE(flag.insert);
  EXPECT_FALSE(flag.schema);
  EXPECT_FALSE(flag.batch);
  EXPECT_FALSE(flag.create_temp_table);
  EXPECT_FALSE(flag.transaction);
}
}  // namespace gopt
}  // namespace gs