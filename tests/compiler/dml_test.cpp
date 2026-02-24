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
#include <yaml-cpp/node/emit.h>
#include "gopt_test.h"
namespace neug {
namespace gopt {

class DMLTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/modern_schema_v2.yaml");
  std::string statsData = getGOptResource("stats/modern_stats_v2.json");
  std::string getDMLResource(std::string resource) {
    return getGOptResource("dml_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(DMLTest, COPY_PERSON) {
  std::string query = replaceResource(
      "COPY person from 'DML_RESOURCE/person.csv' (DELIM='|');");
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(
      *physical, replaceResource(getDMLResource("COPY_PERSON_physical")));
  auto resultYaml = GResultSchema::infer(*logical, aliasManager, getCatalog());
  auto returns = resultYaml["returns"];
  ASSERT_TRUE(returns.IsSequence() && returns.size() == 0);
}

TEST_F(DMLTest, COPY_KNOWS) {
  std::string query =
      replaceResource("COPY knows from 'DML_RESOURCE/knows.csv';");
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(
      *physical, replaceResource(getDMLResource("COPY_KNOWS_physical")));
  auto resultYaml = GResultSchema::infer(*logical, aliasManager, getCatalog());
  auto returns = resultYaml["returns"];
  ASSERT_TRUE(returns.IsSequence() && returns.size() == 0);
}

TEST_F(DMLTest, EXPORT_PERSON) {
  std::string query = replaceResource(
      "COPY (MATCH (u:person) RETURN u.*) TO '/workspace/person.csv' "
      "(header=true);");
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  ASSERT_TRUE(physical != nullptr);
  auto resultYaml = GResultSchema::infer(*logical, aliasManager, getCatalog());
  auto returns = resultYaml["returns"];
  ASSERT_TRUE(returns.IsSequence() && returns.size() == 0);
}

TEST_F(DMLTest, EXPORT_KNOWS) {
  std::string query = replaceResource(
      "COPY (MATCH (a:person)-[f:knows]->(b:person) RETURN a.name, f.weight, "
      "b.name) TO 'knows.csv' (header=false, delim='|');");
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  ASSERT_TRUE(physical != nullptr);
  auto resultYaml = GResultSchema::infer(*logical, aliasManager, getCatalog());
  auto returns = resultYaml["returns"];
  ASSERT_TRUE(returns.IsSequence() && returns.size() == 0);
}

TEST_F(DMLTest, CREATE_VERTEX) {
  std::string query = "CREATE(p1 : person{id : 1}), (p2 : person{id : 2})";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getDMLResource("CREATE_VERTEX_physical"));
}

TEST_F(DMLTest, CREATE_VERTEX_CAST_TYPE) {
  std::string query =
      "CREATE (t:User {prop_int32: 1, prop_int64: 1234567890123, "
      "prop_uint32: 123, prop_uint64: 456, prop_float: 1.23, prop_double: "
      "4.56, prop_varchar: 'abc'})";
  auto logical = planLogical(
      query, getGOptResource("schema/create_follows_schema.yaml"), "", rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDMLResource("CREATE_VERTEX_CAST_TYPE_physical"));
}

TEST_F(DMLTest, CREATE_VERTEX_EDGE) {
  std::string query =
      "CREATE (p1:person {id: 3})<-[:knows {weight: 3.0}]-(p2:person {id: 4})";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDMLResource("CREATE_VERTEX_EDGE_physical"));
}

// TEST_F(DMLTest, CREATE_EDGE) {
//   std::string query =
//       "MATCH (p1:person {id: 1}), (p2:person {id: 2}) CREATE (p1)-[:knows "
//       "{weight: 3.0}]->(p2)";
//   auto logical = planLogical(query, schemaData, statsData, rules);
//   auto aliasManager = std::make_shared<GAliasManager>(*logical);
//   auto physical = planPhysical(*logical, aliasManager);
//   VerifyFactory::verifyPhysicalByJson(*physical,
//                                       getDMLResource("CREATE_EDGE_physical"));
// }

TEST_F(DMLTest, DEL_VERTEX) {
  std::string query = "MATCH (u:person) WHERE u.name = 'A' DELETE u";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getDMLResource("DEL_VERTEX_physical"));
}

TEST_F(DMLTest, DETACH_DEL_VERTEX) {
  std::string query = "MATCH (u:person) WHERE u.name = 'A' DETACH DELETE u";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDMLResource("DETACH_DEL_VERTEX_physical"));
}

TEST_F(DMLTest, DEL_EDGE) {
  std::string query =
      "MATCH (u:person)-[f]->(u1) WHERE u.name = 'Karissa' DELETE f";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getDMLResource("DEL_EDGE_physical"));
}

TEST_F(DMLTest, SET_VER_PROP) {
  std::string query =
      "MATCH (u:person) WHERE u.name = 'Adam' SET u.age = 50, u.name = 'mark' "
      "RETURN u";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getDMLResource("SET_VER_PROP_physical"));
}

TEST_F(DMLTest, SET_VER_EDGE_PROP) {
  std::string query =
      "MATCH (u0:person)-[f:knows]->() WHERE u0.name = 'Adam' SET f.weight = "
      "1999.0, u0.age = 10 RETURN f";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDMLResource("SET_VER_EDGE_PROP_physical"));
}

TEST_F(DMLTest, SET_MAX_COUNT) {
  std::string query =
      "Match (n:person)-[:knows]->(m:person)-[:knows]->(c:person) WITH n, m, "
      "count(c) as c_cnt WITH n, max(c_cnt) as max_cnt SET n.age = max_cnt";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getDMLResource("SET_MAX_COUNT_physical"));
}

}  // namespace gopt
}  // namespace neug