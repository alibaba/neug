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

#include <yaml-cpp/emitter.h>
#include <memory>
#include "gopt_test.h"
#include "neug/compiler/gopt/g_alias_manager.h"
#include "neug/compiler/planner/operator/logical_projection.h"
#include "protobuf/src/google/protobuf/parse_context.h"

namespace neug {
namespace gopt {

class PathTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/modern_schema_v2.yaml");
  std::string statsData = getGOptResource("stats/modern_stats_v2.json");
  std::string getPathResourcePath(std::string resource) {
    return getGOptResourcePath("path_test/" + resource);
  };

  std::string getPathResource(std::string resource) {
    return getGOptResource("path_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(PathTest, KNOWS_1_2) {
  std::string query =
      "MATCH (a:person)-[:knows*1..2]->(b:person) WHERE a.ID = 7 RETURN "
      "b.name;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPathResource("KNOWS_1_2_physical"));
}

TEST_F(PathTest, KNOWS_1_2_FILTER) {
  std::string query =
      "MATCH (a:person)-[e:knows*1..2 (r,_ | WHERE r.weight > "
      "1.0)]->(b:person) WHERE a.name = 'Alice' AND b.name = 'Bob' RETURN e;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("KNOWS_1_2_FILTER_physical"));
}

TEST_F(PathTest, KNOWS_V2_1_2) {
  std::string query =
      "MATCH (a:person)-[:knows_v2*1..2]->(b:person) WHERE a.ID = 7 RETURN "
      "b.name;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPathResource("KNOWS_V2_1_2_physical"));
}

TEST_F(PathTest, KNOWS_3_3) {
  std::string query =
      "MATCH (a:person)-[e:knows*3..3]->(b:person) RETURN COUNT(b.name);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPathResource("KNOWS_3_3_physical"));
}

TEST_F(PathTest, TRAIL_Path) {
  std::string query =
      "MATCH (a:person)-[e:knows* TRAIL 2..3]->(b:person) RETURN "
      "COUNT(b.name);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPathResource("TRAIL_Path_physical"));
}

TEST_F(PathTest, ACYCLIC_Path) {
  std::string query =
      "MATCH (a:person)-[e:knows* ACYCLIC 2..3]->(b:person) RETURN "
      "COUNT(b.name);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPathResource("ACYCLIC_Path_physical"));
}

TEST_F(PathTest, ANY_SHORTEST_Path) {
  std::string query =
      "MATCH (p: person {id : 1}) -[k:knows* SHORTEST 1..4]-(f:PERSON {name : "
      "'lion'}) RETURN k;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("ANY_SHORTEST_Path_physical"));
}

TEST_F(PathTest, ALL_SHORTEST_Path) {
  std::string query =
      "MATCH (a:person)-[e:knows* ALL SHORTEST 1..3]->(b:person) RETURN "
      "COUNT(b.name);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("ALL_SHORTEST_Path_physical"));
}

TEST_F(PathTest, START_NODE) {
  std::string query =
      "Match (a:person)-[b:knows]-(c:person) Return START_NODE(b) as n1, "
      "END_NODE(b) as n2;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPathResource("START_NODE_physical"));
  auto schema = GResultSchema::infer(*logical, aliasManager, getCatalog());
  VerifyFactory::verifyResultByYaml(schema,
                                    getPathResource("START_NODE_result"));
}

TEST_F(PathTest, Length) {
  std::string query =
      "Match (a:person)-[b:knows*1..3]-(c:person) Return length(b) as len";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPathResource("Length_physical"));
}

TEST_F(PathTest, Nodes) {
  std::string query =
      "Match (a:person)-[b:knows*1..3]-(c:person) Return nodes(b) as n1, "
      "rels(b) as n2";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPathResource("Nodes_physical"));
}

TEST_F(PathTest, Properties) {
  std::string query =
      "Match (a:person)-[b:knows*1..3]-(c:person) Return properties(nodes(b), "
      "'name') as n1, properties(rels(b), 'weight') as n2";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPathResource("Properties_physical"));
}

TEST_F(PathTest, WSHORTEST_PATH) {
  std::string query =
      "Match (p1:person {id: 123})-[knows:KNOWS* WSHORTEST(weight) "
      "1..30]-(p2:person {id: 456}) Return knows;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("WSHORTEST_PATH_physical"));
}

TEST_F(PathTest, WSHORTEST_COST) {
  std::string query =
      "Match (p1:person {id: 123})-[knows:KNOWS* WSHORTEST( weight ) "
      "1..30]-(p2:person {id: 456}) Return knows, cost(knows);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("WSHORTEST_COST_physical"));
}

}  // namespace gopt
}  // namespace neug