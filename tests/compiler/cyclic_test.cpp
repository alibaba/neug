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
#include <string>
#include "gopt_test.h"

namespace neug {
namespace gopt {

class CyclicTest : public GOptTest {
 public:
  std::string tinySchema = getGOptResource("schema/tinysnb_schema.yaml");
  std::string tinyStats = getGOptResource("stats/tinysnb_stats.json");
  std::string lsqbSchema = getGOptResource("schema/lsqb_schema.yaml");
  std::string lsqbStats = getGOptResource("stats/lsqb_stats.json");

  std::string getCyclicResource(std::string resource) {
    return getGOptResource("cyclic_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(CyclicTest, TRI_ALICE) {
  std::string query =
      "MATCH (a:person)-[e1:knows]->(b:person)-[e2:knows]->(c:person), "
      "(a)-[e3:knows]->(c) WHERE a.fName='Alice' RETURN b.fName, c.fName";
  auto logical = planLogical(query, tinySchema, tinyStats, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getCyclicResource("TRI_ALICE_physical"));
}

TEST_F(CyclicTest, UNI_PATH) {
  std::string query =
      "MATCH "
      "(a:person)-[:knows]->(b:person)-[:studyAt|:meets]->(c:organisation:"
      "person), (a)-[:studyAt|:meets]->(c) RETURN COUNT(*)";
  auto logical = planLogical(query, tinySchema, tinyStats, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getCyclicResource("UNI_PATH_physical"));
}

TEST_F(CyclicTest, TRI_CNT) {
  std::string query =
      "MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person), "
      "(a)-[:knows]->(c) RETURN COUNT(*)";
  auto logical = planLogical(query, tinySchema, tinyStats, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getCyclicResource("TRI_CNT_physical"));
}

TEST_F(CyclicTest, ORG_CNT) {
  std::string query =
      "MATCH (a:person)-[:knows]->(b:person)-[:studyAt]->(c:organisation), "
      "(a)-[:studyAt]->(c) RETURN COUNT(*)";
  auto logical = planLogical(query, tinySchema, tinyStats, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getCyclicResource("ORG_CNT_physical"));
}

TEST_F(CyclicTest, ID_SUM) {
  std::string query =
      "MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person), "
      "(a)-[:knows]->(c) WHERE a.ID = b.ID + c.ID  RETURN a.ID,b.ID,c.ID";
  auto logical = planLogical(query, tinySchema, tinyStats, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getCyclicResource("ID_SUM_physical"));
}

TEST_F(CyclicTest, ALI_ORG) {
  std::string query =
      "MATCH (a:person)-[:knows]->(b:person)-[:studyAt]->(c:organisation), "
      "(a)-[e:studyAt]->(c) WHERE a.fName='Alice' RETURN COUNT(*)";
  auto logical = planLogical(query, tinySchema, tinyStats, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getCyclicResource("ALI_ORG_physical"));
}

TEST_F(CyclicTest, TRI_ORD) {
  std::string query =
      "MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person), "
      "(a)-[:knows]->(c) WHERE a.ID<b.ID and b.ID<c.ID RETURN a.fName, "
      "b.fName, c.fName";
  auto logical = planLogical(query, tinySchema, tinyStats, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getCyclicResource("TRI_ORD_physical"));
}

TEST_F(CyclicTest, QUAD_CNT) {
  std::string query =
      "MATCH "
      "(a:person)-[:knows]->(b:person)-[:knows]->(c:person)-[:knows]->(d:"
      "person), (a)-[:knows]->(d) RETURN COUNT(*)";
  auto logical = planLogical(query, tinySchema, tinyStats, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getCyclicResource("QUAD_CNT_physical"));
}

TEST_F(CyclicTest, REV_CNT) {
  std::string query =
      "MATCH "
      "(a:person)<-[:knows]-(b:person)-[:knows]->(c:person)-[:studyAt]->(d:"
      "organisation), (a)-[:studyAt]->(d) RETURN COUNT(*)";
  auto logical = planLogical(query, tinySchema, tinyStats, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getCyclicResource("REV_CNT_physical"));
}

TEST_F(CyclicTest, BOB_PATH) {
  std::string query =
      "MATCH "
      "(a:person)<-[:knows]-(b:person)-[:knows]->(c:person)-[:studyAt]->(d:"
      "organisation), (a)-[:studyAt]->(d) WHERE a.fName='Bob' RETURN a.fName, "
      "b.fName, c.fName, d.name";
  auto logical = planLogical(query, tinySchema, tinyStats, rules);
  VerifyFactory::verifyLogicalByStr(*logical,
                                    getCyclicResource("BOB_PATH_logical"));
  auto physical = planPhysical(*logical);
  ASSERT_TRUE(physical != nullptr);
}

TEST_F(CyclicTest, ELIZ_CNT) {
  std::string query =
      "MATCH "
      "(a:person)<-[:knows]-(b:person)-[:knows]->(c:person)-[:studyAt]->(d:"
      "organisation), (a)-[:studyAt]->(d) WHERE b.fName='Elizabeth' RETURN "
      "COUNT(*)";
  auto logical = planLogical(query, tinySchema, tinyStats, rules);
  VerifyFactory::verifyLogicalByStr(*logical,
                                    getCyclicResource("ELIZ_CNT_logical"));
  auto physical = planPhysical(*logical);
  ASSERT_TRUE(physical != nullptr);
}

TEST_F(CyclicTest, CMT_LOOP) {
  std::string query =
      "MATCH (person1:Person)-[:Person_knows_Person]-(person2:Person), "
      "(person1)<-[:Comment_hasCreator_Person]-(comment:Comment)-[:Comment_"
      "replyOf_Post]->(Post:Post)-[:Post_hasCreator_Person]->(person2) RETURN "
      "count(*) AS count;";
  auto logical = planLogical(query, lsqbSchema, lsqbStats, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getCyclicResource("CMT_LOOP_physical"));
}
}  // namespace gopt
}  // namespace neug