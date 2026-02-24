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

#include "gopt_test.h"

namespace neug {
namespace gopt {

class JoinTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/tinysnb_schema.yaml");
  std::string statsData = getGOptResource("stats/tinysnb_stats.json");
  std::string getPathResourcePath(std::string resource) {
    return getGOptResourcePath("join_test/" + resource);
  };

  std::string getPathResource(std::string resource) {
    return getGOptResource("join_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(JoinTest, A_MATCH_B_1) {
  std::string query =
      "MATCH (a:person), (b:person) WHERE a.ID = b.ID AND a.ID = 7 RETURN "
      "a.fName, b.fName, a.grades, b.grades;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPathResource("A_MATCH_B_1_physical"));
}

TEST_F(JoinTest, A_MATCH_B_2) {
  std::string query =
      "MATCH (a:person), (b:person) WHERE a.age = b.age AND a.eyeSight = "
      "b.eyeSight AND a.lastJobDuration = b.lastJobDuration AND a.ID > 7 "
      "RETURN a.fName, b.fName;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPathResource("A_MATCH_B_2_physical"));
}

TEST_F(JoinTest, A_MATCH_B_3) {
  std::string query =
      "MATCH (a), (b) WHERE a.name = b.name AND a.length = b.length AND a.note "
      "= b.note RETURN a.description, b.description;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPathResource("A_MATCH_B_3_physical"));
}

TEST_F(JoinTest, A_MATCH_B_4) {
  std::string query =
      "MATCH (a:person), (b:person) WHERE a.courseScoresPerTerm = "
      "b.courseScoresPerTerm AND a.ID = b.ID RETURN SUM(a.ID);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPathResource("A_MATCH_B_4_physical"));
}

TEST_F(JoinTest, AB_CD_9) {
  std::string query =
      "MATCH (a:person)-[e1:knows]->(b:person) , "
      "(c:person)-[e2:knows]->(d:person) WHERE a.ID = 0 AND c.ID = 2 AND "
      "e1.date = e2.date RETURN id(e1), e1.date, id(e2), e2.date;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPathResource("AB_CD_9_physical"));
}

TEST_F(JoinTest, A_MATCH_KB_10) {
  std::string query =
      "MATCH (a:person) WHERE a.gender = 1 WITH a AS k MATCH "
      "(k)-[e:knows]->(b:person) WHERE b.gender = 2 RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("A_MATCH_KB_10_physical"));
}

TEST_F(JoinTest, A_OPTIONAL_AB_11) {
  std::string query =
      "MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) WHERE b.age > "
      "a.age RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("A_OPTIONAL_AB_11_physical"));
}

TEST_F(JoinTest, AB_OPTIONAL_BC_12) {
  std::string query =
      "MATCH (a:person)-[:knows]->(b:person) OPTIONAL MATCH "
      "(b)-[:knows]->(c:person) WHERE c.age > b.age RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("AB_OPTIONAL_BC_12_physical"));
}

TEST_F(JoinTest, AB_OPTIONAL_ABC_13) {
  std::string query =
      "MATCH (a:person)-[:knows]->(b:person) OPTIONAL MATCH "
      "(a)-[:knows]->(c:person), (b)-[:knows]->(c) RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("AB_OPTIONAL_ABC_13_physical"));
}

TEST_F(JoinTest, A_OPTIONAL_B_15) {
  std::string query =
      "MATCH (a:person) WHERE a.fName = 'Alice' OPTIONAL MATCH (b:person) "
      "WHERE b.fName = 'A' RETURN a.ID, b.ID;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("A_OPTIONAL_B_15_physical"));
}

TEST_F(JoinTest, A_OPTIONAL_AB_16) {
  std::string query =
      "MATCH (a:organisation) OPTIONAL MATCH (a)<-[:knows|:studyAt]-(b:person) "
      "RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("A_OPTIONAL_AB_16_physical"));
}

TEST_F(JoinTest, A_OPTIONAL_AB_17) {
  std::string query =
      "MATCH (a:organisation:person) OPTIONAL MATCH "
      "(a)-[:studyAt|:workAt]->(b:person:organisation) RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("A_OPTIONAL_AB_17_physical"));
}

// todo: support query starting with optional match

// TEST_F(JoinTest, OPTIONAL_A_MATCH_B_18) {
//   std::string query =
//       "OPTIONAL MATCH (a:person) WHERE a.fName = 'a' MATCH (b:person) WHERE "
//       "b.fName = 'Alice' RETURN a.ID, b.ID;";
//   auto logical = planLogical(query, schemaData, statsData, rules);
//   auto physical = planPhysical(*logical);
//   VerifyFactory::verifyPhysicalByJson(
//       *physical, getPathResource("OPTIONAL_A_MATCH_B_18_physical"));
// }

// TEST_F(JoinTest, OPTIONAL_A_19) {
//   std::string query = "OPTIONAL MATCH (a:person) WHERE a.ID<0 RETURN
//   COUNT(*);"; auto logical = planLogical(query, schemaData, statsData,
//   rules); auto physical = planPhysical(*logical);
//   VerifyFactory::verifyPhysicalByJson(
//       *physical, getPathResource("OPTIONAL_A_19_physical"));
// }

// TEST_F(JoinTest, OPTIONAL_A_20) {
//   std::string query =
//       "OPTIONAL MATCH (a:person) WHERE a.fName = 'a' RETURN a.age;";
//   auto logical = planLogical(query, schemaData, statsData, rules);
//   auto physical = planPhysical(*logical);
//   VerifyFactory::verifyPhysicalByJson(
//       *physical, getPathResource("OPTIONAL_A_20_physical"));
// }

TEST_F(JoinTest, AB_OPTIONAL_ABC_21) {
  std::string query =
      "MATCH (a:person)-[:knows]->(b:person) WHERE a.ID=0 OPTIONAL MATCH "
      "(a)-[:knows]->(c:person), (b)-[:knows]->(c) RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("AB_OPTIONAL_ABC_21_physical"));
}

TEST_F(JoinTest, AB_OPTIONAL_ABC_22) {
  std::string query =
      "MATCH (a:person)-[:knows]->(b:person) OPTIONAL MATCH "
      "(a)-[:studyAt]->(c:organisation), (b)-[:studyAt]->(c) RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("AB_OPTIONAL_ABC_22_physical"));
}

TEST_F(JoinTest, A_OPTIONAL_AB_MATCH_BC_23) {
  std::string query =
      "MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) MATCH "
      "(b)-[:knows]->(c:person) RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("A_OPTIONAL_AB_MATCH_BC_23_physical"));
}

TEST_F(JoinTest, A_OPTIONAL_AB_MATCH_BC_24) {
  std::string query =
      "MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) MATCH "
      "(b)-[:studyAt]->(c:organisation) RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("A_OPTIONAL_AB_MATCH_BC_24_physical"));
}

TEST_F(JoinTest, A_OPTIONAL_AB_MATCH_BC_25) {
  std::string query =
      "MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) WHERE "
      "b.fName='Farooq' MATCH (b)<-[:knows]-(c:person) RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("A_OPTIONAL_AB_MATCH_BC_25_physical"));
}

TEST_F(JoinTest, AB_OPTIONAL_AC_MATCH_BC_26) {
  std::string query =
      "match (a:person)-[:knows]->(b:person) optional match "
      "(a)-[:knows]->(c:person) match (b)-[:knows]->(c) return count(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("AB_OPTIONAL_AC_MATCH_BC_26_physical"));
}

TEST_F(JoinTest, AB_OPTIONAL_BC_MATCH_CDE_27) {
  std::string query =
      "MATCH (a:person)-[:knows]->(b:person) OPTIONAL MATCH "
      "(b)-[:knows]->(c:person) MATCH "
      "(c)-[:knows]->(d:person)-[:knows]->(e:person) RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("AB_OPTIONAL_BC_MATCH_CDE_27_physical"));
}

TEST_F(JoinTest, A_OPTIONAL_AB_28) {
  std::string query =
      "MATCH (a:person) WHERE a.fName='Elizabeth' OR a.fName='Hubert Blaine "
      "Wolfeschlegelsteinhausenbergerdorff' OPTIONAL MATCH "
      "(a)-[:knows]->(b:person) RETURN a.fName, b.fName;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("A_OPTIONAL_AB_28_physical"));
}

TEST_F(JoinTest, AB_OPTIONAL_BC_29) {
  std::string query =
      "MATCH (a:person)-[:knows]->(b:person) WHERE a.fName='Alice' OPTIONAL "
      "MATCH (b)-[:studyAt]->(c:organisation) RETURN c.orgCode;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("AB_OPTIONAL_BC_29_physical"));
}

TEST_F(JoinTest, A_OPTIONAL_AB_30) {
  std::string query =
      "MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) WHERE "
      "b.fName='Farooq' RETURN a.fName, b.fName;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("A_OPTIONAL_AB_30_physical"));
}

TEST_F(JoinTest, A_OPTIONAL_AB_OPTIONAL_BC_34) {
  std::string query =
      "MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) OPTIONAL MATCH "
      "(b)-[:knows]->(c:person) RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("A_OPTIONAL_AB_OPTIONAL_BC_34_physical"));
}

TEST_F(JoinTest, WHERE_SUBQUERY) {
  std::string query =
      "Match (a:person)-[:knows*2..3]->(c:person) Where NOT (a)-[:knows]->(c) "
      "Return count(a)";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("WHERE_SUBQUERY_physical"));
}

TEST_F(JoinTest, WHERE_NOT_SUBQUERY) {
  std::string query =
      "Match (a:person)-[:knows*2..3]->(c:person) Where (a)-[:knows]->(c) "
      "Return count(a)";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("WHERE_NOT_SUBQUERY_physical"));
}

TEST_F(JoinTest, FILTER_AFTER_JOIN) {
  std::string query =
      "MATCH (a:person) WITH COUNT(*) AS s MATCH (c:person) WHERE c.ID > s "
      "RETURN c.ID";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("FILTER_AFTER_JOIN_physical"));
}

}  // namespace gopt
}  // namespace neug