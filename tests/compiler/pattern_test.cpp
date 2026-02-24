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

class PatternTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/modern_schema.yaml");
  std::string statsData = getGOptResource("stats/modern_stats.json");
  std::string getPatResource(std::string resource) {
    return getGOptResource("pattern_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(PatternTest, SCAN) {
  std::string query = "MATCH (n:person) return n.name;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPatResource("SCAN_physical"));
}

TEST_F(PatternTest, SCAN_FILTER) {
  std::string query = "Match (n:person) Where n.id = 1 Return n;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPatResource("SCAN_FILTER_physical"));
}

TEST_F(PatternTest, EXPAND) {
  std::string query =
      "Match (n:person)-[f:knows]->(m:person) Return n.name, f, m.name;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPatResource("EXPAND_physical"));
}

TEST_F(PatternTest, EXPAND_FILTER) {
  std::string query =
      "Match (n:person)-[f:knows]->(m:person) Where n.id = 1 AND f.weight > "
      "10.0 Return n.name, f, m.name;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPatResource("EXPAND_FILTER_physical"));
}

TEST_F(PatternTest, GETV_FILTER) {
  std::string query =
      "Match (n:person)-[f:knows]->(m:person) Where n.id = 1 AND f.weight > "
      "10.0 AND m.name = 'cc' Return n.name, f.weight, m.name;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPatResource("GETV_FILTER_physical"));
}

TEST_F(PatternTest, EXPAND_FILTER_2) {
  std::string query =
      "Match (:person {name: \"mark\"})-[:knows {weight: 10.0}]->(m:person) "
      "Return m;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPatResource("EXPAND_FILTER_2_physical"));
}

TEST_F(PatternTest, PROJECT_AS) {
  std::string query =
      "Match (:person {name: \"mark\"})-[:knows {weight: 10.0}]->(m:person) "
      "Return m as q;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPatResource("PROJECT_AS_physical"));
}

TEST_F(PatternTest, FILTER) {
  std::string query =
      "MATCH (a:person)-[:knows]->(:person)-[:knows]->(b:person) WHERE "
      "a.age>b.age+10 RETURN a.age, b.age;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPatResource("FILTER_physical"));
}

// return empty code if schema is empty, and the query does not give a explict
// label
TEST_F(PatternTest, EMPTY_RESULT) {
  std::string emptySchema = getGOptResource("schema/empty_schema.yaml");
  std::string query = "MATCH (n) Return count(n);";
  auto logical = planLogical(query, emptySchema, "", rules);
  ASSERT_TRUE(logical->emptyResult(logical->getLastOperator()));
}

// throw exception if the query contains a explict label, even if the schema is
// empty
TEST_F(PatternTest, INVALID_LABEL) {
  try {
    std::string emptySchema = getGOptResource("schema/empty_schema.yaml");
    std::string query = "MATCH (n:person) Return count(n);";
    auto logical = planLogical(query, emptySchema, "", rules);
  } catch (const std::exception& e) {
    ASSERT_TRUE(std::string(e.what()).find("Table person does not exist") !=
                std::string::npos)
        << "Unexpected exception message: " << e.what();
    return;
  }
  FAIL() << "Expected exception to be thrown due to invalid label, but no "
            "exception was thrown.";
}

// TEST_F(PatternTest, STANDARD_UNION) {
//   std::string query =
//       "Match (n {name: 'marko'}) Return n.age UNION ALL Match (n {name: "
//       "'josh'}) "
//       "Return n.age;";
//   auto logical = planLogical(query, schemaData, statsData, rules);
//   VerifyFactory::verifyLogicalByStr(*logical,
//                                     getPatResource("STANDARD_UNION_logical"));
//   auto physical = planPhysical(*logical);
//   VerifyFactory::verifyPhysicalByJson(
//       *physical, getPatResource("STANDARD_UNION_physical"));
// }

// TEST_F(PatternTest, CALL_UNION) {
//   std::string query = getPatResource("call_union.cypher");
//   auto logical = planLogical(query, schemaData, statsData, rules);
//   VerifyFactory::verifyLogicalByStr(*logical,
//                                     getPatResource("CALL_UNION_logical"));
//   auto physical = planPhysical(*logical);
//   VerifyFactory::verifyPhysicalByJson(*physical,
//                                       getPatResource("CALL_UNION_physical"));
// }

TEST_F(PatternTest, OPTIONAL_MATCH) {
  std::string query =
      "Match (n:person {name: 'marko'}) WITH n OPTIONAL Match "
      "(n)-[:knows]->(f) Return f;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical,
                                    getPatResource("OPTIONAL_MATCH_logical"));
}

}  // namespace gopt
}  // namespace neug