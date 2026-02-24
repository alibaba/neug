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
class RBOTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/modern_schema_v2.yaml");
  std::string statsData = getGOptResource("stats/modern_stats_v2.json");
  std::string getRBOResourcePath(std::string resource) {
    return getGOptResourcePath("rbo_test/" + resource);
  };

  std::string getRBOResource(std::string resource) {
    return getGOptResource("rbo_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(RBOTest, FILTER_PUSH_DOWN) {
  std::string query =
      "Match (n:person)-[k:knows]->(m:person) where k.weight > 10.0 AND n.id = "
      "1 AND m.name = 'mark' Return n.name;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getRBOResource("FILTER_PUSH_DOWN_physical"));
}

TEST_F(RBOTest, EV_FUSION_ALIAS) {
  std::string query =
      "Match (n:person {id: 1})-[k:knows]->(m:person) Return m.name;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getRBOResource("EV_FUSION_ALIAS_physical"));
}

TEST_F(RBOTest, EV_FUSION_LABEL) {
  std::string query =
      "Match (n:person {id: 1})-[:knows_v2]->(m:person_v2) Return m.name;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getRBOResource("EV_FUSION_LABEL_physical"));
}

TEST_F(RBOTest, EV_FUSION_LABEL_2) {
  std::string query =
      "Match (n:person {id: 1})-[:knows_v2]->(m) Return m.name;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getRBOResource("EV_FUSION_LABEL_2_physical"));
}

TEST_F(RBOTest, EV_FUSION_PREDICATE) {
  std::string query =
      "Match (n:person)-[:knows]->(m:person) where n.id = 1 AND m.name = "
      "'mark' Return m.name;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getRBOResource("EV_FUSION_PREDICATE_physical"));
}

TEST_F(RBOTest, EV_FUSION) {
  std::string query =
      "Match (n:person {id: 1})-[:knows {weight: 1.0}]->(m:person) Return "
      "m.name;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getRBOResource("EV_FUSION_physical"));
}

}  // namespace gopt
}  // namespace neug