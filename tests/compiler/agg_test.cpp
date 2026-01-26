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

class AggTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/modern_schema.yaml");
  std::string statsData = getGOptResource("stats/modern_stats.json");
  std::string getAggResource(std::string resource) {
    return getGOptResource("agg_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(AggTest, COUNT) {
  std::string query = "Match (n:person) Return count(n)";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getAggResource("COUNT_physical"));
}

TEST_F(AggTest, COUNT_BY_AGE) {
  std::string query =
      "Match (n:person)-[k:knows]->(m:person) Return m.age, count(n)";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getAggResource("COUNT_BY_AGE_physical"));
}

TEST_F(AggTest, COUNT_BY_AGE_ID) {
  std::string query =
      "Match (n:person)-[k:knows]->(m:person) Return m.age, n.id, count(n)";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getAggResource("COUNT_BY_AGE_ID_physical"));
}

TEST_F(AggTest, COUNT_BY_AGE_ID_2) {
  std::string query =
      "Match (n:person)-[k:knows]->(m:person) Return m.age, count(n), n.id as "
      "id";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getAggResource("COUNT_BY_AGE_ID_2_physical"));
}

TEST_F(AggTest, COUNT_DISTINCT) {
  std::string query = "Match (n:person) Return count(distinct n.name)";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getAggResource("COUNT_DISTINCT_physical"));
}

TEST_F(AggTest, COUNT_STAR) {
  std::string query = "Match (n:person) Return count(*)";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getAggResource("COUNT_STAR_physical"));
}

TEST_F(AggTest, AGGREGATE_AND_ORDER_BY) {
  std::string schemaData = getGOptResource("schema/tinysnb_schema.yaml");
  std::string statsData = getGOptResource("stats/tinysnb_stats.json");
  std::string query =
      "MATCH (a:person)-[:knows]->(b:person) RETURN a.ID, a.gender, "
      "b.gender, sum(b.age) ORDER BY a.ID, a.gender, b.gender ";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getAggResource("AGGREGATE_AND_ORDER_BY_physical"));
}

}  // namespace gopt
}  // namespace neug
