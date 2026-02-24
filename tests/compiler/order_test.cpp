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

class OrderTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/modern_schema.yaml");
  std::string statsData = getGOptResource("stats/modern_stats.json");
  std::string getOrderResourcePath(std::string resource) {
    return getGOptResourcePath("order_test/" + resource);
  };

  std::string getOrderResource(std::string resource) {
    return getGOptResource("order_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(OrderTest, ORDER_BY_ID) {
  std::string query = "MATCH (p:person) RETURN p.id ORDER BY p.id";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getOrderResource("ORDER_BY_ID_physical"));
}

TEST_F(OrderTest, ORDER_BY_ID_NAME_LIMIT) {
  std::string query =
      "MATCH (a:person)-[:knows*1..2]->(b:person) Return b.name as name, a.id "
      "as id Order BY a.id DESC, b.name ASC Limit 10;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getOrderResource("ORDER_BY_ID_NAME_LIMIT_physical"));
}

TEST_F(OrderTest, RETURN_LIMIT) {
  std::string query = "MATCH (a:person) Return a limit 10;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getOrderResource("RETURN_LIMIT_physical"));
}

TEST_F(OrderTest, RETURN_SKIP) {
  // the upper bound of `skip` is set to INT_MAX
  std::string query = "MATCH (a:person) Return a skip 10;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getOrderResource("RETURN_SKIP_physical"));
}
}  // namespace gopt
}  // namespace neug