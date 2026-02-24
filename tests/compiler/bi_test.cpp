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

class BITest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/sf_0.1.yaml");
  std::string statsData = getGOptResource("stats/sf_0.1-statistics.json");

  std::string getBIResource(std::string resource) {
    return getGOptResource("bi_test/" + resource);
  };

  std::string getBIResourcePath(std::string resource) {
    return getGOptResourcePath("bi_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(BITest, BI_1_MINI) {
  std::string query =
      "MATCH (message:COMMENT:POST) RETURN count(message) AS messageCount, "
      "sum(message.length) / count(message) AS averageMessageLength, "
      "count(message.length) AS sumMessageLength ";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getBIResource("BI_1_MINI_physical"));
}

// TEST_F(BITest, BI_18) {
//   std::string query = getBIResource("bi_18.cypher");
//   auto logical = planLogical(query, schemaData, statsData, rules);
//   auto physical = planPhysical(*logical);
//   VerifyFactory::verifyPhysicalByJson(*physical,
//                                       getBIResource("BI_18_physical"));
// }

TEST_F(BITest, BI_4) {
  std::string query = getBIResource("bi_4.cypher");
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getBIResource("BI_4_logical"));
  auto physical = planPhysical(*logical);
  ASSERT_TRUE(physical != nullptr);
}

// TEST_F(BITest, BI_13) {
//   std::string query = getBIResource("bi_13.cypher");
//   auto logical = planLogical(query, schemaData, statsData, rules);
//   VerifyFactory::verifyLogicalByStr(*logical,
//   getBIResource("BI_13_logical")); auto physical = planPhysical(*logical);
//   ASSERT_TRUE(physical != nullptr);
// }

TEST_F(BITest, BI_17) {
  std::string query = getBIResource("bi_17.cypher");
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getBIResource("BI_17_logical"));
  auto physical = planPhysical(*logical);
  ASSERT_TRUE(physical != nullptr);
}

TEST_F(BITest, BI_3) {
  std::string query = getBIResource("bi_3.cypher");
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getBIResource("BI_3_logical"));
  auto physical = planPhysical(*logical);
  ASSERT_TRUE(physical != nullptr);
}

TEST_F(BITest, BI_7) {
  std::string query = getBIResource("bi_7.cypher");
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getBIResource("BI_7_logical"));
  auto physical = planPhysical(*logical);
  ASSERT_TRUE(physical != nullptr);
}

TEST_F(BITest, BI_15) {
  std::string query = getBIResource("bi_15_precompute.cypher");
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getBIResource("BI_15_logical"));
  auto physical = planPhysical(*logical);
  ASSERT_TRUE(physical != nullptr);
}

// TEST_F(BITest, BI_16) {
//   std::string query = getBIResource("bi_16.cypher");
//   auto logical = planLogical(query, schemaData, statsData, rules);
//   VerifyFactory::verifyLogicalByStr(*logical,
//   getBIResource("BI_16_logical")); auto physical = planPhysical(*logical);
//   ASSERT_TRUE(physical != nullptr);
// }

}  // namespace gopt
}  // namespace neug