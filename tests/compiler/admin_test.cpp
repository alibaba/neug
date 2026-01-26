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

class AdminTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/tinysnb_schema.yaml");
  std::string statsData = "";
  std::string getAdminResourcePath(std::string resource) {
    return getGOptResourcePath("admin_test/" + resource);
  };

  std::string getAdminResource(std::string resource) {
    return getGOptResource("admin_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(AdminTest, CHECKPOINT) {
  std::string query = "checkpoint";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getAdminResource("CHECKPOINT_physical"));
  auto resultYaml = GResultSchema::infer(*logical, aliasManager, getCatalog());
  auto returns = resultYaml["returns"];
  ASSERT_TRUE(returns.IsSequence() && returns.size() == 0);
}
}  // namespace gopt
}  // namespace neug