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

namespace gs {
namespace gopt {

class ExtensionTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/tinysnb_schema.yaml");

  std::string getExprResourcePath(std::string resource) {
    return getGOptResourcePath("extension_test/" + resource);
  };

  std::string getExprResource(std::string resource) {
    return getGOptResource("extension_test/" + resource);
  };
};

TEST_F(ExtensionTest, LOAD) {
  std::string query = "load json";
  auto logical = planLogical(query, schemaData, "", {});
  auto aliasManager = std::make_shared<gopt::GAliasManager>(*logical);
  auto resultYaml = GResultSchema::infer(*logical, aliasManager, getCatalog());
  auto returns = resultYaml["returns"];
  ASSERT_TRUE(returns.IsSequence() && returns.size() == 0);
}
}  // namespace gopt
}  // namespace gs