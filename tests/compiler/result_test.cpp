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
#include "neug/compiler/gopt/g_result_schema.h"

namespace neug {
namespace gopt {
class ResultTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/modern_schema.yaml");
  std::string statsData = getGOptResource("stats/modern_stats.json");

  std::string getResultResource(std::string resource) {
    return getGOptResource("result_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(ResultTest, COUNT_NO_ALIAS) {
  std::string query = "Match (n:person) Return count(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto resultYaml = GResultSchema::infer(*logical, aliasManager, getCatalog());
  VerifyFactory::verifyResultByYaml(resultYaml,
                                    getResultResource("COUNT_NO_ALIAS_result"));
}

TEST_F(ResultTest, COUNT_ALIAS) {
  std::string query = "Match (n:person) Return count(*) as cnt;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto resultYaml = GResultSchema::infer(*logical, aliasManager, getCatalog());
  VerifyFactory::verifyResultByYaml(resultYaml,
                                    getResultResource("COUNT_ALIAS_result"));
}

TEST_F(ResultTest, RETURN_VERTEX) {
  std::string query = "Match (n:person) Return n as a;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto resultYaml = GResultSchema::infer(*logical, aliasManager, getCatalog());
  VerifyFactory::verifyResultByYaml(resultYaml,
                                    getResultResource("RETURN_VERTEX_result"));
}

TEST_F(ResultTest, RETURN_EDGE) {
  std::string query = "Match (n:person)-[k:knows]->(m:person) Return k;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto resultYaml = GResultSchema::infer(*logical, aliasManager, getCatalog());
  VerifyFactory::verifyResultByYaml(resultYaml,
                                    getResultResource("RETURN_EDGE_result"));
}

TEST_F(ResultTest, RETURN_MIX) {
  std::string query =
      "Match (n:person)-[k:knows]->(m:person) Return k, m.name as name, n.age "
      "+ 1 as age;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto resultYaml = GResultSchema::infer(*logical, aliasManager, getCatalog());
  VerifyFactory::verifyResultByYaml(resultYaml,
                                    getResultResource("RETURN_MIX_result"));
}

TEST_F(ResultTest, RETURN_STAR) {
  std::string query = "Match (n:person) Return n.*;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto resultYaml = GResultSchema::infer(*logical, aliasManager, getCatalog());
  VerifyFactory::verifyResultByYaml(resultYaml,
                                    getResultResource("RETURN_STAR_result"));
}

}  // namespace gopt
}  // namespace neug