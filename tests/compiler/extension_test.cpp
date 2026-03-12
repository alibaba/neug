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

#include <execinfo.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <yaml-cpp/emitter.h>
#include <string>

#include "gopt_test.h"
#include "neug/compiler/extension/extension_api.h"
#include "neug/compiler/function/neug_call_function.h"

namespace neug {
using namespace function;
namespace gopt {

class TestShowExtensionsFunction : public function::NeugCallFunction {
 public:
  TestShowExtensionsFunction()
      : NeugCallFunction("TEST_SHOW_LOADED_EXTENSIONS", {},
                         {{"name", common::LogicalTypeID::STRING},
                          {"description", common::LogicalTypeID::STRING},
                          {"path", common::LogicalTypeID::STRING}}) {}
};

struct TestShowExtensionsFunctionSet {
  static constexpr const char* name = "TEST_SHOW_LOADED_EXTENSIONS";
  static function::function_set getFunctionSet() {
    function::function_set funcSet;
    funcSet.emplace_back(std::make_unique<TestShowExtensionsFunction>());
    return funcSet;
  }
};

class ExtensionTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/tinysnb_schema.yaml");

  std::string getExtensionResourcePath(std::string resource) {
    return getGOptResourcePath("extension_test/" + resource);
  };

  std::string getExtensionResource(std::string resource) {
    return getGOptResource("extension_test/" + resource);
  };

  std::string replaceResource(const std::string& query) {
    auto testResourceVal = getGOptResourcePath("dml_test");
    std::string processedLine = query;
    std::string pattern = "DML_RESOURCE";
    size_t pos = processedLine.find(pattern);
    while (pos != std::string::npos) {
      processedLine.replace(pos, strlen(pattern.c_str()), testResourceVal);
      pos = processedLine.find(pattern, pos + 1);
    }
    return processedLine;
  }
};

TEST_F(ExtensionTest, LOAD) {
  std::string query = "load json";
  auto logical = planLogical(query);
  auto aliasManager = std::make_shared<gopt::GAliasManager>(*logical);
  auto resultYaml = GResultSchema::infer(*logical, aliasManager, getCatalog());
  auto returns = resultYaml["returns"];
  ASSERT_TRUE(returns.IsSequence() && returns.size() == 0);
  auto physical = planPhysical(*logical, aliasManager);
  ASSERT_TRUE(physical != nullptr);
}

TEST_F(ExtensionTest, SHOW_LOADED_EXTENSIONS) {
  extension::ExtensionAPI::registerFunction<TestShowExtensionsFunctionSet>(
      catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);
  std::string query = "CALL TEST_SHOW_LOADED_EXTENSIONS();";
  auto logical = planLogical(query, schemaData, "", {});
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExtensionResource("SHOW_LOADED_EXTENSIONS_physical"));
  auto resultSchema =
      GResultSchema::infer(*logical, aliasManager, getCatalog());
  VerifyFactory::verifyResultByYaml(
      resultSchema, getExtensionResource("SHOW_LOADED_EXTENSIONS_result"));
}

TEST_F(ExtensionTest, SHOW_LOADED_EXTENSIONS_RETURN) {
  extension::ExtensionAPI::registerFunction<TestShowExtensionsFunctionSet>(
      catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);
  std::string query = "CALL TEST_SHOW_LOADED_EXTENSIONS() Return name, path;";
  auto logical = planLogical(query);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(
      *physical,
      getExtensionResource("SHOW_LOADED_EXTENSIONS_RETURN_physical"));
  auto resultSchema =
      GResultSchema::infer(*logical, aliasManager, getCatalog());
  VerifyFactory::verifyResultByYaml(
      resultSchema,
      getExtensionResource("SHOW_LOADED_EXTENSIONS_RETURN_result"));
}

TEST_F(ExtensionTest, COPY_TO_CSV) {
  std::string query =
      "COPY (MATCH (u:person) RETURN u.*) TO '/workspace/person.csv' "
      "(header=true);";
  auto logical = planLogical(query, schemaData, "", {});
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  ASSERT_TRUE(physical != nullptr);
  auto resultSchema =
      GResultSchema::infer(*logical, aliasManager, getCatalog());
  auto returns = resultSchema["returns"];
  ASSERT_TRUE(returns.IsSequence() && returns.size() == 0);
}
}  // namespace gopt
}  // namespace neug