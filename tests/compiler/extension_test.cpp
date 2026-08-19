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

#include <stdio.h>
#include <stdlib.h>
#ifndef _WIN32
#include <execinfo.h>
#include <unistd.h>
#endif
#include <yaml-cpp/emitter.h>
#include <string>

#include "gopt_test.h"
#include "neug/compiler/binder/binder.h"
#include "neug/compiler/extension/extension_api.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/bind_input.h"

namespace neug {
using namespace function;
namespace gopt {

class TestShowExtensionsFunction : public function::NeugCallFunction {
 public:
  TestShowExtensionsFunction()
      : NeugCallFunction(
            "TEST_SHOW_LOADED_EXTENSIONS", function::call_input_types{},
            {{"name", ::neug::DataType(::neug::DataTypeId::kVarchar)},
             {"description", ::neug::DataType(::neug::DataTypeId::kVarchar)},
             {"path", ::neug::DataType(::neug::DataTypeId::kVarchar)}}) {}
};

struct TestShowExtensionsFunctionSet {
  static constexpr const char* name = "TEST_SHOW_LOADED_EXTENSIONS";
  static function::function_set getFunctionSet() {
    function::function_set funcSet;
    funcSet.emplace_back(std::make_unique<TestShowExtensionsFunction>());
    return funcSet;
  }
};

class TestIndexScanFunction : public function::NeugCallFunction {
 public:
  TestIndexScanFunction()
      : NeugCallFunction(
            "TEST_INDEX_SCAN", {common::DataType::Varchar()},
            {{"node_id", common::DataType(common::DataTypeId::kInt64)},
             {"score", common::DataType(common::DataTypeId::kDouble)}}) {
    TableFunction::bindFunc = [](main::ClientContext*,
                                 const TableFuncBindInput* input) {
      auto columns = input->binder->createVariables(
          {"node_id", "score"},
          {common::DataType(common::DataTypeId::kInt64),
           common::DataType(common::DataTypeId::kDouble)});
      auto result = std::make_unique<IndexScanBindData>(
          std::move(columns), "test:index", input->params.at(0));
      result->options.emplace("limit", "10");
      return result;
    };
  }
};

struct TestIndexScanFunctionSet {
  static constexpr const char* name = "TEST_INDEX_SCAN";
  static function::function_set getFunctionSet() {
    function::function_set funcSet;
    funcSet.emplace_back(std::make_unique<TestIndexScanFunction>());
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

TEST_F(ExtensionTest, INDEX_SCAN_PHYSICAL_CONVERSION) {
  extension::ExtensionAPI::registerFunction<TestIndexScanFunctionSet>(
      catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);
  auto logical = planLogical("CALL TEST_INDEX_SCAN('needle');");
  auto physical = planPhysical(*logical);

  ASSERT_GE(physical->plan_size(), 1);
  const auto& op = physical->plan(0);
  ASSERT_TRUE(op.opr().has_index_scan());
  const auto& scan = op.opr().index_scan();
  EXPECT_EQ(scan.unique_index_name(), "test:index");
  ASSERT_EQ(scan.options_size(), 1);
  const auto option = scan.options().begin();
  EXPECT_EQ(option->first, "limit");
  EXPECT_EQ(option->second, "10");
  ASSERT_EQ(scan.target_value().operators_size(), 1);
  ASSERT_TRUE(scan.target_value().operators(0).has_const_());
  EXPECT_EQ(scan.target_value().operators(0).const_().str(), "needle");
  EXPECT_EQ(op.meta_data_size(), 2);
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
