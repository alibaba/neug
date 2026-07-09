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

#include <rapidjson/document.h>
#include <memory>
#include <string>
#include "gopt_test.h"
#include "neug/compiler/common/enums/explain_type.h"
#include "neug/compiler/gopt/g_physical_convertor.h"

namespace neug {
namespace gopt {

class ExplainTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/modern_schema.yaml");
  std::string statsData = getGOptResource("stats/modern_stats.json");
  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};

  // Helper to check if explain_mode is present in JSON
  bool hasExplainModeInJson(const std::string& json) {
    rapidjson::Document doc;
    if (doc.Parse(json.c_str()).HasParseError()) {
      return false;
    }
    return doc.HasMember("explain_mode");
  }

  // Helper to extract explain_mode value from JSON
  std::string getExplainModeFromJson(const std::string& json) {
    rapidjson::Document doc;
    if (doc.Parse(json.c_str()).HasParseError()) {
      return "";
    }
    if (!doc.HasMember("explain_mode")) {
      return "";
    }
    return doc["explain_mode"].GetString();
  }

  // Helper: prepare query with schema, return prepared statement
  std::shared_ptr<main::PreparedStatement> prepareWithSchema(
      const std::string& query) {
    auto schemaResult = Schema::LoadFromYamlNode(YAML::Load(schemaData));
    if (!schemaResult) {
      THROW_RUNTIME_ERROR(schemaResult.error().ToString());
    }
    currentSchema = std::move(schemaResult).value();
    GraphStats stats;
#ifdef NEUG_BUILD_TEST
    stats.LoadFromJson(currentSchema, statsData);
#endif
    currentQueryDatabase = database->clone(&currentSchema, stats);
    auto queryContext =
        std::make_unique<main::ClientContext>(currentQueryDatabase.get());
    return queryContext->prepare(query);
  }

  // Helper: convert logical plan to physical with explicit explainMode
  std::unique_ptr<::physical::PhysicalPlan> planPhysicalWithExplainMode(
      const planner::LogicalPlan& logicalPlan,
      common::ExplainType explainMode) {
    auto aliasManager = std::make_shared<gopt::GAliasManager>(logicalPlan);
    gopt::GPhysicalConvertor converter(aliasManager, getCatalog());
    return converter.convert(logicalPlan, false, explainMode);
  }
};

// Test 1: Regular query without EXPLAIN should have explain_mode set to NONE
TEST_F(ExplainTest, RegularQueryNoExplainMode) {
  std::string query = "MATCH (n:person) RETURN n.name";
  auto statement = prepareWithSchema(query);
  ASSERT_NE(statement, nullptr);

  EXPECT_EQ(statement->getExplainMode(), common::ExplainType::NONE);
  EXPECT_FALSE(statement->isExplain());

  auto logicalPlan = std::move(statement->logicalPlan);
  auto physicalPlan = planPhysical(*logicalPlan);
  auto physicalJson = Utils::getPhysicalJson(*physicalPlan);

  EXPECT_TRUE(hasExplainModeInJson(physicalJson))
      << "explain_mode should be present for all queries";
  EXPECT_EQ(getExplainModeFromJson(physicalJson), "NONE")
      << "explain_mode should be NONE for regular queries";
}

// Test 2: EXPLAIN sets explain_mode to EXPLAIN
TEST_F(ExplainTest, ExplainPhysicalPlan) {
  std::string query = "EXPLAIN MATCH (n:person) RETURN n.name";
  auto statement = prepareWithSchema(query);
  ASSERT_NE(statement, nullptr);

  EXPECT_EQ(statement->getExplainMode(), common::ExplainType::PHYSICAL_PLAN);
  EXPECT_TRUE(statement->isExplain());

  auto logicalPlan = std::move(statement->logicalPlan);
  auto physicalPlan =
      planPhysicalWithExplainMode(*logicalPlan, statement->getExplainMode());
  auto physicalJson = Utils::getPhysicalJson(*physicalPlan);

  EXPECT_TRUE(hasExplainModeInJson(physicalJson));
  EXPECT_EQ(getExplainModeFromJson(physicalJson), "EXPLAIN");
}

// Test 3: PROFILE sets explain_mode to PROFILE
TEST_F(ExplainTest, ProfileQuery) {
  std::string query = "PROFILE MATCH (n:person) RETURN n.name";
  auto statement = prepareWithSchema(query);
  ASSERT_NE(statement, nullptr);

  EXPECT_EQ(statement->getExplainMode(), common::ExplainType::PROFILE);
  EXPECT_TRUE(statement->isExplain());

  auto logicalPlan = std::move(statement->logicalPlan);
  auto physicalPlan =
      planPhysicalWithExplainMode(*logicalPlan, statement->getExplainMode());
  auto physicalJson = Utils::getPhysicalJson(*physicalPlan);

  EXPECT_TRUE(hasExplainModeInJson(physicalJson));
  EXPECT_EQ(getExplainModeFromJson(physicalJson), "PROFILE");
}

// Test 4: EXPLAIN on join query
TEST_F(ExplainTest, ExplainWithJoin) {
  std::string query =
      "EXPLAIN MATCH (n:person)-[e:knows]->(m:person) RETURN n.name, m.name";
  auto statement = prepareWithSchema(query);
  ASSERT_NE(statement, nullptr);

  EXPECT_EQ(statement->getExplainMode(), common::ExplainType::PHYSICAL_PLAN);
  EXPECT_TRUE(statement->isExplain());

  auto logicalPlan = std::move(statement->logicalPlan);
  auto physicalPlan =
      planPhysicalWithExplainMode(*logicalPlan, statement->getExplainMode());
  auto physicalJson = Utils::getPhysicalJson(*physicalPlan);

  EXPECT_TRUE(hasExplainModeInJson(physicalJson));
  EXPECT_EQ(getExplainModeFromJson(physicalJson), "EXPLAIN");
  EXPECT_FALSE(physicalJson.empty());
}

// Test 5: PROFILE on join query
TEST_F(ExplainTest, ProfileWithJoin) {
  std::string query =
      "PROFILE MATCH (n:person)-[e:knows]->(m:person) RETURN n.name, m.name";
  auto statement = prepareWithSchema(query);
  ASSERT_NE(statement, nullptr);

  EXPECT_EQ(statement->getExplainMode(), common::ExplainType::PROFILE);
  EXPECT_TRUE(statement->isExplain());

  auto logicalPlan = std::move(statement->logicalPlan);
  auto physicalPlan =
      planPhysicalWithExplainMode(*logicalPlan, statement->getExplainMode());
  auto physicalJson = Utils::getPhysicalJson(*physicalPlan);

  EXPECT_TRUE(hasExplainModeInJson(physicalJson));
  EXPECT_EQ(getExplainModeFromJson(physicalJson), "PROFILE");
}

// Test 6: EXPLAIN unwrapping - inner statement is preserved
TEST_F(ExplainTest, ExplainUnwrappingVerification) {
  std::string query = "EXPLAIN MATCH (n:person) RETURN n.name";
  auto statement = prepareWithSchema(query);
  ASSERT_NE(statement, nullptr);

  EXPECT_EQ(statement->getExplainMode(), common::ExplainType::PHYSICAL_PLAN);

  EXPECT_NE(statement->logicalPlan, nullptr)
      << "Inner logical plan should be preserved after EXPLAIN unwrapping";

  EXPECT_EQ(statement->getStatementType(), common::StatementType::QUERY)
      << "Unwrapped statement type should be QUERY (MATCH), not EXPLAIN";
}

// Test 7: Verify all four modes
TEST_F(ExplainTest, AllExplainModes) {
  std::vector<std::pair<std::string, common::ExplainType>> cases = {
      {"MATCH (n:person) RETURN n.name", common::ExplainType::NONE},
      {"EXPLAIN MATCH (n:person) RETURN n.name",
       common::ExplainType::PHYSICAL_PLAN},
      {"PROFILE MATCH (n:person) RETURN n.name", common::ExplainType::PROFILE},
  };

  for (const auto& [query, expectedMode] : cases) {
    auto statement = prepareWithSchema(query);
    ASSERT_NE(statement, nullptr) << "Failed to prepare: " << query;
    EXPECT_EQ(statement->getExplainMode(), expectedMode)
        << "Wrong explainMode for: " << query;
    EXPECT_NE(statement->logicalPlan, nullptr)
        << "Logical plan should always be available for: " << query;
  }
}

}  // namespace gopt
}  // namespace neug
