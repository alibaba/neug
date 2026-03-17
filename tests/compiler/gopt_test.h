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

#pragma once

#include <sched.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>

#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <yaml-cpp/node/emit.h>
#include <yaml-cpp/node/node.h>
#include <ranges>
#include <regex>
#include <string>
#include <utility>
#include <vector>
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/gopt/g_alias_manager.h"
#include "neug/compiler/gopt/g_catalog.h"
#include "neug/compiler/gopt/g_constants.h"
#include "neug/compiler/gopt/g_node_table.h"
#include "neug/compiler/gopt/g_physical_convertor.h"
#include "neug/compiler/gopt/g_result_schema.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/main/metadata_manager.h"
#include "neug/compiler/main/metadata_registry.h"
#include "neug/compiler/optimizer/expand_getv_fusion.h"
#include "neug/compiler/optimizer/filter_push_down_pattern.h"
#include "neug/compiler/planner/gopt_planner.h"
#include "neug/compiler/planner/operator/logical_plan_util.h"
#include "neug/compiler/storage/buffer_manager/memory_manager.h"
#include "neug/compiler/transaction/transaction.h"
#include "neug/utils/service_utils.h"

namespace neug {
namespace gopt {
class Utils {
 public:
  static std::string getPhysicalJson(const ::physical::PhysicalPlan& plan) {
    google::protobuf::util::JsonPrintOptions options;
    options.add_whitespace = true;  // Enables pretty-printing

    // Protobuf version compatibility:
    // - always_print_primitive_fields was removed in v26.0 (commit 06e7caba5,
    // Feb 2024)
    // - always_print_fields_with_no_presence is the replacement with consistent
    // behavior
#if PROTOBUF_VERSION < 4026000  // Before v26.0
    options.always_print_primitive_fields =
        true;  // Print fields even if default values
#else
    options.always_print_fields_with_no_presence =
        true;  // Replacement for v26.0+
#endif

    options.preserve_proto_field_names =
        true;  // Optional: use proto field names instead of camelCase
    std::string json;
    (void) google::protobuf::util::MessageToJsonString(plan, &json, options);
    return json;
  }

  static std::string getEnvVarOrDefault(const char* varName,
                                        const std::string& defaultVal) {
    const char* val = std::getenv(varName);
    return val ? std::string(val) : defaultVal;
  }

  static std::filesystem::path getTestResourcePath(
      const std::string& relativePath) {
    auto parentPath =
        getEnvVarOrDefault("TEST_RESOURCE", "/workspaces/neug/tests/compiler");
    return std::filesystem::path(parentPath) / relativePath;
  }

  template <typename T>
  static std::string vectorToString(std::vector<T> v) {
    std::ostringstream oss;
    for (auto i :
         v | std::views::transform([](int x) { return std::to_string(x); })) {
      oss << i << ", ";
    }
    return oss.str();
  }

  static std::string readString(const std::string& input) {
    std::ifstream file(input);

    if (!file.is_open()) {
      THROW_RUNTIME_ERROR("file " + input + " is not found");
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
  }

  static void writeString(const std::string& output,
                          const std::string& content) {
    std::ofstream file(output);
    if (!file.is_open()) {
      THROW_RUNTIME_ERROR("Could not open file: " + output);
    }
    file << content;
    file.close();
  }

  static void getQueries(std::vector<std::string>& queries,
                         const std::string& ddlFile) {
    std::ifstream file(ddlFile);
    if (!file.is_open()) {
      THROW_RUNTIME_ERROR("DDL file " + ddlFile + " not found");
    }

    std::string line;
    while (std::getline(file, line)) {
      // Trim whitespace and newlines
      line.erase(0, line.find_first_not_of(" \n\r\t"));
      line.erase(line.find_last_not_of(" \n\r\t") + 1);

      if (!line.empty() && !line.starts_with("//")) {
        // Replace TEST_RESOURCE with actual env value if present
        auto testResourceVal = getEnvVarOrDefault("TEST_RESOURCE", "/home");
        std::string processedLine = line;
        size_t pos = processedLine.find("TEST_RESOURCE");
        while (pos != std::string::npos) {
          processedLine.replace(pos, strlen("TEST_RESOURCE"), testResourceVal);
          pos = processedLine.find("TEST_RESOURCE", pos + 1);
        }
        line = processedLine;
        queries.push_back(line);
      }
    }
  }

  static std::pair<std::string, std::string> splitSchemaQuery(
      const std::string& line) {
    // Split line into segments
    std::string segment;
    std::vector<std::string> segments;
    std::stringstream lineStream(line);
    while (std::getline(lineStream, segment, ' ')) {
      if (!segment.empty()) {
        segments.push_back(segment);
      }
    }
    if (segments.size() < 3) {
      THROW_RUNTIME_ERROR("Invalid schema update line: " + line);
    }

    return std::make_pair(segments[1], segments[2]);
  }

  static void updateSchema(const std::string& line,
                           main::MetadataManager* database) {
    // Split line into segments
    auto segments = splitSchemaQuery(line);
    database->updateSchema(getTestResourcePath(segments.first));
    database->updateStats(getTestResourcePath(segments.second));
  }
};

// Test fixture
class GOptTest : public ::testing::Test {
 protected:
  void SetUp() override {
    database = std::make_unique<main::MetadataManager>();
    ctx = std::make_unique<main::ClientContext>(database.get());
    main::MetadataRegistry::registerMetadata(database.get());
  }

  void TearDown() override {
    ctx.reset();
    database.reset();
  }

  void TestBody() override {
    // // This is a base test class, actual test cases should inherit from this
    // // and override TestBody() with their specific test logic
  }

 public:
  std::string getGOptResource(const std::string& resourceName) {
    auto path = getGOptResourcePath(resourceName);
    return Utils::readString(path);
  }

  catalog::Catalog* getCatalog() { return database->getCatalog(); }

  std::string getGOptResourcePath(const std::string& resourceName) {
    return Utils::getTestResourcePath("resources/" + resourceName);
  }

  std::string replaceResource(const std::string& query) {
    auto testResourceVal = getGOptResourcePath("dataset");
    std::string processedLine = query;
    std::string pattern = "DML_RESOURCE";
    size_t pos = processedLine.find(pattern);
    while (pos != std::string::npos) {
      processedLine.replace(pos, strlen(pattern.c_str()), testResourceVal);
      pos = processedLine.find(pattern, pos + 1);
    }
    return processedLine;
  }

  std::unique_ptr<planner::LogicalPlan> planLogical(
      const std::string& query, const std::string& schemaData,
      const std::string& statsData, std::vector<std::string> rules) {
    // Update schema and stats
    database->updateSchema(schemaData);
    database->updateStats(statsData);

    // Prepare the query
    auto statement = ctx->prepare(query);
    if (!statement->success) {
      THROW_RUNTIME_ERROR("Failed to prepare query: " + statement->errMsg);
    }
    return std::move(statement->logicalPlan);
  }

  std::unique_ptr<planner::LogicalPlan> planLogical(const std::string& query) {
    // Prepare the query
    auto statement = ctx->prepare(query);
    if (!statement->success) {
      THROW_RUNTIME_ERROR("Failed to prepare query: " + statement->errMsg);
    }
    return std::move(statement->logicalPlan);
  }

  std::unique_ptr<::physical::PhysicalPlan> planPhysical(
      const planner::LogicalPlan& plan,
      std::shared_ptr<gopt::GAliasManager> aliasManager) {
    gopt::GPhysicalConvertor converter(aliasManager, database->getCatalog());
    auto physicalPlan = converter.convert(plan);
    return physicalPlan;
  }

  std::unique_ptr<::physical::PhysicalPlan> planPhysical(
      const planner::LogicalPlan& plan) {
    // Convert to physical plan
    auto aliasManager = std::make_shared<gopt::GAliasManager>(plan);
    gopt::GPhysicalConvertor converter(aliasManager, database->getCatalog());
    auto physicalPlan = converter.convert(plan);
    return physicalPlan;
  }

  void applyRules(std::vector<std::string> rules, planner::LogicalPlan* plan) {
    for (const auto& rule : rules) {
      if (rule == "FilterPushDown") {
        optimizer::FilterPushDownPattern filterPushDown;
        filterPushDown.rewrite(plan);
      } else if (rule == "ExpandGetVFusion") {
        optimizer::ExpandGetVFusion evFusion(database->getCatalog());
        evFusion.rewrite(plan);
      } else {
        FAIL() << "Unknown optimization rule: " << rule;
      }
    }
  }

  void generateQuery(const std::string& fixtureName,
                     const std::string& testName, const std::string& query,
                     const std::string& schema, const std::string& stats,
                     std::vector<std::string>& rules) {
    auto logicalPlan = planLogical(query, schema, stats, rules);
    auto physicalPlan = planPhysical(*logicalPlan);
    auto physicalJson = Utils::getPhysicalJson(*physicalPlan);
    Utils::writeString(
        getGOptResourcePath(fixtureName + "/" + testName + "_physical"),
        physicalJson);
  }

  // generate physical plan
  void generateQueries(const std::string& fixtureName,
                       const std::string& schema, const std::string& stats,
                       std::vector<std::string>& rules) {
    std::string queriesContent = getGOptResource(fixtureName + "/queries");
    std::istringstream iss(queriesContent);
    std::string line;
    std::cout << "start to generate queries" << std::endl
              << queriesContent << std::endl;
    while (std::getline(iss, line)) {
      if (line.empty() || line.starts_with("//")) {
        continue;  // Skip empty lines and comments
      }
      size_t colonPos = line.find('#');
      if (colonPos != std::string::npos) {
        std::string query = line.substr(0, colonPos);
        std::string testName = line.substr(colonPos + 1);
        std::cout << "testName:" + testName << std::endl;
        std::cout << "query:" + query << std::endl;
        // Trim whitespace
        testName.erase(0, testName.find_first_not_of(" \t"));
        testName.erase(testName.find_last_not_of(" \t") + 1);
        query.erase(0, query.find_first_not_of(" \t"));
        query.erase(query.find_last_not_of(" \t") + 1);

        generateQuery(fixtureName, testName, query, schema, stats, rules);
      }
    }
  }

  void generateResult(const std::string& fixtureName,
                      const std::string& testName, const std::string& query,
                      const std::string& schema, const std::string& stats,
                      std::vector<std::string>& rules) {
    auto logicalPlan = planLogical(query, schema, stats, rules);
    auto aliasManager = std::make_shared<GAliasManager>(*logicalPlan);
    auto resultYaml =
        GResultSchema::infer(*logicalPlan, aliasManager, getCatalog());
    Utils::writeString(
        getGOptResourcePath(fixtureName + "/" + testName + "_result"),
        YAML::Dump(resultYaml));
  }

  // generate result schema
  void generateResults(const std::string& fixtureName,
                       const std::string& schema, const std::string& stats,
                       std::vector<std::string>& rules) {
    std::string queriesContent = getGOptResource(fixtureName + "/queries");
    std::istringstream iss(queriesContent);
    std::string line;
    std::cout << "start to generate queries" << std::endl
              << queriesContent << std::endl;
    while (std::getline(iss, line)) {
      if (line.empty() || line.starts_with("//")) {
        continue;  // Skip empty lines and comments
      }
      size_t colonPos = line.find('#');
      if (colonPos != std::string::npos) {
        std::string query = line.substr(0, colonPos);
        std::string testName = line.substr(colonPos + 1);
        std::cout << "testName:" + testName << std::endl;
        std::cout << "query:" + query << std::endl;
        // Trim whitespace
        testName.erase(0, testName.find_first_not_of(" \t"));
        testName.erase(testName.find_last_not_of(" \t") + 1);
        query.erase(0, query.find_first_not_of(" \t"));
        query.erase(query.find_last_not_of(" \t") + 1);

        generateResult(fixtureName, testName, query, schema, stats, rules);
      }
    }
  }

  void generateLPlan(const std::string& fixtureName,
                     const std::string& testName, const std::string& query,
                     const std::string& schema, const std::string& stats,
                     std::vector<std::string>& rules) {
    auto logicalPlan = planLogical(query, schema, stats, rules);
    Utils::writeString(
        getGOptResourcePath(fixtureName + "/" + testName + "_logical"),
        logicalPlan->toString());
  }

  // generate logical plan
  void generateLPlans(const std::string& fixtureName, const std::string& schema,
                      const std::string& stats,
                      std::vector<std::string>& rules) {
    std::string queriesContent = getGOptResource(fixtureName + "/queries");
    std::istringstream iss(queriesContent);
    std::string line;
    std::cout << "start to generate queries" << std::endl
              << queriesContent << std::endl;
    while (std::getline(iss, line)) {
      if (line.empty() || line.starts_with("//")) {
        continue;  // Skip empty lines and comments
      }
      size_t colonPos = line.find('#');
      if (colonPos != std::string::npos) {
        std::string query = line.substr(0, colonPos);
        std::string testName = line.substr(colonPos + 1);
        std::cout << "testName:" + testName << std::endl;
        std::cout << "query:" + query << std::endl;
        // Trim whitespace
        testName.erase(0, testName.find_first_not_of(" \t"));
        testName.erase(testName.find_last_not_of(" \t") + 1);
        query.erase(0, query.find_first_not_of(" \t"));
        query.erase(query.find_last_not_of(" \t") + 1);

        generateLPlan(fixtureName, testName, query, schema, stats, rules);
      }
    }
  }

 protected:
  std::unique_ptr<main::MetadataManager> database;
  std::unique_ptr<main::ClientContext> ctx;
};

// (regex_pattern, replacement) pairs applied in order during normalize
using RegexReplaceMap = std::vector<std::pair<std::string, std::string>>;

class VerifyFactory {
 public:
  static RegexReplaceMap defaultNormalizePatterns() {
    return {{R"("max_length":\s*\d+)", "\"max_length\": <IGNORED>"},
            {R"(max_length:\s*\d+)", "max_length: <IGNORED>"}};
  }

  static std::string normalize(const std::string& s) {
    return normalize(s, defaultNormalizePatterns());
  }

  static std::string normalize(const std::string& s,
                               const RegexReplaceMap& patterns) {
    std::string out = s;
    for (const auto& p : patterns) {
      out = std::regex_replace(out, std::regex(p.first), p.second);
    }
    return out;
  }

  static void verifyPhysicalByJson(const ::physical::PhysicalPlan& plan,
                                   const std::string& expectedStr) {
    rapidjson::Document document;
    document.Parse(expectedStr.c_str());
    if (document.HasParseError()) {
      THROW_RUNTIME_ERROR("Failed to parse expected JSON: " + expectedStr);
    }
    std::string planExpectedStr = expectedStr;
    if (document.HasMember("query_plan")) {
      const rapidjson::Value& queryPlan = document["query_plan"];
      if (queryPlan.IsObject() && queryPlan.HasMember("plan")) {
        const rapidjson::Value& plan = queryPlan["plan"];
        planExpectedStr = rapidjson_stringify(plan);
      }
    } else if (document.HasMember("plan")) {
      const rapidjson::Value& plan = document["plan"];
      planExpectedStr = rapidjson_stringify(plan);
    }
    auto actualStr = Utils::getPhysicalJson(plan);
    document.Parse(actualStr.c_str());
    if (document.HasParseError()) {
      THROW_RUNTIME_ERROR("Failed to parse actual JSON: " + actualStr);
    }
    if (document.HasMember("plan")) {
      const rapidjson::Value& plan = document["plan"];
      actualStr = rapidjson_stringify(plan);
    }
    ASSERT_EQ(normalize(actualStr), normalize(planExpectedStr))
        << "Expected: " << planExpectedStr << "\nActual: " << actualStr;
  }

  static void verifyLogicalByStr(const planner::LogicalPlan& plan,
                                 const std::string& expectedStr) {
    auto actualStr = plan.toString();
    ASSERT_EQ(actualStr, expectedStr)
        << "Expected: " << expectedStr << "\nActual: " << actualStr;
  }

  static void verifyLogicalByEncodeStr(planner::LogicalPlan& plan,
                                       const std::string& expectedStr) {
    auto actualStr = planner::LogicalPlanUtil::encodeJoin(plan);
    ASSERT_EQ(actualStr, expectedStr)
        << "Expected: " << expectedStr << "\nActual: " << actualStr;
  }

  static void verifyResultByYaml(const YAML::Node& resultYaml,
                                 const std::string& expectedStr) {
    auto actualStr = YAML::Dump(resultYaml);
    ASSERT_EQ(normalize(actualStr), normalize(expectedStr))
        << "Expected: " << expectedStr << "\nActual: " << actualStr;
  }
};

}  // namespace gopt
}  // namespace neug
