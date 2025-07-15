#pragma once

#include <sched.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>

#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>
#include <yaml-cpp/node/emit.h>
#include <yaml-cpp/node/node.h>
#include <ranges>
#include <string>
#include <vector>
#include "src/include/catalog/catalog.h"
#include "src/include/gopt/g_alias_manager.h"
#include "src/include/gopt/g_catalog.h"
#include "src/include/gopt/g_constants.h"
#include "src/include/gopt/g_database.h"
#include "src/include/gopt/g_node_table.h"
#include "src/include/gopt/g_physical_convertor.h"
#include "src/include/gopt/g_result_schema.h"
#include "src/include/gopt/g_storage_manager.h"
#include "src/include/main/client_context.h"
#include "src/include/main/database.h"
#include "src/include/optimizer/expand_getv_fusion.h"
#include "src/include/optimizer/filter_push_down_pattern.h"
#include "src/include/planner/operator/logical_plan_util.h"
#include "src/include/storage/buffer_manager/memory_manager.h"
#include "src/include/storage/wal/wal.h"
#include "src/include/transaction/transaction.h"
#include "src/planner/gopt_planner.h"

namespace gs {
namespace gopt {
class Utils {
 public:
  static std::string getPhysicalJson(const ::physical::PhysicalPlan& plan) {
    google::protobuf::util::JsonPrintOptions options;
    options.add_whitespace = true;  // Enables pretty-printing
    options.always_print_primitive_fields =
        true;  // Optional: print even if field is default
    options.preserve_proto_field_names =
        true;  // Optional: use proto field names instead of camelCase
    std::string json;
    google::protobuf::util::MessageToJsonString(plan, &json, options);
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
        getEnvVarOrDefault("TEST_RESOURCE", "/workspaces/neug/src/compiler");
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
      throw std::runtime_error("file " + input + " is not found");
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
  }

  static void writeString(const std::string& output,
                          const std::string& content) {
    std::ofstream file(output);
    if (!file.is_open()) {
      throw std::runtime_error("Could not open file: " + output);
    }
    file << content;
    file.close();
  }

  static void getQueries(std::vector<std::string>& queries,
                         const std::string& ddlFile) {
    std::ifstream file(ddlFile);
    if (!file.is_open()) {
      throw std::runtime_error("DDL file " + ddlFile + " not found");
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
      throw std::runtime_error("Invalid schema update line: " + line);
    }

    return std::make_pair(segments[1], segments[2]);
  }

  static void updateSchema(const std::string& line, main::GDatabase* database) {
    // Split line into segments
    auto segments = splitSchemaQuery(line);
    database->updateSchema(getTestResourcePath(segments.first),
                           getTestResourcePath(segments.second));
  }
};

// Test fixture
class GOptTest : public ::testing::Test {
 protected:
  void SetUp() override {
    sysConfig.readOnly = false;
    database = std::make_unique<main::GDatabase>(sysConfig);
    ctx = std::make_unique<main::ClientContext>(database.get());
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
    return Utils::getTestResourcePath("test/gopt/resources/" + resourceName);
  }
  std::unique_ptr<planner::LogicalPlan> planLogical(
      const std::string& query, const std::string& schemaData,
      const std::string& statsData, std::vector<std::string> rules) {
    // Update schema and stats
    database->updateSchema(schemaData, statsData);

    // Prepare the query
    auto statement = ctx->prepare(query);
    if (!statement->success) {
      throw std::runtime_error("Failed to prepare query: " + statement->errMsg);
    }
    return std::move(statement->logicalPlan);
  }

  std::unique_ptr<::physical::PhysicalPlan> planPhysical(
      const planner::LogicalPlan& plan,
      std::shared_ptr<gopt::GAliasManager> aliasManager) {
    gopt::GPhysicalConvertor converter(aliasManager, database->getCatalog());
    auto physicalPlan = converter.convert(plan);
    return std::move(physicalPlan);
  }

  std::unique_ptr<::physical::PhysicalPlan> planPhysical(
      const planner::LogicalPlan& plan) {
    // Convert to physical plan
    auto aliasManager = std::make_shared<gopt::GAliasManager>(plan);
    gopt::GPhysicalConvertor converter(aliasManager, database->getCatalog());
    auto physicalPlan = converter.convert(plan);
    return std::move(physicalPlan);
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

 private:
  main::SystemConfig sysConfig;
  std::unique_ptr<main::GDatabase> database;
  std::unique_ptr<main::ClientContext> ctx;
};

class VerifyFactory {
 public:
  static void verifyPhysicalByJson(const ::physical::PhysicalPlan& plan,
                                   const std::string& expectedStr) {
    auto actualStr = Utils::getPhysicalJson(plan);
    ASSERT_EQ(actualStr, expectedStr)
        << "Expected: " << expectedStr << "\nActual: " << actualStr;
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
    ASSERT_EQ(actualStr, expectedStr)
        << "Expected: " << expectedStr << "\nActual: " << actualStr;
  }
};

}  // namespace gopt
}  // namespace gs
