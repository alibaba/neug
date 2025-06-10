#include <sched.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include "third_party/nlohmann_json/json.hpp"

#include <gtest/gtest.h>
#include <ranges>
#include <string>
#include <vector>
#include "gopt/g_alias_manager.h"
#include "gopt/g_catalog.h"
#include "gopt/g_constants.h"
#include "gopt/g_database.h"
#include "gopt/g_node_table.h"
#include "gopt/g_physical_convertor.h"
#include "gopt/g_storage_manager.h"
#include "main/client_context.h"
#include "main/database.h"
#include "src/planner/gopt_planner.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/wal/wal.h"
#include "transaction/transaction.h"

namespace gs {
namespace testing {

std::string getEnvVarOrDefault(const char* varName,
                               const std::string& defaultVal) {
  const char* val = std::getenv(varName);
  return val ? std::string(val) : defaultVal;
}

std::filesystem::path getTestResourcePath(const std::string& relativePath) {
  auto parentPath = getEnvVarOrDefault("TEST_RESOURCE", "/home");
  return std::filesystem::path(parentPath) / relativePath;
}

template <typename T>
std::string vectorToString(std::vector<T> v) {
  std::ostringstream oss;
  for (auto i :
       v | std::views::transform([](int x) { return std::to_string(x); })) {
    oss << i << ", ";
  }
  return oss.str();
}

std::string readString(const std::string& input) {
  std::ifstream file(input);

  if (!file.is_open()) {
    throw std::runtime_error("file " + input + " is not found");
  }

  std::stringstream buffer;
  buffer << file.rdbuf();
  return buffer.str();
}

void getQueries(std::vector<std::string>& queries, const std::string& ddlFile) {
  std::ifstream file(ddlFile);
  if (!file.is_open()) {
    throw std::runtime_error("DDL file " + ddlFile + " not found");
  }

  std::string line;
  while (std::getline(file, line)) {
    // Trim whitespace and newlines
    line.erase(0, line.find_first_not_of(" \n\r\t"));
    line.erase(line.find_last_not_of(" \n\r\t") + 1);

    if (!line.empty() && !line.starts_with("#")) {
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

std::pair<std::string, std::string> splitSchemaQuery(const std::string& line) {
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

void updateSchema(const std::string& line, main::GDatabase* database) {
  // Split line into segments
  auto segments = splitSchemaQuery(line);
  database->updateSchema(getTestResourcePath(segments.first),
                         getTestResourcePath(segments.second));
}

TEST(GOptTest, GCataLog) {
  auto& transaction = gs::Constants::DEFAULT_TRANSACTION;
  gs::catalog::GCatalog catalog(
      getTestResourcePath("test/gopt/resources/ldbc_schema.yaml"));
  auto entry = catalog.getTableCatalogEntry(&transaction, "KNOWS");
  auto knowsEntry = entry->constPtrCast<catalog::GRelTableCatalogEntry>();
  ASSERT_EQ("KNOWS", knowsEntry->getName());
  ASSERT_EQ(8, knowsEntry->getLabelId());
  ASSERT_EQ(1, knowsEntry->getSrcTableID());
  ASSERT_EQ(1, knowsEntry->getDstTableID());
  auto groupEntry = catalog.getRelGroupEntry(&transaction, "HASCREATOR");
  ASSERT_EQ(2, groupEntry->getRelTableIDs().size());
  std::vector<
      std::tuple<common::table_id_t, common::table_id_t, common::table_id_t>>
      expected = {
          {2, 0, 1},  // COMMENT -> PERSON
          {3, 0, 1}   // POST -> PERSON
      };
  for (auto relTableID : groupEntry->getRelTableIDs()) {
    auto entry = catalog.getTableCatalogEntry(&transaction, relTableID);
    auto hasCreatorEntry =
        entry->constPtrCast<catalog::GRelTableCatalogEntry>();
    auto triplet = std::make_tuple(hasCreatorEntry->getSrcTableID(),
                                   hasCreatorEntry->getLabelId(),
                                   hasCreatorEntry->getDstTableID());
    auto it = std::find(expected.begin(), expected.end(), triplet);
    ASSERT_TRUE(it != expected.end())
        << "Unexpected triplet: {" << std::get<0>(triplet) << ", "
        << std::get<1>(triplet) << ", " << std::get<2>(triplet) << "}";
  }
}

TEST(GOptTest, GStorageManager) {
  auto schemaPath = getTestResourcePath("test/gopt/resources/ldbc_schema.yaml");
  auto statsPath =
      getTestResourcePath("test/gopt/resources/ldbc_0.1_statistics.json");
  gs::catalog::GCatalog catalog(schemaPath);
  storage::MemoryManager memoryManager;
  storage::WAL wal;
  gs::storage::GStorageManager storageManager(statsPath, catalog, memoryManager,
                                              wal);
  auto& transaction = gs::Constants::DEFAULT_TRANSACTION;
  auto entry = catalog.getTableCatalogEntry(&transaction, "KNOWS");
  auto knowsTable = storageManager.getTable(entry->getTableID());
  ASSERT_EQ(knowsTable->getNumTotalRows(&transaction), 14073);
  auto entry2 = catalog.getTableCatalogEntry(&transaction, "COMMENT");
  auto commentTable = storageManager.getTable(entry2->getTableID())
                          ->ptrCast<storage::GNodeTable>();
  ASSERT_EQ(commentTable->getStats(&transaction).getTableCard(), 151043);
  auto entry3 =
      catalog.getTableCatalogEntry(&transaction, "HASCREATOR_COMMENT_PERSON");
  auto hasCreatorTable = storageManager.getTable(entry3->getTableID());
  ASSERT_EQ(hasCreatorTable->getNumTotalRows(&transaction), 151043);
}

TEST(GOptTest, Query) {
  std::string queryFile = getTestResourcePath("test/gopt/resources/query");

  gs::main::SystemConfig sysConfig;
  sysConfig.readOnly = false;
  auto database = std::make_unique<gs::main::GDatabase>(sysConfig);
  auto ctx = std::make_unique<gs::main::ClientContext>(database.get());
  std::vector<std::string> queries;
  getQueries(queries, queryFile);
  for (auto& query : queries) {
    if (query.starts_with("SCHEMA")) {
      updateSchema(query, database.get());
      continue;
    }
    std::cout << "Executing Query:" << query << std::endl
              << std::endl
              << std::endl;
    auto statement = ctx->prepare(query);
    if (!statement->success) {
      std::cerr << "query error: " << statement->errMsg << std::endl;
      FAIL() << "Unexpected exception type";
    } else {
      auto aliasManager =
          std::make_shared<gopt::GAliasManager>(*statement->logicalPlan);
      gopt::GPhysicalConvertor converter(aliasManager, database->getCatalog());
      auto plan = converter.convert(*statement->logicalPlan);
      std::cout << "Physical Plan: " << std::endl;
      std::cout << plan->DebugString() << std::endl << std::endl;
    }
  }
}

TEST(GOptTest, GOptPlanner) {
  std::string queryFile = getTestResourcePath("test/gopt/resources/query");
  std::string schema;
  std::string stats;

  auto planner = std::make_unique<gs::GOptPlanner>("");

  std::vector<std::string> queries;
  getQueries(queries, queryFile);

  for (auto& query : queries) {
    if (query.starts_with("SCHEMA")) {
      // Extract schema and stats from SCHEMA query
      auto parts = splitSchemaQuery(query);
      schema = readString(getTestResourcePath(parts.first));
      stats = readString(getTestResourcePath(parts.second));
      continue;
    }

    std::cout << "Executing Query:" << query << std::endl
              << std::endl
              << std::endl;

    auto plan = planner->compilePlan(query, schema, stats);

    if (plan.error_code != gs::StatusCode::OK) {
      std::cerr << "Query error: " << plan.full_message << std::endl;
      FAIL() << "Unexpected error during query execution";
    } else {
      std::cout << "Physical Plan: " << std::endl;
      std::cout << plan.physical_plan.DebugString() << std::endl << std::endl;
    }
  }
}

// TEST(GOptTest, CopyEdge) {
//   std::string schemaData = readString(
//       "/workspaces/neug/src/compiler/test/gopt/resources/person_schema.yaml");
//   std::string statsData = readString(
//       "/workspaces/neug/src/compiler/test/gopt/resources/person_stats.json");
//   // std::string query =
//   //     "COPY knows from "
//   //     "'/workspaces/neug/src/compiler/test/gopt/resources/knows.csv'";
//   // std::string query = "Match (n:person)-[k:knows]->(m:person) where m.id =
//   10 Return n.name"; std::string query = "Match
//   (n:person)-[k:knows]->(m:person) Return m.id, k, n.name";
//   gs::main::SystemConfig sysConfig;
//   sysConfig.readOnly = false;
//   auto database = std::make_unique<gs::main::GDatabase>(sysConfig);
//   database->updateSchema(schemaData, statsData);
//   auto ctx = std::make_unique<gs::main::ClientContext>(database.get());
//   auto statement = ctx->prepare(query);
//   if (!statement->success) {
//     std::cerr << "Query error: " << statement->errMsg << std::endl;
//     FAIL() << "Unexpected error during query execution";
//   } else {
//     std::cout << "Logical Plan: " << std::endl
//               << statement->logicalPlan->toString() << std::endl
//               << std::endl;
//     auto aliasManager =
//         std::make_shared<gopt::GAliasManager>(*statement->logicalPlan);
//     gopt::GPhysicalConvertor converter(aliasManager, database->getCatalog());
//     auto plan = converter.convert(*statement->logicalPlan);
//     std::cout << "Physical Plan: " << std::endl;
//     std::cout << plan->DebugString() << std::endl << std::endl;
//   }
// }

}  // namespace testing
}  // namespace gs