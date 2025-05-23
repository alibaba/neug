#include <filesystem>
#include <fstream>
#include <sstream>

#include <gtest/gtest.h>
#include <ranges>
#include "common/enums/table_type.h"
#include "gopt/g_catalog.h"
#include "gopt/g_constants.h"
#include "gopt/g_database.h"
#include "gopt/g_node_table.h"
#include "gopt/g_rel_table.h"
#include "gopt/g_storage_manager.h"
#include "main/client_context.h"
#include "planner/operator/logical_plan.h"
#include "storage/buffer_manager/memory_manager.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace testing {

std::string getTestResourcePath(const std::string& relativePath) {
  std::filesystem::path currentPath = std::filesystem::current_path();
  while (currentPath.filename() != "build" && !currentPath.empty()) {
    currentPath = currentPath.parent_path();
  }
  if (currentPath.empty()) {
    throw std::runtime_error("Could not find build directory in parent paths");
  }
  return (currentPath.parent_path() / relativePath).string();
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

std::string getEnvVarOrDefault(const char* varName,
                               const std::string& defaultVal) {
  const char* val = std::getenv(varName);
  return val ? std::string(val) : defaultVal;
}

TEST(GOptTest, GCataLog) {
  auto& transaction = kuzu::Constants::DEFAULT_TRANSACTION;
  std::cout << "Transaction ID: " << transaction.getCommitTS() << std::endl;
  kuzu::catalog::GCatalog catalog(
      getTestResourcePath("test/gopt/resources/ldbc_schema.yaml"));
  kuzu::catalog::TableCatalogEntry* tableEntry =
      catalog.getTableCatalogEntry(&transaction, "KNOWS");
  std::cout << "Table ID: " << tableEntry->getTableID() << std::endl;
  std::cout << "Table Name: " << tableEntry->getName() << std::endl;
  std::cout << "Table Type: " << static_cast<int>(tableEntry->getType())
            << std::endl;
  kuzu::catalog::RelGroupCatalogEntry* groupEntry =
      catalog.getRelGroupEntry(&transaction, "HASCREATOR");
  for (auto relTableID : groupEntry->getRelTableIDs()) {
    std::cout << "Rel Table ID: " << relTableID << std::endl;
  }
}

TEST(GOptTest, GStorageManager) {
  std::string schemaPath = getEnvVarOrDefault(
      "schema",
      getTestResourcePath("test/gopt/resources/ldbc_relgo_schema2.yaml"));
  std::string statsPath = getEnvVarOrDefault(
      "stats",
      getTestResourcePath("test/gopt/resources/ldbc_relgo_stats.json"));
  kuzu::main::SystemConfig sysConfig;
  sysConfig.readOnly = true;
  auto database =
      std::make_unique<kuzu::main::GDatabase>(schemaPath, statsPath, sysConfig);
  auto ctx = std::make_unique<kuzu::main::ClientContext>(database.get());
  std::string queryPath = getEnvVarOrDefault(
      "query", getTestResourcePath("test/gopt/resources/queries"));
  if (std::filesystem::is_directory(queryPath)) {
    for (auto& query : std::filesystem::directory_iterator(queryPath)) {
      if (query.is_regular_file()) {
        std::string path = query.path();
        if (path.ends_with(".cypher")) {
          // std::cout << "start to plan queries" << std::endl;
          auto statement = ctx->prepare(readString(path));
          if (!statement->success) {
            std::cerr << "query error: " << statement->errMsg << std::endl;
            FAIL() << "Unexpected exception type";
          } else {
            std::ofstream outfile(path + ".out");
            outfile << statement->logicalPlan->toString() << std::endl;
          }
        }
      }
    }
  } else if (std::filesystem::is_regular_file(queryPath)) {
    std::string path = queryPath;
    // std::cout << "start to plan queries" << std::endl;
    auto statement = ctx->prepare(readString(path));
    if (!statement->success) {
      std::cerr << "query error: " << statement->errMsg << std::endl;
      FAIL() << "Unexpected exception type";
    } else {
      std::ofstream outfile(path + ".out");
      outfile << statement->logicalPlan->toString() << std::endl;
    }
  } else {
    throw std::runtime_error("invalid query path: " + queryPath);
  }
}

}  // namespace testing
}  // namespace kuzu