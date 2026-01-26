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

#include <memory>
#include "gopt_test.h"
#include "neug/compiler/gopt/g_alias_manager.h"

namespace neug {
namespace gopt {
class GOptDDLTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/create_follows_schema.yaml");
  std::string statsData = getGOptResource("stats/create_follows_stats.json");
  std::string getDDLResourcePath(std::string resource) {
    return getGOptResourcePath("ddl_test/" + resource);
  };

  std::string getDDLResource(std::string resource) {
    return getGOptResource("ddl_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

// todo: support temporal data type
TEST_F(GOptDDLTest, CREATE_USER) {
  std::string query =
      "CREATE NODE TABLE User (name STRING, age INT64 DEFAULT 0, reg_date "
      "INT64, PRIMARY KEY (name));";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<gopt::GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getDDLResource("CREATE_USER_physical"));
  auto resultYaml = GResultSchema::infer(*logical, aliasManager, getCatalog());
  auto returns = resultYaml["returns"];
  ASSERT_TRUE(returns.IsSequence() && returns.size() == 0);
}

TEST_F(GOptDDLTest, CREATE_USER_DEFAULT_VALUE) {
  std::string query = R"(
  CREATE NODE TABLE User (
    i64 INT64 DEFAULT 0,
    u64 UINT64 DEFAULT 0,
    i32 INT32 DEFAULT 0,
    u32 UINT32 DEFAULT 0,
    ft FLOAT DEFAULT 0.0,
    db DOUBLE DEFAULT 0.0,
    str STRING DEFAULT '',
    date DATE DEFAULT DATE('1970-01-01'),
    datetime TIMESTAMP DEFAULT TIMESTAMP('1970-01-01 00:00:00'),
    interval INTERVAL DEFAULT INTERVAL('0 year 0 month 0 day'),
    PRIMARY KEY (str)
);
  )";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<gopt::GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("CREATE_USER_DEFAULT_VALUE_physical"));
}

TEST_F(GOptDDLTest, CREATE_USER_SYSTEM_DEFAULT_VALUE) {
  std::string query = R"(
CREATE NODE TABLE User (
    i64 INT64,
    u64 UINT64,
    i32 INT32,
    u32 UINT32,
    ft FLOAT,
    db DOUBLE,
    str STRING,
    date DATE,
    datetime TIMESTAMP,
    interval INTERVAL,
    PRIMARY KEY (str)
);
  )";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<gopt::GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("CREATE_USER_SYSTEM_DEFAULT_VALUE_physical"));
}

TEST_F(GOptDDLTest, CREATE_FOLLOWS) {
  std::string query =
      "CREATE REL TABLE Follows(FROM User TO User, since INT64);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("CREATE_FOLLOWS_physical"));
}

TEST_F(GOptDDLTest, ALTER_USER_ADD) {
  std::string query = "ALTER TABLE User ADD age INT64;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("ALTER_USER_ADD_physical"));
}

TEST_F(GOptDDLTest, ALTER_USER_RENAME_COLUMN) {
  std::string query = "ALTER TABLE User RENAME age TO grade;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("ALTER_USER_RENAME_COLUMN_physical"));
}

TEST_F(GOptDDLTest, ALTER_USER_DROP_COLUMN) {
  std::string query = "ALTER TABLE User DROP grade;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("ALTER_USER_DROP_COLUMN_physical"));
}

TEST_F(GOptDDLTest, ALTER_FOLLOWS_ADD) {
  std::string query = "ALTER TABLE Follows ADD age INT64;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("ALTER_FOLLOWS_ADD_physical"));
}

TEST_F(GOptDDLTest, ALTER_FOLLOWS_RENAME_COLUMN) {
  std::string query = "ALTER TABLE Follows RENAME age TO grade;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("ALTER_FOLLOWS_RENAME_COLUMN_physical"));
}

TEST_F(GOptDDLTest, ALTER_FOLLOWS_DROP_COLUMN) {
  std::string query = "ALTER TABLE Follows DROP grade;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("ALTER_FOLLOWS_DROP_COLUMN_physical"));
}

TEST_F(GOptDDLTest, ALTER_USER_RENAME_TABLE) {
  std::string query = "ALTER TABLE User RENAME TO Student;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("ALTER_USER_RENAME_TABLE_physical"));
}

TEST_F(GOptDDLTest, ALTER_FOLLOWS_RENAME_TABLE) {
  std::string query = "ALTER TABLE Follows RENAME TO Follows2;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("ALTER_FOLLOWS_RENAME_TABLE_physical"));
}

TEST_F(GOptDDLTest, DROP_USER) {
  std::string query = "DROP TABLE User;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getDDLResource("DROP_USER_physical"));
}

TEST_F(GOptDDLTest, DROP_FOLLOWS) {
  std::string query = "DROP TABLE Follows;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getDDLResource("DROP_FOLLOWS_physical"));
}

TEST_F(GOptDDLTest, CREATE_MULTI_EDGE) {
  std::string schemaData = getGOptResource("schema/modern_schema.yaml");
  std::string query =
      "CREATE REL TABLE Follows(FROM person TO person, FROM person TO "
      "software, since INT64);";
  auto logical = planLogical(query, schemaData, "", rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("CREATE_MULTI_EDGE_physical"));
}

TEST_F(GOptDDLTest, DROP_PERSON_IF_EXISTS) {
  std::string query = "DROP TABLE IF EXISTS User2;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  ASSERT_TRUE(logical->emptyResult(logical->getLastOperator()));
}

TEST_F(GOptDDLTest, DROP_KNOWS_IF_EXISTS) {
  std::string query = "DROP TABLE IF EXISTS Follows2;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  ASSERT_TRUE(logical->emptyResult(logical->getLastOperator()));
}

TEST_F(GOptDDLTest, CREATE_PERSON_IF_NOT_EXISTS) {
  std::string query =
      "CREATE NODE TABLE IF NOT EXISTS User(name STRING, PRIMARY KEY (name));";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("CREATE_PERSON_IF_NOT_EXISTS_physical"));
}

TEST_F(GOptDDLTest, CREATE_KNOWS_IF_NOT_EXISTS) {
  std::string query =
      "CREATE REL TABLE IF NOT EXISTS Follows (FROM User TO User, weight "
      "DOUBLE);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("CREATE_KNOWS_IF_NOT_EXISTS_physical"));
}

}  // namespace gopt
}  // namespace neug
