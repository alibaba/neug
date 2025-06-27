#include <memory>
#include "gopt_test.h"
#include "src/include/gopt/g_alias_manager.h"

namespace gs {
namespace gopt {

class DDLTest : public GOptTest {
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
TEST_F(DDLTest, CREATE_USER) {
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

TEST_F(DDLTest, CREATE_FOLLOWS) {
  std::string query =
      "CREATE REL TABLE Follows(FROM User TO User, since INT64);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("CREATE_FOLLOWS_physical"));
}

TEST_F(DDLTest, ALTER_USER_ADD) {
  std::string query = "ALTER TABLE User ADD age INT64;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("ALTER_USER_ADD_physical"));
}

TEST_F(DDLTest, ALTER_USER_RENAME_COLUMN) {
  std::string query = "ALTER TABLE User RENAME age TO grade;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("ALTER_USER_RENAME_COLUMN_physical"));
}

TEST_F(DDLTest, ALTER_USER_DROP_COLUMN) {
  std::string query = "ALTER TABLE User DROP grade;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("ALTER_USER_DROP_COLUMN_physical"));
}

TEST_F(DDLTest, ALTER_FOLLOWS_ADD) {
  std::string query = "ALTER TABLE Follows ADD age INT64;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("ALTER_FOLLOWS_ADD_physical"));
}

TEST_F(DDLTest, ALTER_FOLLOWS_RENAME_COLUMN) {
  std::string query = "ALTER TABLE Follows RENAME age TO grade;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("ALTER_FOLLOWS_RENAME_COLUMN_physical"));
}

TEST_F(DDLTest, ALTER_FOLLOWS_DROP_COLUMN) {
  std::string query = "ALTER TABLE Follows DROP grade;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("ALTER_FOLLOWS_DROP_COLUMN_physical"));
}

TEST_F(DDLTest, ALTER_USER_RENAME_TABLE) {
  std::string query = "ALTER TABLE User RENAME TO Student;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("ALTER_USER_RENAME_TABLE_physical"));
}

TEST_F(DDLTest, ALTER_FOLLOWS_RENAME_TABLE) {
  std::string query = "ALTER TABLE Follows RENAME TO Follows2;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDDLResource("ALTER_FOLLOWS_RENAME_TABLE_physical"));
}

TEST_F(DDLTest, DROP_USER) {
  std::string query = "DROP TABLE User;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getDDLResource("DROP_USER_physical"));
}

TEST_F(DDLTest, DROP_FOLLOWS) {
  std::string query = "DROP TABLE Follows;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getDDLResource("DROP_FOLLOWS_physical"));
}

}  // namespace gopt
}  // namespace gs
