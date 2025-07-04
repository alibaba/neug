#include <gtest/gtest.h>
#include "gopt_test.h"
namespace gs {
namespace gopt {

class DMLTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/person_schema.yaml");
  std::string statsData = getGOptResource("stats/person_stats.json");
  std::string getDMLResource(std::string resource) {
    return getGOptResource("dml_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};

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

TEST_F(DMLTest, COPY_PERSON) {
  std::string query =
      replaceResource("COPY person from 'DML_RESOURCE/person.csv';");
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  ASSERT_TRUE(physical != nullptr);
  auto resultYaml = GResultSchema::infer(*logical, aliasManager, getCatalog());
  auto returns = resultYaml["returns"];
  ASSERT_TRUE(returns.IsSequence() && returns.size() == 0);
}

TEST_F(DMLTest, COPY_KNOWS) {
  std::string query =
      replaceResource("COPY knows from 'DML_RESOURCE/knows.csv';");
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  ASSERT_TRUE(physical != nullptr);
  auto resultYaml = GResultSchema::infer(*logical, aliasManager, getCatalog());
  auto returns = resultYaml["returns"];
  ASSERT_TRUE(returns.IsSequence() && returns.size() == 0);
}

TEST_F(DMLTest, CREATE_VERTEX) {
  std::string query = "CREATE(p1 : person{id : 1}), (p2 : person{id : 2})";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getDMLResource("CREATE_VERTEX_physical"));
}

TEST_F(DMLTest, CREATE_VERTEX_EDGE) {
  std::string query =
      "CREATE (p1:person {id: 3})<-[:knows {weight: 3.0}]-(p2:person {id: 4})";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDMLResource("CREATE_VERTEX_EDGE_physical"));
}

TEST_F(DMLTest, CREATE_EDGE) {
  std::string query =
      "MATCH (p1:person {id: 1}), (p2:person {id: 2}) CREATE (p1)-[:knows "
      "{weight: 3.0}]->(p2)";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getDMLResource("CREATE_EDGE_physical"));
}

TEST_F(DMLTest, DEL_VERTEX) {
  std::string query = "MATCH (u:person) WHERE u.name = 'A' DELETE u";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getDMLResource("DEL_VERTEX_physical"));
}

TEST_F(DMLTest, DETACH_DEL_VERTEX) {
  std::string query = "MATCH (u:person) WHERE u.name = 'A' DETACH DELETE u";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDMLResource("DETACH_DEL_VERTEX_physical"));
}

TEST_F(DMLTest, DEL_EDGE) {
  std::string query =
      "MATCH (u:person)-[f]->(u1) WHERE u.name = 'Karissa' DELETE f";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getDMLResource("DEL_EDGE_physical"));
}

TEST_F(DMLTest, SET_VER_PROP) {
  std::string query =
      "MATCH (u:person) WHERE u.name = 'Adam' SET u.age = 50, u.name = 'mark' "
      "RETURN u";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getDMLResource("SET_VER_PROP_physical"));
}

TEST_F(DMLTest, SET_VER_EDGE_PROP) {
  std::string query =
      "MATCH (u0:person)-[f:knows]->() WHERE u0.name = 'Adam' SET f.weight = "
      "1999.0, u0.age = 10 RETURN f";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getDMLResource("SET_VER_EDGE_PROP_physical"));
}

}  // namespace gopt
}  // namespace gs