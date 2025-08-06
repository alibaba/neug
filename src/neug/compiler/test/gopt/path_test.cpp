#include "gopt_test.h"

namespace gs {
namespace gopt {

class PathTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/modern_schema_v2.yaml");
  std::string statsData = getGOptResource("stats/modern_stats_v2.json");
  std::string getPathResourcePath(std::string resource) {
    return getGOptResourcePath("path_test/" + resource);
  };

  std::string getPathResource(std::string resource) {
    return getGOptResource("path_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(PathTest, KNOWS_1_2) {
  std::string query =
      "MATCH (a:person)-[:knows*1..2]->(b:person) WHERE a.ID = 7 RETURN "
      "b.name;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPathResource("KNOWS_1_2_physical"));
}

TEST_F(PathTest, KNOWS_1_2_FILTER) {
  std::string query =
      "MATCH (a:person)-[e:knows*1..2 (r,_ | WHERE r.weight > "
      "1.0)]->(b:person) WHERE a.name = 'Alice' AND b.name = 'Bob' RETURN e;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("KNOWS_1_2_FILTER_physical"));
}

TEST_F(PathTest, KNOWS_V2_1_2) {
  std::string query =
      "MATCH (a:person)-[:knows_v2*1..2]->(b:person) WHERE a.ID = 7 RETURN "
      "b.name;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPathResource("KNOWS_V2_1_2_physical"));
}

TEST_F(PathTest, KNOWS_3_3) {
  std::string query =
      "MATCH (a:person)-[e:knows*3..3]->(b:person) RETURN COUNT(b.name);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPathResource("KNOWS_3_3_physical"));
}

TEST_F(PathTest, TRAIL_Path) {
  std::string query =
      "MATCH (a:person)-[e:knows* TRAIL 2..3]->(b:person) RETURN "
      "COUNT(b.name);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPathResource("TRAIL_Path_physical"));
}

TEST_F(PathTest, ACYCLIC_Path) {
  std::string query =
      "MATCH (a:person)-[e:knows* ACYCLIC 2..3]->(b:person) RETURN "
      "COUNT(b.name);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPathResource("ACYCLIC_Path_physical"));
}

TEST_F(PathTest, ANY_SHORTEST_Path) {
  std::string query =
      "MATCH (p: person {id : 1}) -[k:knows* SHORTEST 1..4]-(f:PERSON {name : "
      "'lion'}) RETURN k;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("ANY_SHORTEST_Path_physical"));
}

TEST_F(PathTest, ALL_SHORTEST_Path) {
  std::string query =
      "MATCH (a:person)-[e:knows* ALL SHORTEST 1..3]->(b:person) RETURN "
      "COUNT(b.name);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPathResource("ALL_SHORTEST_Path_physical"));
}

}  // namespace gopt
}  // namespace gs