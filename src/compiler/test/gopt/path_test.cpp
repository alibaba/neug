#include "gopt_test.h"

namespace gs {
namespace gopt {

class PathTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/person_schema.yaml");
  std::string statsData = getGOptResource("stats/person_stats.json");
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

}  // namespace gopt
}  // namespace gs