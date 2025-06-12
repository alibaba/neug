#include "gopt_test.h"
namespace gs {
namespace gopt {

class PatternTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/person_schema.yaml");
  std::string statsData = getGOptResource("stats/person_stats.json");
  std::string getPatResource(std::string resource) {
    return getGOptResource("pattern_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(PatternTest, SCAN) {
  std::string query = "MATCH (n:person) return n.name;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPatResource("SCAN_physical"));
}

TEST_F(PatternTest, SCAN_FILTER) {
  std::string query = "Match (n:person) Where n.id = 1 Return n;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPatResource("SCAN_FILTER_physical"));
}

TEST_F(PatternTest, EXPAND) {
  std::string query =
      "Match (n:person)-[f:knows]->(m:person) Return n.name, f, m.name;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPatResource("EXPAND_physical"));
}

TEST_F(PatternTest, EXPAND_FILTER) {
  std::string query =
      "Match (n:person)-[f:knows]->(m:person) Where n.id = 1 AND f.weight > "
      "10.0 Return n.name, f, m.name;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPatResource("EXPAND_FILTER_physical"));
}

TEST_F(PatternTest, GETV_FILTER) {
  std::string query =
      "Match (n:person)-[f:knows]->(m:person) Where n.id = 1 AND f.weight > "
      "10.0 AND m.name = 'cc' Return n.name, f.weight, m.name;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPatResource("GETV_FILTER_physical"));
}

TEST_F(PatternTest, EXPAND_FILTER_2) {
  std::string query =
      "Match (:person {name: \"mark\"})-[:knows {weight: 10.0}]->(m:person) "
      "Return m;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getPatResource("EXPAND_FILTER_2_physical"));
}

TEST_F(PatternTest, PROJECT_AS) {
  std::string query =
      "Match (:person {name: \"mark\"})-[:knows {weight: 10.0}]->(m:person) "
      "Return m as q;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getPatResource("PROJECT_AS_physical"));
}

}  // namespace gopt
}  // namespace gs