#include "gopt_test.h"

namespace gs {
namespace gopt {

class AggTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/person_schema.yaml");
  std::string statsData = getGOptResource("stats/person_stats.json");
  std::string getAggResource(std::string resource) {
    return getGOptResource("agg_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(AggTest, COUNT) {
  std::string query = "Match (n:person) Return count(n)";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getAggResource("COUNT_physical"));
}

TEST_F(AggTest, COUNT_BY_AGE) {
  std::string query =
      "Match (n:person)-[k:knows]->(m:person) Return m.age, count(n)";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getAggResource("COUNT_BY_AGE_physical"));
}

TEST_F(AggTest, COUNT_BY_AGE_ID) {
  std::string query =
      "Match (n:person)-[k:knows]->(m:person) Return m.age, n.id, count(n)";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getAggResource("COUNT_BY_AGE_ID_physical"));
}

TEST_F(AggTest, COUNT_BY_AGE_ID_2) {
  std::string query =
      "Match (n:person)-[k:knows]->(m:person) Return m.age, count(n), n.id as "
      "id";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getAggResource("COUNT_BY_AGE_ID_2_physical"));
}

TEST_F(AggTest, COUNT_DISTINCT) {
  std::string query = "Match (n:person) Return count(distinct n.name)";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getAggResource("COUNT_DISTINCT_physical"));
}

TEST_F(AggTest, COUNT_STAR) {
  std::string query = "Match (n:person) Return count(*)";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getAggResource("COUNT_STAR_physical"));
}

}  // namespace gopt
}  // namespace gs
