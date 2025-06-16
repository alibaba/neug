#include "gopt_test.h"

namespace gs {
namespace gopt {

class OrderTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/person_schema.yaml");
  std::string statsData = getGOptResource("stats/person_stats.json");
  std::string getOrderResourcePath(std::string resource) {
    return getGOptResourcePath("order_test/" + resource);
  };

  std::string getOrderResource(std::string resource) {
    return getGOptResource("order_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(OrderTest, ORDER_BY_ID) {
  std::string query = "MATCH (p:person) RETURN p.id ORDER BY p.id";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getOrderResource("ORDER_BY_ID_physical"));
}

TEST_F(OrderTest, ORDER_BY_ID_NAME_LIMIT) {
  std::string query =
      "MATCH (a:person)-[:knows*1..2]->(b:person) Return b.name as name, a.id "
      "as id Order BY a.id DESC, b.name ASC Limit 10;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getOrderResource("ORDER_BY_ID_NAME_LIMIT_physical"));
}

TEST_F(OrderTest, RETURN_LIMIT) {
  std::string query = "MATCH (a:person) Return a limit 10;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getOrderResource("RETURN_LIMIT_physical"));
}

TEST_F(OrderTest, RETURN_SKIP) {
  // the upper bound of `skip` is set to INT_MAX
  std::string query = "MATCH (a:person) Return a skip 10;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getOrderResource("RETURN_SKIP_physical"));
}
}  // namespace gopt
}  // namespace gs