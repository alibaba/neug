#include "gopt_test.h"

namespace gs {
namespace gopt {

class ExprTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/tinysnb_schema.yaml");
  std::string statsData = "";
  std::string getExprResourcePath(std::string resource) {
    return getGOptResourcePath("expr_test/" + resource);
  };

  std::string getExprResource(std::string resource) {
    return getGOptResource("expr_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(ExprTest, IS_NULL) {
  std::string query = "MATCH (a:person) WHERE a.age IS NULL RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("IS_NULL_physical"));
}

TEST_F(ExprTest, IS_NOT_NULL) {
  std::string query =
      "MATCH (a:person) WHERE a.age IS NOT NULL RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("IS_NOT_NULL_physical"));
}

TEST_F(ExprTest, MULTI_EQUAL_1) {
  std::string query =
      "MATCH (a:person) WHERE a.gender * 2.1 = 2.1 * a.gender RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("MULTI_EQUAL_1_physical"));
}

TEST_F(ExprTest, MULTI_EQUAL_2) {
  std::string query =
      "MATCH (a:person) WHERE (a.gender*3.5 = 7.0) RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("MULTI_EQUAL_2_physical"));
}

TEST_F(ExprTest, ADD_NULL) {
  // null is converted to 0 in arithmetic expressions
  std::string query =
      "MATCH (a:person) WHERE a.age + null <= 200 RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("ADD_NULL_physical"));
}

TEST_F(ExprTest, ORDER_BY_ADD) {
  // p.age + p.ID is calculated before ordering
  std::string query =
      "MATCH (p:person) RETURN p.ID, p.age ORDER BY p.age + p.ID;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("ORDER_BY_ADD_physical"));
}

TEST_F(ExprTest, TYPE_CAST) {
  // `a.gender` is i32 type, `1` is i64 type, here actually exists a
  // cast(a.gender, i64) function, but engine can support the implicit type
  // conversion among numeric types, so the cast is skipped.
  std::string query =
      "MATCH (a:person)-[:knows]-(b:person)-[:knows]-(c:person) "
      "WHERE a.gender = 1 AND b.gender = 2 AND c.fName = \"Bob\" "
      "RETURN a.fName, b.fName;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("TYPE_CAST_physical"));
}

TEST_F(ExprTest, SUM_AGE) {
  std::string query =
      "MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person) "
      "WHERE a.age < 31 RETURN a.age + b.age + c.age;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getExprResource("SUM_AGE_physical"));
}

TEST_F(ExprTest, MULTI_EQUAL_3) {
  std::string query =
      "MATCH (a:person) WHERE (a.gender*3.5 = 7.0) RETURN COUNT(*);";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getExprResource("MULTI_EQUAL_3_physical"));
}

}  // namespace gopt
}  // namespace gs