#include "gopt_test.h"
namespace neug {
namespace gopt {

class ParamTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/data_types.yaml");

  std::string getParamResource(std::string resource) {
    return getGOptResource("param_test/" + resource);
  };

  std::string getParamResourcePath(std::string resource) {
    return getGOptResourcePath("param_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(ParamTest, COMPARE) {
  std::string query = R"(
    Match (n:person) 
    Where n.prop_int32 = $_int32
          OR n.prop_int64 = $_int64
          OR n.prop_uint32 = $_uint32
          OR n.prop_uint64 = $_uint64
          OR n.prop_bool = $_bool
          OR n.prop_float = $_float
          OR n.prop_double = $_double
          OR n.prop_text = $_text
          OR n.prop_varchar = $_varchar
          OR n.prop_date = $_date
          OR n.prop_datetime = $_datetime
          OR n.prop_interval = $_interval
    Return count(n);
  )";
  auto logical = planLogical(query, schemaData, "", rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getParamResource("COMPARE_physical"));
}

TEST_F(ParamTest, TEMPORAL_COMPARE) {
  std::string query = R"(
    Match (n:person) Where n.prop_date < $date Return count(n);
  )";
  auto logical = planLogical(query, schemaData, "", rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getParamResource("TEMPORAL_COMPARE_physical"));
}

TEST_F(ParamTest, FUNCTION_COMPARE) {
  std::string query = R"(
    Match (n:person) Where date_part('month', n.prop_date) = $month Return count(n);
  )";
  auto logical = planLogical(query, schemaData, "", rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getParamResource("FUNCTION_COMPARE_physical"));
}

TEST_F(ParamTest, FUNCTION_COMPARE_2) {
  std::string query = R"(
    Match (n:person) Where date_part('month', n.prop_date) = ($month % 12) Return count(n);
  )";
  auto logical = planLogical(query, schemaData, "", rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getParamResource("FUNCTION_COMPARE_2_physical"));
}

TEST_F(ParamTest, COMPOSITE_COMPARE) {
  std::string query = R"(
    Match (n:person) Where n.prop_array = $array Return count(n);
  )";
  auto logical = planLogical(query, schemaData, "", rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getParamResource("COMPOSITE_COMPARE_physical"));
}

TEST_F(ParamTest, CAST_PARAM) {
  std::string query = R"(
    UNWIND CAST($ids, 'INT64[]') as id 
    Create (p:person {prop_int64: id});
  )";
  auto logical = planLogical(query, schemaData, "", rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getParamResource("CAST_PARAM_physical"));
}

TEST_F(ParamTest, CAST_PARAM_2) {
  std::string query = R"(
    UNWIND CAST($ids, 'INT64[][]') as id 
    WITH id[0] as id0, id[1] as id1 
    Create(p1:person {prop_int64: id0})-[:knows]->(p2:person {prop_int64: id1});
  )";
  auto logical = planLogical(query, schemaData, "", rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getParamResource("CAST_PARAM_2_physical"));
}
}  // namespace gopt
}  // namespace neug