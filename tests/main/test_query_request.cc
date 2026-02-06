#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>

#include "neug/execution/common/params_map.h"
#include "neug/main/query_request.h"

#include <rapidjson/document.h>
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace neug {
namespace test {

struct NumericParameterScenario {
  static std::tuple<std::string, std::string, neug::execution::ParamsMap>
  MakeRequest() {
    neug::execution::ParamsMap params = {
        {"min_id", execution::Value::INT64(100)},
        {"limit", execution::Value::INT32(10)},
    };
    return {"MATCH (n) WHERE n.id > $min_id RETURN n.id", "read",
            std::move(params)};
  }

  static std::map<std::string, DataType> GetParamMetaMap() {
    return {{"min_id", DataType::INT64}, {"limit", DataType::INT32}};
  }
};

struct ListParameterScenario {
  static std::tuple<std::string, std::string, neug::execution::ParamsMap>
  MakeRequest() {
    neug::execution::ParamsMap params;
    params.emplace("id_list", execution::Value::LIST(ListStorage()));
    return {"MATCH (n) WHERE n.id IN $id_list RETURN n.id", "read",
            std::move(params)};
  }

  static std::map<std::string, DataType> GetParamMetaMap() {
    return {{"id_list", DataType::List(DataType::INT32)}};
  }

 private:
  static std::vector<execution::Value> ListStorage() {
    static std::vector<execution::Value> elements;
    for (int i = 1; i <= 5; ++i) {
      elements.emplace_back(execution::Value::INT32(i));
    }
    return elements;
  }
};

using QueryRequestScenarios =
    ::testing::Types<NumericParameterScenario, ListParameterScenario>;

template <typename Scenario>
class QueryRequestSerializerTypedTest : public ::testing::Test {};

TYPED_TEST_SUITE(QueryRequestSerializerTypedTest, QueryRequestScenarios);

TYPED_TEST(QueryRequestSerializerTypedTest, RoundTripParameters) {
  auto request = TypeParam::MakeRequest();
  const std::string serialized = RequestSerializer::SerializeRequest(
      std::get<0>(request), std::get<1>(request), std::get<2>(request));
  ASSERT_FALSE(serialized.empty());

  std::string query;
  neug::AccessMode mode;
  rapidjson::Document parameters;
  const auto status =
      RequestParser::ParseFromString(serialized, query, mode, parameters);
  ASSERT_TRUE(status.ok()) << status.ToString();

  EXPECT_EQ(query, std::get<0>(request));
  EXPECT_EQ(neug::AccessModeToString(mode), std::get<1>(request));
  auto param_meta_map = TypeParam::GetParamMetaMap();
  auto params = ParamsParser::ParseFromJsonObj(param_meta_map, parameters);
  EXPECT_EQ(params, std::get<2>(request));
}

}  // namespace test
}  // namespace neug
