#include "gopt_test.h"
namespace neug {
namespace gopt {

class LDBCTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/sf_0.1.yaml");
  std::string statsData = getGOptResource("stats/sf_0.1-statistics.json");

  std::string getLDBCResource(std::string resource) {
    return getGOptResource("ldbc_test/" + resource);
  };

  std::string getLDBCResourcePath(std::string resource) {
    return getGOptResourcePath("ldbc_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(LDBCTest, IC_1) {
  std::string query =
      neug::gopt::Utils::readString(getLDBCResourcePath("ic_1.cypher"));
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getLDBCResource("IC_1_physical"));
}

TEST_F(LDBCTest, IC_11) {
  std::string query =
      neug::gopt::Utils::readString(getLDBCResourcePath("ic_11.cypher"));
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getLDBCResource("IC_11_logical"));
  auto physical = planPhysical(*logical);
  ASSERT_TRUE(physical != nullptr);
}

TEST_F(LDBCTest, IC_6) {
  std::string query =
      neug::gopt::Utils::readString(getLDBCResourcePath("ic_6.cypher"));
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getLDBCResource("IC_6_logical"));
  auto physical = planPhysical(*logical);
  ASSERT_TRUE(physical != nullptr);
}

TEST_F(LDBCTest, IC_3) {
  std::string query =
      neug::gopt::Utils::readString(getLDBCResourcePath("ic_3.cypher"));
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getLDBCResource("IC_3_logical"));
  auto physical = planPhysical(*logical);
  ASSERT_TRUE(physical != nullptr);
}

TEST_F(LDBCTest, IC_5) {
  std::string query =
      neug::gopt::Utils::readString(getLDBCResourcePath("ic_5.cypher"));
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getLDBCResource("IC_5_physical"));
}

TEST_F(LDBCTest, IC_14) {
  std::string query =
      neug::gopt::Utils::readString(getLDBCResourcePath("ic_14.cypher"));
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getLDBCResource("IC_14_logical"));
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getLDBCResource("IC_14_physical"));
}

TEST_F(LDBCTest, IC_7) {
  std::string query =
      neug::gopt::Utils::readString(getLDBCResourcePath("ic_7.cypher"));
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getLDBCResource("IC_7_logical"));
}

TEST_F(LDBCTest, IU_7) {
  std::string query =
      neug::gopt::Utils::readString(getLDBCResourcePath("iu_7.cypher"));
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getLDBCResource("IU_7_logical"));
}

TEST_F(LDBCTest, IC_13) {
  std::string query =
      neug::gopt::Utils::readString(getLDBCResourcePath("ic_13.cypher"));
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getLDBCResource("IC_13_logical"));
  auto physical = planPhysical(*logical);
  ASSERT_TRUE(physical != nullptr);
}

TEST_F(LDBCTest, IC_SHORT) {
  std::string query =
      "Match (m:COMMENT:POST {id: $messageId})-[:REPLYOF*0..]->(p:POST) Return "
      "count(p) ";
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical,
                                    getLDBCResource("IC_SHORT_logical"));
}

TEST_F(LDBCTest, IC_10) {
  std::string query =
      neug::gopt::Utils::readString(getLDBCResourcePath("ic_10.cypher"));
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getLDBCResource("IC_10_physical"));
}

}  // namespace gopt
}  // namespace neug