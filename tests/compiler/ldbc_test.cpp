#include "gopt_test.h"
namespace gs {
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

TEST_F(LDBCTest, IC_9) {
  std::string query =
      gs::gopt::Utils::readString(getLDBCResourcePath("ic_9.cypher"));
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getLDBCResource("IC_9_physical"));
}

TEST_F(LDBCTest, IC_5) {
  std::string query =
      gs::gopt::Utils::readString(getLDBCResourcePath("ic_5.cypher"));
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getLDBCResource("IC_5_physical"));
}

TEST_F(LDBCTest, IC_14) {
  std::string query =
      gs::gopt::Utils::readString(getLDBCResourcePath("ic_14.cypher"));
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getLDBCResource("IC_14_logical"));
}

TEST_F(LDBCTest, IC_7) {
  std::string query =
      gs::gopt::Utils::readString(getLDBCResourcePath("ic_7.cypher"));
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getLDBCResource("IC_7_logical"));
}

TEST_F(LDBCTest, IU_7) {
  std::string query =
      gs::gopt::Utils::readString(getLDBCResourcePath("iu_7.cypher"));
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getLDBCResource("IU_7_logical"));
}

}  // namespace gopt
}  // namespace gs